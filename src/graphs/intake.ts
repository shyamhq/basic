// INTAKE / SUPERVISOR AGENT: one thread per CONVERSATION (not per message).
//
//  START -> identify_caller -> classify -+-- emergency ----------> emergency (page manager, HITL ack) -> END
//                                        +-- low confidence -----> human_triage (HITL picks the intent) -> route again
//                                        +-- callout ------------> find_affected_shifts -> spawn_children
//                                        |                           -> wait_children <-> collect   (FAN-OUT / FAN-IN)
//                                        +-- preference_update --> update_preferences -> END
//                                        +-- question/other -----> reply -> END
//
// SUPERVISOR PATTERN at the system level: each affected shift gets its own CHILD THREAD
// ("fill:<shiftId>" - a natural key, so a second text or a facility post can't start a duplicate).
// The parent waits on child_done events (including outcome "failed"), with a deadline timer so the
// barrier can never hang. Follow-up texts from the same nurse arrive as nurse_message events
// ("double texting") instead of starting new conversations.

import { Annotation, Command, END, interrupt, START, StateGraph } from "@langchain/langgraph";
import { and, asc, between, eq } from "drizzle-orm";
import { addMinutes, clock } from "../clock.js";
import { TRIAGE_MIN_CONFIDENCE } from "../config.js";
import { db } from "../db/index.js";
import { caregivers, shifts } from "../db/schema.js";
import { addMemoryJob } from "../jobs.js";
import { classifyInbound, type Triage } from "../llm.js";
import { startThread } from "../mailbox.js";
import { getProcedures } from "../memory.js";
import { setTimer } from "../timers.js";
import { callTool } from "../tools.js";
import { appendList, fmtTime, type MailboxEvent, type WaitPayload } from "./common.js";

type ChildResult = { childThreadId: string; shiftId: string; outcome: string; caregiverId: string | null };

export const IntakeState = Annotation.Root({
  threadId: Annotation<string>,
  agencyId: Annotation<string>,
  channel: Annotation<string>,
  fromPhone: Annotation<string>,
  text: Annotation<string>,
  caller: Annotation<{ kind: "caregiver" | "unknown"; id: string | null; name: string }>,
  triage: Annotation<Triage>,
  children: Annotation<string[]>,
  childResults: Annotation<Record<string, ChildResult>>({ reducer: (a, b) => ({ ...a, ...b }), default: () => ({}) }),
  deadlineHit: Annotation<boolean>,
  outcome: Annotation<string | null>,
  log: appendList<string>(),
});
type S = typeof IntakeState.State;

const sms = (s: S, body: string, key: string) =>
  callTool("comms.send_sms", { to: s.fromPhone, body }, { agencyId: s.agencyId, threadId: s.threadId, idempotencyKey: `${s.threadId}:sms:${key}` });

async function identifyCaller(s: S): Promise<Partial<S>> {
  const [cg] = await db.select({ id: caregivers.id, name: caregivers.name }).from(caregivers).where(eq(caregivers.phone, s.fromPhone));
  const caller = cg ? { kind: "caregiver" as const, id: cg.id, name: cg.name } : { kind: "unknown" as const, id: null, name: s.fromPhone };
  return { caller, children: [], deadlineHit: false, outcome: null, log: [`Inbound ${s.channel} from ${caller.name}: "${s.text}"`] };
}

async function triage(s: S): Promise<Partial<S>> {
  const playbook = (await getProcedures(s.agencyId)).map((p) => p.text);   // procedural memory in the prompt
  const t = await classifyInbound(s.text, playbook);
  return { triage: t, log: [`Triage: ${t.intent} (confidence ${t.confidence}, ${t.urgency})`] };
}

function route(s: S): string {
  if (s.triage.intent === "emergency") return "emergency";                       // never AI-only
  if (s.triage.confidence < TRIAGE_MIN_CONFIDENCE) return "human_triage";        // unsure -> ask a human
  if (s.triage.intent === "callout" && s.caller.kind === "caregiver") return "find_affected_shifts";
  if (s.triage.intent === "preference_update" && s.caller.kind === "caregiver") return "update_preferences";
  return "reply";
}

async function humanTriage(s: S): Promise<Command> {
  const ev = interrupt<WaitPayload, MailboxEvent>({
    type: "triage_review", accepts: ["human_decision"],
    title: `Unclear message from ${s.caller.name}`, text: s.text, suggestion: s.triage,
    options: ["callout", "preference_update", "question", "other"],
  });
  const d = ev.payload;
  switch (d.action) {
    case "edit": {
      const triage = { ...s.triage, intent: d.edits.intent, confidence: 1 };
      return new Command({ goto: route({ ...s, triage }), update: { triage, log: [`${d.decidedBy} set intent = ${triage.intent}`] } });
    }
    default:
      throw new Error(`human_triage: unexpected action ${d.action}`);
  }
}

async function emergency(s: S): Promise<Partial<S>> {
  // Side effects BEFORE interrupt() run again when the node resumes -> they must be idempotent (tool keys).
  await callTool("ops.page_manager", { message: `EMERGENCY from ${s.caller.name} (${s.fromPhone}): ${s.text}` },
    { agencyId: s.agencyId, threadId: s.threadId, idempotencyKey: `${s.threadId}:page` });
  await sms(s, "We've alerted the on-call manager right now. If anyone is in danger, call 911.", "emergency_ack");
  const ev = interrupt<WaitPayload, MailboxEvent>({ type: "emergency_ack", accepts: ["human_decision"], title: `EMERGENCY: ${s.text}`, caller: s.caller, options: ["approve"] });
  return { outcome: "handled_by_human", log: [`Emergency handled by ${ev.payload.decidedBy}: ${ev.payload.reason ?? ""}`] };
}

async function findAffectedShifts(s: S): Promise<Partial<S> | Command> {
  const rows = await db.select({ id: shifts.id, startsAt: shifts.startsAt }).from(shifts)
    .where(and(eq(shifts.caregiverId, s.caller.id!), eq(shifts.status, "scheduled"), between(shifts.startsAt, clock.now(), addMinutes(clock.now(), 48 * 60))))
    .orderBy(asc(shifts.startsAt));
  if (!rows.length) {
    await sms(s, "Thanks for letting us know - you have no shifts in the next 48h.", "noshifts");
    return new Command({ goto: END, update: { outcome: "no_shifts", log: ["No upcoming shifts"] } });
  }
  return { children: rows.map((r) => `fill:${r.id}`), log: [`Affected shifts: ${rows.map((r) => `${r.id} @ ${fmtTime(r.startsAt)}`).join(", ")}`] };
}

async function spawnChildren(s: S): Promise<Partial<S> | Command> {
  // FAN-OUT: all children created in ONE transaction. Natural ids make it idempotent AND
  // prevent duplicates: if a shift already has a fill thread (another conversation, a facility
  // post), we don't start a second one - and we don't wait for one we don't own.
  const owned: string[] = [];
  const log: string[] = [];
  await db.transaction(async (tx) => {
    for (const childId of s.children) {
      const shiftId = childId.slice("fill:".length);
      const created = await startThread({
        threadId: childId, graph: "shiftfill", agencyId: s.agencyId, subject: shiftId, parentThreadId: s.threadId,
        input: { threadId: childId, agencyId: s.agencyId, shiftId, parentThreadId: s.threadId, excludeCaregiverId: s.caller.id },
      }, tx);
      if (created) owned.push(childId); else log.push(`${shiftId} is already being covered by ${childId}`);
    }
    // the barrier must not hang forever: give up waiting at the earliest shift start
    const [first] = await tx.select({ startsAt: shifts.startsAt }).from(shifts).where(eq(shifts.id, s.children[0].slice(5)));
    await setTimer({ id: `${s.threadId}:deadline`, agencyId: s.agencyId, threadId: s.threadId, kind: "children_deadline", fireAt: first.startsAt }, tx);
  });
  await sms(s, `Feel better, ${s.caller.name.split(" ")[0]}. We're finding cover for your ${s.children.length} shift(s).`, "ack");
  if (!owned.length) return new Command({ goto: "summarize", update: { children: [], log } });
  return { children: owned, log: [...log, `Spawned ${owned.length} ShiftFill child thread(s)`] };
}

async function waitChildren(s: S): Promise<Partial<S>> {
  const ev = interrupt<WaitPayload, MailboxEvent>({
    type: "wait_children", accepts: ["child_done", "nurse_message", "timer"], pending: s.children.filter((c) => !s.childResults[c]),
  });
  if (ev.kind === "nurse_message") {                 // double texting: acknowledge, don't start new work
    await sms(s, "Thanks - we're already finding cover for your shifts and will text you when they're covered.", `followup:${ev.id}`);
    return { log: [`Follow-up text acknowledged: "${ev.payload.text}"`] };
  }
  if (ev.kind === "timer") return { deadlineHit: true, log: ["Deadline reached while waiting for children"] };
  const r = ev.payload as ChildResult;
  return { childResults: { [r.childThreadId]: r }, log: [`Child ${r.shiftId} finished: ${r.outcome}`] };
}

const collect = (s: S) => (s.deadlineHit || s.children.every((c) => s.childResults[c]) ? "summarize" : "wait_children");   // barrier

async function summarize(s: S): Promise<Partial<S>> {
  const results = Object.values(s.childResults);
  const filled = results.filter((r) => r.outcome === "filled").length;
  const failed = results.filter((r) => r.outcome === "failed").length;
  return { outcome: `${filled}/${s.children.length} shifts covered`, log: [`All children done: ${filled}/${s.children.length} covered${failed ? `, ${failed} failed (humans paged)` : ""}`] };
}

async function updatePreferences(s: S): Promise<Partial<S>> {
  await addMemoryJob(db, "memory_extract", { agencyId: s.agencyId, caregiverId: s.caller.id!, text: s.text, source: "nurse_stated" }, `extract:${s.threadId}`);
  await sms(s, "Got it - we've updated your preferences.", "prefs");
  return { outcome: "preferences_updated", log: ["Queued preference extraction"] };
}

async function reply(s: S): Promise<Partial<S>> {
  await sms(s, "Thanks! A staffing coordinator will get back to you shortly.", "reply");
  return { outcome: "replied", log: ["Sent holding reply"] };
}

export function buildIntakeGraph() {
  return new StateGraph(IntakeState)
    .addNode("identify_caller", identifyCaller)
    .addNode("classify", triage, { retryPolicy: { maxAttempts: 3 } })
    .addNode("human_triage", humanTriage, { ends: ["find_affected_shifts", "update_preferences", "reply", "emergency"] })
    .addNode("emergency", emergency)
    .addNode("find_affected_shifts", findAffectedShifts, { ends: [END] })
    .addNode("spawn_children", spawnChildren, { ends: ["summarize"] })
    .addNode("wait_children", waitChildren)
    .addNode("summarize", summarize)
    .addNode("update_preferences", updatePreferences)
    .addNode("reply", reply)
    .addEdge(START, "identify_caller")
    .addEdge("identify_caller", "classify")
    .addConditionalEdges("classify", route, ["emergency", "human_triage", "find_affected_shifts", "update_preferences", "reply"])
    .addEdge("find_affected_shifts", "spawn_children")
    .addEdge("spawn_children", "wait_children")
    .addConditionalEdges("wait_children", collect, ["summarize", "wait_children"])
    .addEdge("summarize", END)
    .addEdge("emergency", END)
    .addEdge("update_preferences", END)
    .addEdge("reply", END);
}
