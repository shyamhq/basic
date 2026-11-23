// INTAKE / SUPERVISOR AGENT: one thread per inbound conversation that isn't a reply to an offer.
//
//  START -> identify_caller -> classify -+-- emergency ----------> emergency (page manager, HITL ack) -> END
//                                       +-- low confidence -----> human_triage (HITL picks the intent) -> route again
//                                       +-- callout ------------> find_affected_shifts -> spawn_children
//                                       |                           -> wait_children <-> collect   (FAN-OUT / FAN-IN)
//                                       +-- preference_update --> update_preferences -> END
//                                       +-- question/other -----> reply -> END
//
// SUPERVISOR PATTERN at the *system* level: instead of running ShiftFill as a subgraph inside
// this thread, we spawn one CHILD THREAD per shift. Each child is independently durable,
// lockable, retryable and observable; the parent just waits for "child_done" mailbox events.
// (In-graph parallelism with Send is shown in shiftFill.ts: score_candidate.)

import { Annotation, Command, END, interrupt, START, StateGraph } from "@langchain/langgraph";
import { addMinutes, clock } from "../clock.js";
import { TRIAGE_MIN_CONFIDENCE } from "../config.js";
import { one, q, tx } from "../db.js";
import { classifyInbound, type Triage } from "../llm.js";
import { getProcedures } from "../memory.js";
import { enqueue, startThread } from "../outbox.js";
import { callTool } from "../tools.js";
import { appendList, fmtTime, type MailboxEvent, type WaitPayload } from "./common.js";

type ChildResult = { childThreadId: string; shiftId: string; outcome: string; caregiverId: string | null };

export const IntakeState = Annotation.Root({
  threadId: Annotation<string>,
  agencyId: Annotation<string>,
  channel: Annotation<string>,
  fromPhone: Annotation<string>,
  text: Annotation<string>,
  caller: Annotation<{ kind: "caregiver" | "facility" | "unknown"; id: string | null; name: string }>,
  triage: Annotation<Triage>,
  children: Annotation<string[]>,
  childResults: Annotation<Record<string, ChildResult>>({ reducer: (a, b) => ({ ...a, ...b }), default: () => ({}) }),
  outcome: Annotation<string | null>,
  log: appendList<string>(),
});
type S = typeof IntakeState.State;

const sms = (s: S, body: string, key: string) =>
  callTool("comms.send_sms", { to: s.fromPhone, body }, { threadId: s.threadId, idempotencyKey: `${s.threadId}:sms:${key}` });

async function identifyCaller(s: S): Promise<Partial<S>> {
  const cg = await one(`SELECT id, name FROM caregivers WHERE phone = $1`, [s.fromPhone]);
  const caller = cg ? { kind: "caregiver" as const, id: cg.id, name: cg.name } : { kind: "unknown" as const, id: null, name: s.fromPhone };
  return { caller, log: [`Inbound ${s.channel} from ${caller.name}: "${s.text}"`] };
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
  const d = interrupt<WaitPayload, MailboxEvent>({
    type: "triage_review", accepts: ["human_decision"],
    title: `Unclear message from ${s.caller.name}`, text: s.text, suggestion: s.triage,
    options: ["callout", "preference_update", "question", "other"],
  }).payload;
  const triage = { ...s.triage, intent: d.edits?.intent ?? s.triage.intent, confidence: 1 };
  const next = route({ ...s, triage });
  return new Command({ goto: next, update: { triage, log: [`${d.decidedBy} set intent = ${triage.intent}`] } });
}

async function emergency(s: S): Promise<Partial<S>> {
  // Interrupt first; side effects after resume are idempotent anyway (tool keys).
  await callTool("ops.page_manager", { message: `EMERGENCY from ${s.caller.name} (${s.fromPhone}): ${s.text}` },
    { threadId: s.threadId, idempotencyKey: `${s.threadId}:page` });
  await sms(s, "We've alerted the on-call manager right now. If anyone is in danger, call 911.", "emergency_ack");
  const d = interrupt<WaitPayload, MailboxEvent>({
    type: "emergency_ack", accepts: ["human_decision"], title: `EMERGENCY: ${s.text}`, caller: s.caller,
  }).payload;
  return { outcome: "handled_by_human", log: [`Emergency handled by ${d.decidedBy}: ${d.reason ?? ""}`] };
}

async function findAffectedShifts(s: S): Promise<Partial<S> | Command> {
  const shifts = await q(
    `SELECT id, starts_at FROM shifts WHERE caregiver_id = $1 AND status = 'scheduled' AND starts_at BETWEEN $2 AND $3 ORDER BY starts_at`,
    [s.caller.id, clock.now(), addMinutes(clock.now(), 48 * 60)],
  );
  if (!shifts.length) {
    await sms(s, "Thanks for letting us know - you have no shifts in the next 48h.", "noshifts");
    return new Command({ goto: END, update: { outcome: "no_shifts", log: ["No upcoming shifts"] } });
  }
  return { children: shifts.map((sh) => `fill:${sh.id}:${s.threadId}`), log: [`Affected shifts: ${shifts.map((x) => `${x.id} @ ${fmtTime(x.starts_at)}`).join(", ")}`] };
}

async function spawnChildren(s: S): Promise<Partial<S>> {
  // FAN-OUT through the outbox: all child threads are created in ONE transaction (all-or-nothing),
  // and re-running this node is harmless (startThread is ON CONFLICT DO NOTHING).
  await tx(async (c) => {
    for (const childId of s.children) {
      const shiftId = childId.split(":")[1];
      await startThread({
        threadId: childId, graph: "shiftfill", agencyId: s.agencyId, parentThreadId: s.threadId,
        input: { threadId: childId, agencyId: s.agencyId, shiftId, parentThreadId: s.threadId, excludeCaregiverId: s.caller.id },
      }, c);
    }
  });
  await sms(s, `Feel better, ${s.caller.name.split(" ")[0]}. We're finding cover for your ${s.children.length} shift(s).`, "ack");
  return { log: [`Spawned ${s.children.length} ShiftFill child thread(s)`] };
}

async function waitChildren(s: S): Promise<Partial<S>> {
  const ev = interrupt<WaitPayload, MailboxEvent>({
    type: "wait_children", accepts: ["child_done"], pending: s.children.filter((c) => !s.childResults[c]),
  });
  const r = ev.payload as ChildResult;
  return { childResults: { [r.childThreadId]: r }, log: [`Child ${r.shiftId} finished: ${r.outcome}`] };
}

function collect(s: S): string {
  return s.children.every((c) => s.childResults[c]) ? "summarize" : "wait_children";   // barrier
}

async function summarize(s: S): Promise<Partial<S>> {
  const results = Object.values(s.childResults);
  const filled = results.filter((r) => r.outcome === "filled").length;
  return { outcome: `${filled}/${results.length} shifts covered`, log: [`All children done: ${filled}/${results.length} covered`] };
}

async function updatePreferences(s: S): Promise<Partial<S>> {
  await enqueue("memory.extract", { agencyId: s.agencyId, caregiverId: s.caller.id, text: s.text, source: "nurse_stated" }, `extract:${s.threadId}`);
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
    .addNode("spawn_children", spawnChildren)
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
