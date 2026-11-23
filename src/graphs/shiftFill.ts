// SHIFT-FILL AGENT: a long-running graph, one thread per open shift (thread id = "fill:<shiftId>").
//
//  START
//    -> load_shift          open the shift in the facility system, set a hard-deadline timer
//    -> find_candidates     HARD filters in SQL (license, certs, overlap, distance, pay floor)
//                           + memory hard prefs ("no nights") -> explainable exclusions
//    -> score_candidate xN  PARALLEL fan-out with Send (map) ...
//    -> rank                ... fan-in (reduce): sort, keep reasons
//    -> prepare_wave        next WAVE_SIZE nurses (memory re-checked), channel, drafts
//    -> review_drafts       HITL (optional per agency): a human approves/edits outbound text
//    -> send_wave           send (idempotent tools), record offers, set the wave timer
//    -> wait                interrupt(): PAUSE for minutes or hours, using no compute
//    -> interpret           the LLM reads the reply - in its OWN node, so its output is checkpointed
//                           and a replay can't classify the same text differently (non-determinism)
//    -> handle_event        decide + replay-safe writes -> Command(goto=...)
//         accept/counter -> check_policy -> (approval) -> book -> finish
//         decline        -> wait / next wave;  timeout -> next wave / escalate
//    -> escalate            HITL handoff: a coordinator assigns someone or cancels the shift
//    -> finish              withdraw open offers, episodes -> memory, tell the parent thread
// Every pause also accepts a "cancel" event (e.g. the facility no longer needs the shift).
//
// RULES for nodes, because LangGraph re-runs the WHOLE node on resume/after a crash:
//   * interrupt() first; side effects after it, or idempotent (deterministic keys, ON CONFLICT).
//   * a DB write that a later branch depends on must be replay-safe: offers remember the
//     event that last moved them (last_event_id), so re-running the node takes the same branch.
//   * keep state small and free of contact details: ids + scores, look phones up when sending.

import { Annotation, Command, END, interrupt, Send, START, StateGraph } from "@langchain/langgraph";
import { and, count, eq, inArray, or, sql } from "drizzle-orm";
import { addMinutes, clock, hoursBetween } from "../clock.js";
import { MAX_WAVES, OVERTIME_HOURS, URGENT_HOURS, WAVE_SIZE, WAVE_TIMEOUT_MIN } from "../config.js";
import { db } from "../db/index.js";
import { agencies, caregivers, offers, shifts } from "../db/schema.js";
import { maybeCrash } from "../faults.js";
import { addMemoryJob } from "../jobs.js";
import { parseReply, type Reply } from "../llm.js";
import { deliver } from "../mailbox.js";
import { getPreferences, getProcedures, recallForShift, type CandidateMemory } from "../memory.js";
import { cancelTimers, setTimer } from "../timers.js";
import { callTool, ToolConflict, type ToolCtx } from "../tools.js";
import { appendList, fmtTime, offerCodeFor, shiftTimeOf, type MailboxEvent, type WaitPayload } from "./common.js";

// ------------------------------------------------------------------ state
type Shift = {
  id: string; facilityId: string; facilityName: string; facilityNotes: string | null;
  role: string; specialty: string | null; startsAt: string; endsAt: string; rate: number; maxRate: number;
  shiftTime: "day" | "evening" | "night"; hours: number; urgent: boolean;
};
type Scored = { id: string; name: string; score: number; reasons: string[]; channel: "sms" | "call"; overtime: boolean; minRate: number };
type Draft = { caregiverId: string; name: string; channel: "sms" | "call"; offerCode: string; text: string };
type Chosen = { caregiverId: string; name: string; rate: number; offerCode: string };

export const ShiftFillState = Annotation.Root({
  threadId: Annotation<string>,
  agencyId: Annotation<string>,
  shiftId: Annotation<string>,
  parentThreadId: Annotation<string | null>,
  excludeCaregiverId: Annotation<string | null>,   // the nurse who called out
  shift: Annotation<Shift>,
  memory: Annotation<{ perCaregiver: Record<string, CandidateMemory>; facilityFacts: string[] }>,
  candidates: Annotation<any[]>,
  scored: appendList<Scored>(),                    // written by N parallel score_candidate branches
  ranked: Annotation<Scored[]>,
  wave: Annotation<number>,
  contacted: appendList<string>(),
  declined: appendList<string>(),
  drafts: Annotation<Draft[]>,
  event: Annotation<MailboxEvent | null>,
  reply: Annotation<Reply | null>,
  seenEvents: appendList<string>(),                // idempotent consumer: ignore duplicate deliveries
  chosen: Annotation<Chosen | null>,
  risks: Annotation<string[]>,
  approvedVia: Annotation<{ by: string; approvalId?: number } | null>,
  outcome: Annotation<string | null>,
  log: appendList<string>(),                       // human-readable timeline (learning + audit)
});
type S = typeof ShiftFillState.State;

// ------------------------------------------------------------------ helpers
const ctx = (s: S, key: string, approval?: ToolCtx["approval"]): ToolCtx =>
  ({ agencyId: s.agencyId, threadId: s.threadId, idempotencyKey: `${s.threadId}:${key}`, approval });

async function contact(caregiverId: string) {
  const [c] = await db.select({ phone: caregivers.phone, name: caregivers.name }).from(caregivers).where(eq(caregivers.id, caregiverId));
  return { phone: c.phone, first: c.name.split(" ")[0] };
}
async function textNurse(s: S, caregiverId: string, body: (first: string) => string, key: string) {
  const c = await contact(caregiverId);
  await callTool("comms.send_sms", { to: c.phone, body: body(c.first) }, ctx(s, `sms:${key}`));
}

/** Replay-safe offer transition: applies if the offer is in one of `from`, or was already moved by THIS event. */
async function moveOffer(code: string, to: (typeof offers.$inferSelect)["status"], from: string[], eventId: string) {
  const rows = await db.update(offers).set({ status: to, lastEventId: eventId })
    .where(and(eq(offers.code, code), or(inArray(offers.status, from as any), eq(offers.lastEventId, eventId))))
    .returning();
  return rows.length > 0;
}

/** Every pause accepts "cancel"; this is where it ends up. */
const cancelled = (ev: MailboxEvent) =>
  new Command({ goto: "finish", update: { outcome: "cancelled", log: [`Cancelled: ${ev.payload.reason ?? "no reason"}`] } });

// ------------------------------------------------------------------ nodes
async function loadShift(s: S): Promise<Partial<S>> {
  const row = await callTool("scheduler.get_shift", { shiftId: s.shiftId }, ctx(s, "get_shift"));
  await callTool("scheduler.open_shift", { shiftId: s.shiftId, reason: "needs coverage" }, ctx(s, "open_shift"));
  const { shift: sh, facility: f } = row;
  const shift: Shift = {
    id: sh.id, facilityId: f.id, facilityName: f.name, facilityNotes: f.notes, role: sh.role, specialty: sh.specialty,
    startsAt: new Date(sh.startsAt).toISOString(), endsAt: new Date(sh.endsAt).toISOString(), rate: sh.rate, maxRate: sh.maxRate,
    shiftTime: shiftTimeOf(sh.startsAt), hours: hoursBetween(new Date(sh.startsAt), new Date(sh.endsAt)),
    urgent: hoursBetween(clock.now(), new Date(sh.startsAt)) < URGENT_HOURS,
  };
  // Hard deadline: if still unfilled 2h before start, a human must take over.
  await setTimer({ id: `${s.threadId}:deadline`, agencyId: s.agencyId, threadId: s.threadId, kind: "deadline", fireAt: addMinutes(new Date(shift.startsAt), -120) });
  return {
    shift, wave: 0, chosen: null, risks: [], approvedVia: null, outcome: null, event: null, reply: null,
    log: [`Opened ${shift.role} ${shift.shiftTime} shift at ${shift.facilityName} (${fmtTime(shift.startsAt)}, $${shift.rate}/h${shift.urgent ? ", URGENT" : ""})`],
  };
}

async function findCandidates(s: S): Promise<Partial<S>> {
  const sh = s.shift;
  // 1) HARD constraints in SQL. Rules that must never be violated live in code, not in a prompt.
  //    Raw SQL on purpose: array containment, distance math and NOT EXISTS read better than a builder.
  //    Only the columns scoring needs - no phone numbers or bios end up in checkpoints.
  const { rows } = await db.execute<any>(sql`
    SELECT c.id, c.name, c.specialties, c.shift_prefs, c.preferred_channel, c.contact_start, c.min_rate,
           c.max_distance_mi, c.hours_this_week, c.reliability,
           sqrt(power(c.x - f.x, 2) + power(c.y - f.y, 2)) AS distance
    FROM app.caregivers c JOIN app.facilities f ON f.id = ${sh.facilityId}
    WHERE c.agency_id = ${s.agencyId} AND c.active AND c.license = ${sh.role}
      AND c.certs @> f.requirements                                     -- holds every required cert
      AND c.id <> coalesce(${s.excludeCaregiverId}::text, '')
      AND c.min_rate <= ${sh.maxRate}                                   -- facility can afford them
      AND NOT EXISTS (SELECT 1 FROM app.shifts o WHERE o.caregiver_id = c.id AND o.status IN ('scheduled','filled')
                      AND o.starts_at < ${sh.endsAt}::timestamptz AND o.ends_at > ${sh.startsAt}::timestamptz)
    ORDER BY c.id`);
  const inRange = rows.filter((c) => c.distance <= c.max_distance_mi);
  // 2) MEMORY read path: learned preferences + episodes for each remaining nurse.
  const mem = await recallForShift(s.agencyId, { facility_id: sh.facilityId, shiftTime: sh.shiftTime }, inRange.map((c) => c.id));
  const log: string[] = [];
  const candidates = inRange.filter((c) => {
    const hard = mem.perCaregiver[c.id].prefs.find((p) => p.strength === "hard" &&
      ((p.type === "avoid_shift_time" && p.value === sh.shiftTime) || (p.type === "avoid_facility" && p.value === sh.facilityId)));
    if (hard) log.push(`Excluded ${c.name}: memory says "${hard.text}" (${hard.source})`);
    return !hard;
  });
  log.push(`${rows.length} matched license/certs/availability, ${inRange.length} within travel distance, ${candidates.length} after memory filters`);
  return { candidates, memory: { perCaregiver: mem.perCaregiver, facilityFacts: mem.facilityFacts }, log };
}

/** Conditional edge -> one Send per candidate (map step). */
function fanOutScoring(s: S) {
  if (!s.candidates.length) return "escalate";
  return s.candidates.map((c) => new Send("score_candidate", { candidate: c, shift: s.shift, mem: s.memory.perCaregiver[c.id] }));
}

/** Runs in parallel, once per candidate. Receives the Send payload, not the full state. */
async function scoreCandidate(input: any): Promise<Partial<S>> {
  const { candidate: c, shift: sh, mem } = input as { candidate: any; shift: Shift; mem: CandidateMemory };
  let score = 0;
  const reasons: string[] = [];
  const add = (pts: number, why: string) => { score += pts; reasons.push(`${pts > 0 ? "+" : ""}${pts.toFixed(1)} ${why}`); };

  if (c.shift_prefs.includes(sh.shiftTime)) add(3, `likes ${sh.shiftTime} shifts (profile)`);
  if (sh.specialty && c.specialties.includes(sh.specialty)) add(2, `specialty ${sh.specialty}`);
  add(3 * c.reliability, `reliability ${Math.round(c.reliability * 100)}%`);
  add(2 * (1 - c.distance / c.max_distance_mi), `${c.distance.toFixed(1)} mi away`);
  if (c.min_rate > sh.rate) add(-1, `wants $${c.min_rate}/h (posted $${sh.rate})`);
  const overtime = c.hours_this_week + sh.hours > OVERTIME_HOURS;
  if (overtime) add(-2, `would hit overtime (${c.hours_this_week}h + ${sh.hours}h)`);

  for (const p of mem.prefs) {                       // soft preferences from memory
    if (p.type === "avoid_shift_time" && p.value === sh.shiftTime) add(-3, `memory: ${p.text}`);
    if (p.type === "avoid_facility" && p.value === sh.facilityId) add(-3, `memory: ${p.text}`);
    if (p.type === "prefer_facility" && p.value === sh.facilityId) add(2, `memory: ${p.text}`);
  }
  for (const e of mem.episodes) {                    // episodic memory: what happened last times
    if (e.facilityId !== sh.facilityId) continue;
    if (e.signal === "assigned") add(1, "episode: worked here before");
    if (e.signal === "declined" || e.signal === "no_response") add(-1, `episode: ${e.signal} here before`);
  }

  // channel: memory beats profile; urgent shifts get a call unless she asked for texts only
  const chanPref = mem.prefs.find((p) => p.type === "channel");
  let channel: "sms" | "call" = (chanPref?.value as any) ?? c.preferred_channel;
  if (sh.urgent && chanPref?.strength !== "hard") channel = "call";
  const notBefore = Number(mem.prefs.find((p) => p.type === "contact_after_hour")?.value ?? c.contact_start);
  if (channel === "call" && clock.now().getUTCHours() < notBefore) channel = "sms";

  return { scored: [{ id: c.id, name: c.name, score, reasons, channel, overtime, minRate: c.min_rate }] };
}

async function rank(s: S): Promise<Partial<S>> {
  const ranked = [...s.scored].sort((a, b) => b.score - a.score);
  return { ranked, log: ranked.slice(0, 3).map((r, i) => `Rank #${i + 1} ${r.name} score=${r.score.toFixed(1)} via ${r.channel} [${r.reasons.join("; ")}]`) };
}

async function prepareWave(s: S) {
  const sh = s.shift;
  const next: Scored[] = [];
  const log: string[] = [];
  for (const r of s.ranked.filter((r) => !s.contacted.includes(r.id) && !s.declined.includes(r.id))) {
    if (next.length === WAVE_SIZE) break;
    // memory may have changed since ranking (she texted "no nights anymore" an hour ago) -> re-check
    const hard = (await getPreferences(s.agencyId, r.id)).find((p) => p.strength === "hard" && p.type === "avoid_shift_time" && p.value === sh.shiftTime);
    if (hard) { log.push(`Skipped ${r.name}: newly learned "${hard.text}"`); continue; }
    next.push(r);
  }
  if (!next.length || s.wave >= MAX_WAVES) {
    return new Command({ goto: "escalate", update: { log: [...log, `No more candidates to try after wave ${s.wave}`] } });
  }
  const drafts: Draft[] = next.map((r) => {
    const offerCode = offerCodeFor(s.threadId, r.id);
    const rate = Math.max(sh.rate, Math.min(r.minRate, sh.maxRate));
    return {
      caregiverId: r.id, name: r.name, channel: r.channel, offerCode,
      text: `Hi ${r.name.split(" ")[0]}, ${sh.facilityName} needs a ${sh.role} ${fmtTime(sh.startsAt)} (${sh.hours}h, $${rate}/h). Reply YES ${offerCode} to take it or NO ${offerCode}.`,
    };
  });
  return new Command({ goto: "review_drafts", update: { wave: s.wave + 1, drafts, log } });
}

async function reviewDrafts(s: S): Promise<Partial<S> | Command> {
  const [agency] = await db.select({ review: agencies.reviewOutbound }).from(agencies).where(eq(agencies.id, s.agencyId));
  if (!agency?.review) return {};
  // HITL "draft review" mode: nothing reaches a real nurse until a human approves it.
  const ev = interrupt<WaitPayload, MailboxEvent>({
    type: "review_outbound", accepts: ["human_decision", "cancel"],
    title: `Approve wave ${s.wave} outreach for ${s.shift.facilityName}`, drafts: s.drafts, options: ["approve", "reject"],
  });
  if (ev.kind === "cancel") return cancelled(ev);
  const d = ev.payload;
  switch (d.action) {
    case "approve": return { drafts: d.edits?.drafts ?? s.drafts, log: [`${d.decidedBy} approved wave ${s.wave} drafts`] };
    case "reject": return new Command({ goto: "escalate", update: { log: [`${d.decidedBy} rejected wave ${s.wave} drafts`] } });
    default: throw new Error(`review_drafts: unexpected action ${d.action}`);
  }
}

async function sendWave(s: S): Promise<Partial<S>> {
  const sh = s.shift;
  for (const d of s.drafts) {
    await db.insert(offers).values({ code: d.offerCode, agencyId: s.agencyId, threadId: s.threadId, shiftId: sh.id, caregiverId: d.caregiverId, wave: s.wave, channel: d.channel })
      .onConflictDoNothing();
    const { phone } = await contact(d.caregiverId);
    if (d.channel === "call") {
      const prefs = s.memory.perCaregiver[d.caregiverId]?.prefs.map((p) => p.text).join("; ") || "none";
      await callTool("comms.start_call", {
        to: phone,
        vars: {                                     // Retell dynamic variables -> the voice agent's prompt
          nurse_name: d.name, facility: sh.facilityName, shift_time: fmtTime(sh.startsAt), hours: String(sh.hours),
          rate: String(sh.rate), max_rate: String(sh.maxRate), offer_code: d.offerCode, known_preferences: prefs,
          facility_notes: [sh.facilityNotes, ...s.memory.facilityFacts].filter(Boolean).join(" "), opening: d.text,
        },
      }, ctx(s, `offer:${d.caregiverId}`));
    } else {
      await callTool("comms.send_sms", { to: phone, body: d.text }, ctx(s, `offer:${d.caregiverId}`));
    }
  }
  // Wake up after the wave timeout - but never later than 90 min before the shift.
  const fireAt = new Date(Math.min(addMinutes(clock.now(), WAVE_TIMEOUT_MIN).getTime(), addMinutes(new Date(sh.startsAt), -90).getTime()));
  await setTimer({ id: `${s.threadId}:wave:${s.wave}`, agencyId: s.agencyId, threadId: s.threadId, kind: "wave_timeout", fireAt, payload: { wave: s.wave } });
  return {
    contacted: s.drafts.map((d) => d.caregiverId),
    log: [`Wave ${s.wave}: offered to ${s.drafts.map((d) => `${d.name} (${d.channel}, code ${d.offerCode})`).join(", ")}`],
  };
}

/** The pause. Nothing runs and no worker is held while the thread sits here. */
async function wait(s: S) {
  const ev = interrupt<WaitPayload, MailboxEvent>({ type: "wait_event", accepts: ["nurse_reply", "timer", "cancel"], wave: s.wave, shiftId: s.shiftId });
  if (ev.kind === "cancel") return cancelled(ev);
  return new Command({ goto: "interpret", update: { event: ev, reply: null } });
}

/** The only LLM call on the reply path. Its output is checkpointed before any decision is made. */
async function interpret(s: S): Promise<Partial<S>> {
  if (s.event?.kind !== "nurse_reply") return { reply: null };
  return { reply: await parseReply(s.event.payload.text, { facilityId: s.shift.facilityId }) };
}

async function handleEvent(s: S): Promise<Command> {
  const ev = s.event!;
  if (s.seenEvents.includes(ev.id)) return new Command({ goto: "wait" });          // duplicate delivery
  const seen = { seenEvents: [ev.id] };

  if (ev.kind === "timer") {
    if (ev.payload.timerKind === "deadline") return new Command({ goto: "escalate", update: { ...seen, log: ["Hard deadline reached (2h before start)"] } });
    if (ev.payload.wave !== s.wave) return new Command({ goto: "wait", update: seen });   // stale timer from an old wave
    const silent = await db.update(offers).set({ status: "no_response", lastEventId: ev.id })
      .where(and(eq(offers.threadId, s.threadId), eq(offers.wave, s.wave), or(eq(offers.status, "sent"), eq(offers.lastEventId, ev.id))))
      .returning({ id: offers.caregiverId });
    return new Command({ goto: "prepare_wave", update: { ...seen, declined: silent.map((r) => r.id), log: [`Wave ${s.wave} timed out; no reply from ${silent.length} nurse(s)`] } });
  }

  // ---- nurse_reply (already interpreted and checkpointed by `interpret`)
  const { caregiverId, offerCode } = ev.payload;
  const reply = s.reply!;
  const nurse = s.ranked.find((r) => r.id === caregiverId);
  const [offer] = await db.select().from(offers).where(and(eq(offers.threadId, s.threadId), eq(offers.caregiverId, caregiverId)));
  if (!offer || !nurse) return new Command({ goto: "wait", update: { ...seen, log: [`Ignored reply from unknown nurse ${caregiverId}`] } });

  // Memory WRITE path (async, per-nurse queue): preferences mentioned in passing are learned in the background.
  if (reply.preferences.length) {
    await addMemoryJob(db, "memory_extract", { agencyId: s.agencyId, caregiverId, text: ev.payload.text, facilityId: s.shift.facilityId, source: "nurse_stated" }, `extract:${ev.id}`);
  }

  if (reply.intent === "accept" || reply.intent === "counter") {
    const rate = reply.intent === "counter" ? reply.counterRate! : Math.max(s.shift.rate, Math.min(nurse.minRate, s.shift.maxRate));
    if (rate > s.shift.maxRate) {
      if (await moveOffer(offerCode, "declined", ["sent"], ev.id)) {
        await textNurse(s, caregiverId, (f) => `Sorry ${f}, the most this facility can pay is $${s.shift.maxRate}/h.`, `toohigh:${ev.id}`);
      }
      return new Command({ goto: "wait", update: { ...seen, declined: [caregiverId], log: [`${nurse.name} countered $${rate}/h > max $${s.shift.maxRate}`] } });
    }
    const moved = await moveOffer(offerCode, reply.intent === "accept" ? "accepted" : "countered", ["sent"], ev.id);
    maybeCrash("handle_event.after_offer_update");   // tests: replaying this node must still take the accept branch
    if (!moved) return lateReply(s, ev, offer.status, nurse.name, seen);
    return new Command({
      goto: "check_policy",
      update: { ...seen, chosen: { caregiverId, name: nurse.name, rate, offerCode }, log: [`${nurse.name} ${reply.intent === "accept" ? "accepted" : `countered at $${rate}/h`}`] },
    });
  }

  if (reply.intent === "decline") {
    if (!(await moveOffer(offerCode, "declined", ["sent"], ev.id))) return lateReply(s, ev, offer.status, nurse.name, seen);
    const [{ n }] = await db.select({ n: count() }).from(offers).where(and(eq(offers.threadId, s.threadId), eq(offers.wave, s.wave), eq(offers.status, "sent")));
    // Whole wave said no? Don't wait for the timeout - move on now.
    return new Command({ goto: n === 0 ? "prepare_wave" : "wait", update: { ...seen, declined: [caregiverId], log: [`${nurse.name} declined`] } });
  }

  // question / unclear -> answer from facility notes + semantic memory, keep waiting
  const facts = [s.shift.facilityNotes, ...s.memory.facilityFacts].filter(Boolean).join(" ");
  await textNurse(s, caregiverId, () => `${facts || "A coordinator will follow up."} Reply YES ${offerCode} or NO ${offerCode}.`, `answer:${ev.id}`);
  return new Command({ goto: "wait", update: { ...seen, log: [`Answered a question from ${nurse.name}`] } });
}

async function lateReply(s: S, ev: MailboxEvent, status: string, name: string, seen: Partial<S>) {
  await textNurse(s, ev.payload.caregiverId, (f) => `Thanks ${f}! This shift is no longer available.`, `late:${ev.id}`);
  return new Command({ goto: "wait", update: { ...seen, log: [`Late reply from ${name} ignored (offer ${status})`] } });
}

async function checkPolicy(s: S) {
  const c = s.chosen!;
  const [nurse] = await db.select({ hours: caregivers.hoursThisWeek }).from(caregivers).where(eq(caregivers.id, c.caregiverId));
  const risks: string[] = [];
  if (nurse.hours + s.shift.hours > OVERTIME_HOURS) risks.push("overtime");
  if (c.rate > s.shift.rate) risks.push("rate_above_posted");
  // PROCEDURAL MEMORY: rules learned from human corrections become policy. Read fresh, not from state.
  const learned = (await getProcedures(s.agencyId)).filter((p) => p.rule?.type === "require_approval").map((p) => p.rule!.when);
  if (learned.includes("first_shift_at_facility")) {
    const [{ n }] = await db.select({ n: count() }).from(shifts)
      .where(and(eq(shifts.caregiverId, c.caregiverId), eq(shifts.facilityId, s.shift.facilityId), inArray(shifts.status, ["completed", "filled"])));
    if (n === 0) risks.push("first_shift_at_facility");
  }
  if (!risks.length) return new Command({ goto: "book", update: { risks, approvedVia: { by: "policy:auto" }, log: ["No risks -> auto-approved by policy"] } });
  return new Command({ goto: "approval", update: { risks, log: [`Needs human approval: ${risks.join(", ")}`] } });
}

async function approval(s: S): Promise<Command> {
  const c = s.chosen!;
  const ev = interrupt<WaitPayload, MailboxEvent>({
    type: "approval", accepts: ["human_decision", "cancel"],
    title: `Book ${c.name} for ${s.shift.facilityName} ${fmtTime(s.shift.startsAt)} at $${c.rate}/h?`,
    risks: s.risks, options: ["approve", "edit", "reject"], caregiverId: c.caregiverId, rate: c.rate,
  });
  if (ev.kind === "cancel") return cancelled(ev);
  const d = ev.payload;                              // shape already validated per kind by the gateway
  const approvalRef = { by: d.decidedBy, approvalId: d.approvalId };
  switch (d.action) {
    case "approve":
      return new Command({ goto: "book", update: { approvedVia: approvalRef, log: [`Approved by ${d.decidedBy}`] } });
    case "edit":
      return new Command({ goto: "book", update: { approvedVia: approvalRef, chosen: { ...c, rate: d.edits.rate }, log: [`${d.decidedBy} edited rate to $${d.edits.rate}/h and approved`] } });
    case "reject": {
      // tell the nurse, learn from the reason (reflection -> procedural memory), try someone else
      if (await moveOffer(c.offerCode, "declined", ["accepted", "countered"], `approval:${d.approvalId}`)) {
        await textNurse(s, c.caregiverId, (f) => `Thanks ${f} - we've gone another way for this shift.`, `rejected:${c.caregiverId}`);
      }
      await addMemoryJob(db, "memory_reflect", { agencyId: s.agencyId, approvalId: d.approvalId, decision: "reject", reason: d.reason,
        context: `Agent wanted to book ${c.name} at ${s.shift.facilityName}; risks: ${s.risks.join(",")}` }, `reflect:${d.approvalId}`);
      return new Command({ goto: "prepare_wave", update: { chosen: null, declined: [c.caregiverId], log: [`Rejected by ${d.decidedBy}: ${d.reason}`] } });
    }
    default:
      throw new Error(`approval: unexpected action ${d.action}`);
  }
}

async function book(s: S): Promise<Command> {
  const c = s.chosen!;
  try {
    await callTool("scheduler.book_shift", { shiftId: s.shiftId, caregiverId: c.caregiverId, rate: c.rate }, ctx(s, "book", s.approvedVia!));
  } catch (e) {
    if (!(e instanceof ToolConflict)) throw e;       // transient -> job retry
    const why = (e as Error).message;
    await textNurse(s, c.caregiverId, (f) => `Sorry ${f}, we couldn't book this one (${why}).`, `conflict:${c.caregiverId}`);
    // The DATABASE said no. Nurse double-booked -> try the next nurse. Shift itself gone -> stop.
    if (why.includes("overlapping")) {
      return new Command({ goto: "prepare_wave", update: { chosen: null, declined: [c.caregiverId], log: [`Booking conflict for ${c.name}: ${why}`] } });
    }
    return new Command({ goto: "finish", update: { outcome: "conflict", log: [`Booking conflict: ${why}`] } });
  }
  maybeCrash("after_book");                          // tests: die after booking, before the node finishes
  await textNurse(s, c.caregiverId, (f) => `You're booked, ${f}: ${s.shift.facilityName} ${fmtTime(s.shift.startsAt)} at $${c.rate}/h. ${s.shift.facilityNotes ?? ""}`.trim(), `confirm:${c.caregiverId}`);
  return new Command({ goto: "finish", update: { outcome: "filled", log: [`BOOKED ${c.name} at $${c.rate}/h (approved by ${s.approvedVia!.by})`] } });
}

async function escalate(s: S): Promise<Command> {
  const ev = interrupt<WaitPayload, MailboxEvent>({
    type: "escalation", accepts: ["human_decision", "cancel"],
    title: `Could not fill ${s.shift.facilityName} ${fmtTime(s.shift.startsAt)} - please take over`,
    tried: s.contacted, declined: s.declined,
    topCandidates: (s.ranked ?? []).slice(0, 5).map((r) => ({ id: r.id, name: r.name, score: r.score })),
    options: ["assign", "cancel"],
  });
  if (ev.kind === "cancel") return cancelled(ev);
  const d = ev.payload;
  switch (d.action) {
    case "assign": {
      const [cg] = await db.select({ id: caregivers.id, name: caregivers.name }).from(caregivers).where(eq(caregivers.id, d.edits.caregiverId));
      return new Command({
        goto: "book",
        update: { approvedVia: { by: d.decidedBy, approvalId: d.approvalId }, chosen: { caregiverId: cg.id, name: cg.name, rate: d.edits.rate ?? s.shift.rate, offerCode: "manual" },
          log: [`${d.decidedBy} manually assigned ${cg.name}`] },
      });
    }
    case "cancel":
      await callTool("scheduler.cancel_shift", { shiftId: s.shiftId, reason: d.reason ?? "unfilled" }, ctx(s, "cancel", { by: d.decidedBy, approvalId: d.approvalId }));
      return new Command({ goto: "finish", update: { outcome: "cancelled", log: [`${d.decidedBy} cancelled the shift`] } });
    default:
      throw new Error(`escalate: unexpected action ${d.action}`);
  }
}

async function finish(s: S): Promise<Partial<S>> {
  // 1) withdraw every offer still out, so nobody else shows up (select -> text -> update: replay-safe)
  const open = await db.select().from(offers).where(and(eq(offers.threadId, s.threadId), eq(offers.status, "sent")));
  for (const o of open) {
    await textNurse(s, o.caregiverId, (f) => `Thanks ${f}, the ${s.shift.facilityName} shift is no longer available.`, `withdraw:${o.caregiverId}`);
  }
  const all = await db.select().from(offers).where(eq(offers.threadId, s.threadId));
  await db.transaction(async (tx) => {
    await tx.update(offers).set({ status: "withdrawn" }).where(and(eq(offers.threadId, s.threadId), eq(offers.status, "sent")));
    await cancelTimers(s.threadId, tx);
    // 2) EPISODIC MEMORY (background, per-nurse queue, idempotent keys)
    for (const o of all) {
      const assigned = s.outcome === "filled" && o.caregiverId === s.chosen?.caregiverId;
      const signal = assigned ? "assigned" : o.status === "declined" ? "declined" : o.status === "no_response" ? "no_response" : null;
      if (!signal) continue;
      const name = (s.ranked ?? []).find((r) => r.id === o.caregiverId)?.name ?? o.caregiverId;
      await addMemoryJob(tx, "memory_episode", {
        agencyId: s.agencyId, caregiverId: o.caregiverId, key: `${s.threadId}:${o.caregiverId}`,
        episode: { text: `${name} ${signal} a ${s.shift.shiftTime} ${s.shift.role} shift at ${s.shift.facilityName}`,
          caregiverId: o.caregiverId, facilityId: s.shift.facilityId, shiftId: s.shiftId, signal, shiftTime: s.shift.shiftTime },
      }, `episode:${s.threadId}:${o.caregiverId}`);
    }
    // 3) FAN-IN: tell the parent (if any) that this child is done - same transaction
    if (s.parentThreadId) {
      await deliver(tx, { agencyId: s.agencyId, threadId: s.parentThreadId, kind: "child_done",
        payload: { childThreadId: s.threadId, shiftId: s.shiftId, outcome: s.outcome, caregiverId: s.chosen?.caregiverId ?? null }, dedupeKey: `child_done:${s.threadId}` });
    }
  });
  return { log: [`Finished: ${s.outcome}`] };
}

// ------------------------------------------------------------------ wiring
export function buildShiftFillGraph() {
  return new StateGraph(ShiftFillState)
    .addNode("load_shift", loadShift, { retryPolicy: { maxAttempts: 3 } })   // transient errors retried in-process
    .addNode("find_candidates", findCandidates)
    .addNode("score_candidate", scoreCandidate as any)
    .addNode("rank", rank)
    .addNode("prepare_wave", prepareWave, { ends: ["review_drafts", "escalate"] })
    .addNode("review_drafts", reviewDrafts, { ends: ["escalate", "finish"] })
    .addNode("send_wave", sendWave, { retryPolicy: { maxAttempts: 3 } })
    .addNode("wait", wait, { ends: ["interpret", "finish"] })
    .addNode("interpret", interpret, { retryPolicy: { maxAttempts: 3 } })
    .addNode("handle_event", handleEvent, { ends: ["wait", "prepare_wave", "check_policy", "escalate"] })
    .addNode("check_policy", checkPolicy, { ends: ["book", "approval"] })
    .addNode("approval", approval, { ends: ["book", "prepare_wave", "finish"] })
    .addNode("book", book, { ends: ["finish", "prepare_wave"] })
    .addNode("escalate", escalate, { ends: ["book", "finish"] })
    .addNode("finish", finish)
    .addEdge(START, "load_shift")
    .addEdge("load_shift", "find_candidates")
    .addConditionalEdges("find_candidates", fanOutScoring, ["score_candidate", "escalate"])
    .addEdge("score_candidate", "rank")              // fan-in: rank runs after ALL branches finish
    .addEdge("rank", "prepare_wave")
    .addEdge("review_drafts", "send_wave")
    .addEdge("send_wave", "wait")
    .addEdge("interpret", "handle_event")
    .addEdge("finish", END);
}
