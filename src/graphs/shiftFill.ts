// SHIFT-FILL AGENT: a long-running graph, one thread per open shift.
//
//  START
//    -> load_shift          open the shift in the facility system, set a hard deadline timer
//    -> find_candidates     HARD filters in SQL (license, certs, overlap, distance, pay floor)
//                           + memory hard prefs ("no nights") -> explainable exclusions
//    -> score_candidate xN  PARALLEL fan-out with Send (map) ...
//    -> rank                ... fan-in (reduce): sort, keep reasons
//    -> prepare_wave        pick next WAVE_SIZE nurses, choose channel (sms/call), draft messages
//    -> review_drafts       HITL (optional per agency): a human approves/edits outbound text
//    -> send_wave           send (idempotent tools), record offers, set wave timer
//    -> wait                interrupt(): the thread PAUSES here for minutes or hours, using no compute
//    -> handle_event        nurse_reply | timer  -> Command(goto=...)
//         accept/counter -> check_policy -> (approval) -> book -> finish
//         decline        -> wait / next wave
//         timeout        -> next wave / escalate
//    -> escalate            HITL handoff: a coordinator assigns someone or cancels the shift
//    -> finish              write episodes (memory) + tell the parent thread we are done
//
// RULES for nodes that call interrupt() (LangGraph re-runs the WHOLE node on resume):
//   * keep interrupt() at the top of the node, side effects only AFTER it, or
//   * make side effects idempotent (deterministic idempotency keys / ON CONFLICT DO NOTHING).

import { Annotation, Command, END, interrupt, Send, START, StateGraph } from "@langchain/langgraph";
import { addMinutes, clock, hoursBetween } from "../clock.js";
import { MAX_WAVES, OVERTIME_HOURS, URGENT_HOURS, WAVE_SIZE, WAVE_TIMEOUT_MIN } from "../config.js";
import { one, q } from "../db.js";
import { parseReply } from "../llm.js";
import { maybeCrash } from "../faults.js";
import { getProcedures, recallForShift, type CandidateMemory, type Procedure } from "../memory.js";
import { deliverToThread, enqueue } from "../outbox.js";
import { cancelTimers, setTimer } from "../timers.js";
import { callTool, ToolConflict } from "../tools.js";
import { appendList, fmtTime, offerCodeFor, shiftTimeOf, type MailboxEvent, type WaitPayload } from "./common.js";

// ------------------------------------------------------------------ state
type Shift = {
  id: string; facility_id: string; facility_name: string; facility_notes: string | null; requirements: string[];
  role: string; specialty: string | null; starts_at: string; ends_at: string; rate: number; max_rate: number;
  shiftTime: "day" | "evening" | "night"; hours: number; urgent: boolean;
};
type Scored = {
  id: string; name: string; phone: string; score: number; reasons: string[];
  channel: "sms" | "call"; overtime: boolean; minRate: number;
};
type Draft = { caregiverId: string; name: string; phone: string; channel: "sms" | "call"; offerCode: string; text: string };
type Chosen = { caregiverId: string; name: string; phone: string; rate: number; offerCode: string };

export const ShiftFillState = Annotation.Root({
  threadId: Annotation<string>,
  agencyId: Annotation<string>,
  shiftId: Annotation<string>,
  parentThreadId: Annotation<string | null>,
  excludeCaregiverId: Annotation<string | null>,   // the nurse who called out
  shift: Annotation<Shift>,
  memory: Annotation<{ perCaregiver: Record<string, CandidateMemory>; procedures: Procedure[]; facilityFacts: string[] }>,
  candidates: Annotation<any[]>,
  scored: appendList<Scored>(),                    // written by N parallel score_candidate branches
  ranked: Annotation<Scored[]>,
  wave: Annotation<number>,
  contacted: appendList<string>(),
  declined: appendList<string>(),
  drafts: Annotation<Draft[]>,
  event: Annotation<MailboxEvent | null>,
  seenEvents: appendList<string>(),                // idempotent consumer: ignore duplicate deliveries
  chosen: Annotation<Chosen | null>,
  risks: Annotation<string[]>,
  approvedBy: Annotation<string | null>,
  outcome: Annotation<string | null>,
  log: appendList<string>(),                       // human-readable timeline (great for learning + audit)
});
type S = typeof ShiftFillState.State;

const tool = (s: S, name: Parameters<typeof callTool>[0], args: any, key: string, approvedBy?: string) =>
  callTool(name, args, { threadId: s.threadId, idempotencyKey: `${s.threadId}:${key}`, approvedBy });

async function text(s: S, phone: string, body: string, key: string) {
  await tool(s, "comms.send_sms", { to: phone, body }, `sms:${key}`);
}

// ------------------------------------------------------------------ nodes
async function loadShift(s: S): Promise<Partial<S>> {
  const row = await tool(s, "scheduler.get_shift", { shiftId: s.shiftId }, "get_shift");
  await tool(s, "scheduler.open_shift", { shiftId: s.shiftId, reason: "needs coverage" }, "open_shift");
  const shift: Shift = {
    ...row, starts_at: new Date(row.starts_at).toISOString(), ends_at: new Date(row.ends_at).toISOString(),
    shiftTime: shiftTimeOf(row.starts_at),
    hours: hoursBetween(new Date(row.starts_at), new Date(row.ends_at)),
    urgent: hoursBetween(clock.now(), new Date(row.starts_at)) < URGENT_HOURS,
  };
  // Hard deadline: if still unfilled 2h before start, a human must take over.
  await setTimer(`${s.threadId}:deadline`, s.threadId, "deadline", addMinutes(new Date(shift.starts_at), -120));
  return {
    shift, wave: 0, chosen: null, risks: [], approvedBy: null, outcome: null,
    log: [`Opened ${shift.role} ${shift.shiftTime} shift at ${shift.facility_name} (${fmtTime(shift.starts_at)}, $${shift.rate}/h${shift.urgent ? ", URGENT" : ""})`],
  };
}

async function findCandidates(s: S): Promise<Partial<S>> {
  const sh = s.shift;
  // 1) HARD constraints in SQL. Rules that must never be violated live in code, not in a prompt.
  const rows = await q(
    `SELECT c.*, sqrt(power(c.x - f.x, 2) + power(c.y - f.y, 2)) AS distance
     FROM caregivers c JOIN facilities f ON f.id = $2
     WHERE c.agency_id = $1 AND c.active AND c.license = $3
       AND c.certs @> f.requirements                                  -- holds every required cert
       AND c.id <> coalesce($4, '')
       AND c.min_rate <= $5                                           -- facility can afford them
       AND NOT EXISTS (SELECT 1 FROM shifts o WHERE o.caregiver_id = c.id AND o.status IN ('scheduled','filled')
                       AND o.starts_at < $7 AND o.ends_at > $6)     -- no overlapping shift
     ORDER BY c.id`,
    [s.agencyId, sh.facility_id, sh.role, s.excludeCaregiverId, sh.max_rate, sh.starts_at, sh.ends_at],
  );
  const inRange = rows.filter((c) => c.distance <= c.max_distance_mi);
  // 2) MEMORY read path: learned preferences + episodes for each remaining nurse.
  const memory = await recallForShift(s.agencyId, { facility_id: sh.facility_id, shiftTime: sh.shiftTime }, inRange.map((c) => c.id));
  const log: string[] = [];
  const candidates = inRange.filter((c) => {
    const hard = memory.perCaregiver[c.id].prefs.find(
      (p) => p.strength === "hard" &&
        ((p.type === "avoid_shift_time" && p.value === sh.shiftTime) || (p.type === "avoid_facility" && p.value === sh.facility_id)),
    );
    if (hard) log.push(`Excluded ${c.name}: memory says "${hard.text}" (${hard.source})`);
    return !hard;
  });
  log.push(`${rows.length} matched license/certs/availability, ${inRange.length} within travel distance, ${candidates.length} after memory filters`);
  return { candidates, memory, log };
}

/** Conditional edge -> fan out one Send per candidate (map step). */
function fanOutScoring(s: S) {
  if (!s.candidates.length) return "escalate";
  return s.candidates.map(
    (c) => new Send("score_candidate", { candidate: c, shift: s.shift, mem: s.memory.perCaregiver[c.id] }),
  );
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

  // soft preferences from memory
  for (const p of mem.prefs) {
    if (p.type === "avoid_shift_time" && p.value === sh.shiftTime) add(-3, `memory: ${p.text}`);
    if (p.type === "avoid_facility" && p.value === sh.facility_id) add(-3, `memory: ${p.text}`);
    if (p.type === "prefer_facility" && p.value === sh.facility_id) add(2, `memory: ${p.text}`);
  }
  // episodic memory: what happened last times
  for (const e of mem.episodes) {
    if (e.facilityId !== sh.facility_id) continue;
    if (e.signal === "assigned") add(1, `episode: worked here before`);
    if (e.signal === "declined" || e.signal === "no_response") add(-1, `episode: ${e.signal} here before`);
  }

  // channel: memory beats profile; urgent shifts get a call unless the nurse asked for texts only
  const chanPref = mem.prefs.find((p) => p.type === "channel");
  let channel: "sms" | "call" = (chanPref?.value as any) ?? c.preferred_channel;
  if (sh.urgent && chanPref?.strength !== "hard") channel = "call";
  const notBefore = Number(mem.prefs.find((p) => p.type === "contact_after_hour")?.value ?? c.contact_start);
  if (channel === "call" && clock.now().getUTCHours() < notBefore) channel = "sms";   // texts are less intrusive

  return { scored: [{ id: c.id, name: c.name, phone: c.phone, score, reasons, channel, overtime, minRate: c.min_rate }] };
}

async function rank(s: S): Promise<Partial<S>> {
  const ranked = [...s.scored].sort((a, b) => b.score - a.score);
  return {
    ranked,
    log: ranked.slice(0, 3).map((r, i) => `Rank #${i + 1} ${r.name} score=${r.score.toFixed(1)} via ${r.channel} [${r.reasons.join("; ")}]`),
  };
}

async function prepareWave(s: S) {
  const remaining = s.ranked.filter((r) => !s.contacted.includes(r.id) && !s.declined.includes(r.id));
  if (!remaining.length || s.wave >= MAX_WAVES) {
    return new Command({ goto: "escalate", update: { log: [`No more candidates to try after wave ${s.wave}`] } });
  }
  const sh = s.shift;
  const drafts: Draft[] = remaining.slice(0, WAVE_SIZE).map((r) => {
    const offerCode = offerCodeFor(s.threadId, r.id);
    const first = r.name.split(" ")[0];
    const rate = Math.max(sh.rate, Math.min(r.minRate, sh.max_rate));
    return {
      caregiverId: r.id, name: r.name, phone: r.phone, channel: r.channel, offerCode,
      text: `Hi ${first}, ${sh.facility_name} needs a ${sh.role} ${fmtTime(sh.starts_at)} (${sh.hours}h, $${rate}/h). ` +
        `Reply YES ${offerCode} to take it or NO ${offerCode}.`,
    };
  });
  return new Command({ goto: "review_drafts", update: { wave: s.wave + 1, drafts } });
}

async function reviewDrafts(s: S): Promise<Partial<S> | Command> {
  const agency = await one(`SELECT review_outbound FROM agencies WHERE id = $1`, [s.agencyId]);
  if (!agency?.review_outbound) return {};
  // HITL "draft review" mode: nothing reaches a real nurse until a human approves it.
  const d = interrupt<WaitPayload, MailboxEvent>({
    type: "review_outbound", accepts: ["human_decision"],
    title: `Approve wave ${s.wave} outreach for ${s.shift.facility_name}`, drafts: s.drafts,
  }).payload;
  if (d.action === "reject") return new Command({ goto: "escalate", update: { log: [`Coordinator rejected wave ${s.wave} drafts`] } });
  const drafts = (d.edits?.drafts as Draft[] | undefined) ?? s.drafts;   // human may edit the text
  return { drafts, log: [`Coordinator ${d.decidedBy} approved wave ${s.wave} drafts`] };
}

async function sendWave(s: S): Promise<Partial<S>> {
  const sh = s.shift;
  for (const d of s.drafts) {
    await q(`INSERT INTO offers (code, thread_id, shift_id, caregiver_id, wave, channel) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`,
      [d.offerCode, s.threadId, sh.id, d.caregiverId, s.wave, d.channel]);
    if (d.channel === "call") {
      const memory = s.memory.perCaregiver[d.caregiverId]?.prefs.map((p) => p.text).join("; ") || "none";
      await tool(s, "comms.start_call", {
        to: d.phone,
        vars: {                                   // Retell dynamic variables -> the voice agent's prompt
          nurse_name: d.name, facility: sh.facility_name, shift_time: fmtTime(sh.starts_at), hours: String(sh.hours),
          rate: String(sh.rate), max_rate: String(sh.max_rate), offer_code: d.offerCode, known_preferences: memory,
          facility_notes: [sh.facility_notes, ...s.memory.facilityFacts].filter(Boolean).join(" "), opening: d.text,
        },
      }, `offer:${d.caregiverId}`);
    } else {
      await text(s, d.phone, d.text, `offer:${d.caregiverId}`);
    }
  }
  // Wake up after the wave timeout - but never later than 90 min before the shift.
  const fireAt = new Date(Math.min(addMinutes(clock.now(), WAVE_TIMEOUT_MIN).getTime(), addMinutes(new Date(sh.starts_at), -90).getTime()));
  await setTimer(`${s.threadId}:wave:${s.wave}`, s.threadId, "wave_timeout", fireAt, { wave: s.wave });
  return {
    contacted: s.drafts.map((d) => d.caregiverId),
    log: [`Wave ${s.wave}: offered to ${s.drafts.map((d) => `${d.name} (${d.channel}, code ${d.offerCode})`).join(", ")}`],
  };
}

/** The pause. Nothing runs and no worker is held while the thread sits here. */
async function wait(s: S): Promise<Partial<S>> {
  const ev = interrupt<WaitPayload, MailboxEvent>({
    type: "wait_event", accepts: ["nurse_reply", "timer"], wave: s.wave, shiftId: s.shiftId,
  });
  return { event: ev };
}

async function handleEvent(s: S): Promise<Command> {
  const ev = s.event!;
  if (s.seenEvents.includes(ev.id)) return new Command({ goto: "wait" });          // duplicate delivery
  const seen = { seenEvents: [ev.id] };

  if (ev.kind === "timer") {
    if (ev.payload.timerKind === "deadline") {
      return new Command({ goto: "escalate", update: { ...seen, log: ["Hard deadline reached (2h before start)"] } });
    }
    if (ev.payload.wave !== s.wave) return new Command({ goto: "wait", update: seen });   // stale timer from an old wave
    const silent = (await q(`UPDATE offers SET status = 'no_response' WHERE thread_id = $1 AND wave = $2 AND status = 'sent' RETURNING caregiver_id`,
      [s.threadId, s.wave])).map((r) => r.caregiver_id);
    return new Command({
      goto: "prepare_wave",
      update: { ...seen, declined: silent, log: [`Wave ${s.wave} timed out; no reply from ${silent.length} nurse(s)`] },
    });
  }

  // ---- nurse_reply
  const { caregiverId, text: body } = ev.payload;
  const offer = await one(`SELECT * FROM offers WHERE thread_id = $1 AND caregiver_id = $2`, [s.threadId, caregiverId]);
  const nurse = s.ranked.find((r) => r.id === caregiverId);
  if (!offer || !nurse) return new Command({ goto: "wait", update: { ...seen, log: [`Ignored reply from unknown nurse ${caregiverId}`] } });

  const reply = await parseReply(body, { facilityId: s.shift.facility_id });
  // Memory WRITE path (async): preferences mentioned in passing ("no nights anymore") are learned in the background.
  if (reply.preferences.length) {
    await enqueue("memory.extract", { agencyId: s.agencyId, caregiverId, text: body, facilityId: s.shift.facility_id, source: "nurse_stated" }, `extract:${ev.id}`);
  }

  if (offer.status !== "sent") {
    await text(s, nurse.phone, `Thanks ${nurse.name.split(" ")[0]}! This shift is no longer available.`, `late:${ev.id}`);
    return new Command({ goto: "wait", update: { ...seen, log: [`Late reply from ${nurse.name} ignored (offer ${offer.status})`] } });
  }

  if (reply.intent === "accept" || reply.intent === "counter") {
    const rate = reply.intent === "counter" ? reply.counterRate! : Math.max(s.shift.rate, Math.min(nurse.minRate, s.shift.max_rate));
    if (rate > s.shift.max_rate) {
      await q(`UPDATE offers SET status = 'declined' WHERE code = $1`, [offer.code]);
      await text(s, nurse.phone, `Sorry ${nurse.name.split(" ")[0]}, the most this facility can pay is $${s.shift.max_rate}/h.`, `toohigh:${ev.id}`);
      return new Command({ goto: "wait", update: { ...seen, declined: [caregiverId], log: [`${nurse.name} countered $${rate}/h > max $${s.shift.max_rate}`] } });
    }
    await q(`UPDATE offers SET status = $2 WHERE code = $1`, [offer.code, reply.intent === "accept" ? "accepted" : "countered"]);
    return new Command({
      goto: "check_policy",
      update: { ...seen, chosen: { caregiverId, name: nurse.name, phone: nurse.phone, rate, offerCode: offer.code },
        log: [`${nurse.name} ${reply.intent === "accept" ? "accepted" : `countered at $${rate}/h`}`] },
    });
  }

  if (reply.intent === "decline") {
    await q(`UPDATE offers SET status = 'declined' WHERE code = $1`, [offer.code]);
    const pending = await one(`SELECT count(*)::int AS n FROM offers WHERE thread_id = $1 AND wave = $2 AND status = 'sent'`, [s.threadId, s.wave]);
    // Whole wave said no? Don't wait for the timeout - move on now.
    return new Command({
      goto: pending.n === 0 ? "prepare_wave" : "wait",
      update: { ...seen, declined: [caregiverId], log: [`${nurse.name} declined`] },
    });
  }

  // question / unclear -> answer from facility notes + semantic memory, keep waiting
  const facts = [s.shift.facility_notes, ...s.memory.facilityFacts].filter(Boolean).join(" ");
  await text(s, nurse.phone, `${facts || "A coordinator will follow up."} Reply YES ${offer.code} or NO ${offer.code}.`, `answer:${ev.id}`);
  return new Command({ goto: "wait", update: { ...seen, log: [`Answered a question from ${nurse.name}`] } });
}

async function checkPolicy(s: S) {
  const c = s.chosen!;
  const nurse = await one(`SELECT hours_this_week FROM caregivers WHERE id = $1`, [c.caregiverId]);
  const risks: string[] = [];
  if (nurse.hours_this_week + s.shift.hours > OVERTIME_HOURS) risks.push("overtime");
  if (c.rate > s.shift.rate) risks.push("rate_above_posted");
  // PROCEDURAL MEMORY: rules learned from past human corrections become policy.
  // Read fresh (not from state) so a lesson learned 5 minutes ago already applies.
  const learned = (await getProcedures(s.agencyId)).filter((p) => p.rule?.type === "require_approval").map((p) => p.rule!.when);
  if (learned.includes("first_shift_at_facility")) {
    const prior = await one(`SELECT count(*)::int AS n FROM shifts WHERE caregiver_id = $1 AND facility_id = $2 AND status IN ('completed','filled') AND id <> $3`,
      [c.caregiverId, s.shift.facility_id, s.shiftId]);
    if (prior.n === 0) risks.push("first_shift_at_facility");
  }
  if (!risks.length) return new Command({ goto: "book", update: { risks, approvedBy: "policy:auto", log: ["No risks -> auto-approved by policy"] } });
  return new Command({ goto: "approval", update: { risks, log: [`Needs human approval: ${risks.join(", ")}`] } });
}

async function approval(s: S): Promise<Command> {
  const c = s.chosen!;
  const d = interrupt<WaitPayload, MailboxEvent>({
    type: "approval", accepts: ["human_decision"],
    title: `Book ${c.name} for ${s.shift.facility_name} ${fmtTime(s.shift.starts_at)} at $${c.rate}/h?`,
    risks: s.risks, options: ["approve", "edit", "reject"], caregiverId: c.caregiverId, rate: c.rate,
  }).payload;
  // --- everything below runs only after a human decided ---
  if (d.action === "approve") return new Command({ goto: "book", update: { approvedBy: d.decidedBy, log: [`Approved by ${d.decidedBy}`] } });
  if (d.action === "edit") {
    return new Command({
      goto: "book",
      update: { approvedBy: d.decidedBy, chosen: { ...c, rate: d.edits?.rate ?? c.rate }, log: [`${d.decidedBy} edited rate to $${d.edits?.rate}/h and approved`] },
    });
  }
  // reject: tell the nurse, learn from the reason, try someone else
  await q(`UPDATE offers SET status = 'declined' WHERE code = $1`, [c.offerCode]);
  await text(s, c.phone, `Thanks ${c.name.split(" ")[0]} - we've gone another way for this shift.`, `rejected:${c.caregiverId}`);
  if (d.reason) {
    await enqueue("memory.reflect", { agencyId: s.agencyId, approvalId: d.approvalId, decision: "reject", reason: d.reason,
      context: `Agent wanted to book ${c.name} at ${s.shift.facility_name}; risks: ${s.risks.join(",")}` }, `reflect:${d.approvalId}`);
  }
  return new Command({
    goto: "prepare_wave",
    update: { chosen: null, declined: [c.caregiverId], log: [`Rejected by ${d.decidedBy}: ${d.reason ?? "no reason"}`] },
  });
}

async function book(s: S): Promise<Partial<S>> {
  const c = s.chosen!;
  try {
    await tool(s, "scheduler.book_shift", { shiftId: s.shiftId, caregiverId: c.caregiverId, rate: c.rate }, "book", s.approvedBy!);
  } catch (e) {
    if (!(e instanceof ToolConflict)) throw e;                      // transient -> retry policy / worker retry
    await text(s, c.phone, `Sorry ${c.name.split(" ")[0]}, this shift was just filled.`, `conflict:${c.caregiverId}`);
    return { outcome: "filled_elsewhere", log: [`Booking conflict: ${(e as Error).message}`] };
  }
  maybeCrash("after_book");   // tests: die AFTER the booking but BEFORE the node finishes -> replay must not double-book
  await text(s, c.phone, `You're booked: ${s.shift.facility_name} ${fmtTime(s.shift.starts_at)} at $${c.rate}/h. ${s.shift.facility_notes ?? ""}`.trim(), `confirm:${c.caregiverId}`);
  // Withdraw every other outstanding offer so nobody else shows up.
  const others = await q(`UPDATE offers SET status = 'withdrawn' WHERE thread_id = $1 AND status = 'sent' RETURNING caregiver_id`, [s.threadId]);
  for (const o of others) {
    const r = (s.ranked ?? []).find((x) => x.id === o.caregiver_id);
    if (r) await text(s, r.phone, `Thanks ${r.name.split(" ")[0]}, the ${s.shift.facility_name} shift has been filled.`, `withdraw:${r.id}`);
  }
  return { outcome: "filled", log: [`BOOKED ${c.name} at $${c.rate}/h (approved by ${s.approvedBy})`] };
}

async function escalate(s: S): Promise<Command> {
  // Interrupt FIRST; the page to the manager happens via the approval SLA + approvals inbox.
  const d = interrupt<WaitPayload, MailboxEvent>({
    type: "escalation", accepts: ["human_decision"],
    title: `Could not fill ${s.shift.facility_name} ${fmtTime(s.shift.starts_at)} - please take over`,
    tried: s.contacted, declined: s.declined, topCandidates: (s.ranked ?? []).slice(0, 5).map((r) => ({ id: r.id, name: r.name, score: r.score })),
    options: ["assign", "cancel"],
  }).payload;
  if (d.action === "assign") {
    const cg = await one(`SELECT id, name, phone FROM caregivers WHERE id = $1`, [d.edits.caregiverId]);
    return new Command({
      goto: "book",
      update: { approvedBy: d.decidedBy, chosen: { caregiverId: cg.id, name: cg.name, phone: cg.phone, rate: d.edits.rate ?? s.shift.rate, offerCode: "manual" },
        log: [`${d.decidedBy} manually assigned ${cg.name}`] },
    });
  }
  await tool(s, "scheduler.cancel_shift", { shiftId: s.shiftId, reason: d.reason ?? "unfilled" }, "cancel", d.decidedBy);
  return new Command({ goto: "finish", update: { outcome: "cancelled", log: [`${d.decidedBy} cancelled the shift`] } });
}

async function finish(s: S): Promise<Partial<S>> {
  await cancelTimers(s.threadId);
  // EPISODIC MEMORY (background, idempotent keys): one episode per nurse we interacted with.
  const offers = await q(`SELECT caregiver_id, status FROM offers WHERE thread_id = $1`, [s.threadId]);
  for (const o of offers) {
    const assigned = s.outcome === "filled" && o.caregiver_id === s.chosen?.caregiverId;
    const signal = assigned ? "assigned" : o.status === "declined" ? "declined" : o.status === "no_response" ? "no_response" : null;
    if (!signal) continue;
    const name = (s.ranked ?? []).find((r) => r.id === o.caregiver_id)?.name ?? o.caregiver_id;
    await enqueue("memory.episode", {
      agencyId: s.agencyId, key: `${s.threadId}:${o.caregiver_id}`,
      episode: { text: `${name} ${signal} a ${s.shift.shiftTime} ${s.shift.role} shift at ${s.shift.facility_name}`,
        caregiverId: o.caregiver_id, facilityId: s.shift.facility_id, shiftId: s.shiftId, signal, shiftTime: s.shift.shiftTime },
    }, `episode:${s.threadId}:${o.caregiver_id}`);
  }
  // FAN-IN: tell the parent (if any) that this child is done.
  if (s.parentThreadId) {
    await deliverToThread(s.parentThreadId, "child_done",
      { childThreadId: s.threadId, shiftId: s.shiftId, outcome: s.outcome, caregiverId: s.chosen?.caregiverId ?? null }, `child_done:${s.threadId}`);
  }
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
    .addNode("review_drafts", reviewDrafts, { ends: ["escalate"] })
    .addNode("send_wave", sendWave, { retryPolicy: { maxAttempts: 3 } })
    .addNode("wait", wait)
    .addNode("handle_event", handleEvent, { ends: ["wait", "prepare_wave", "check_policy", "escalate"] })
    .addNode("check_policy", checkPolicy, { ends: ["book", "approval"] })
    .addNode("approval", approval, { ends: ["book", "prepare_wave"] })
    .addNode("book", book)
    .addNode("escalate", escalate, { ends: ["book", "finish"] })
    .addNode("finish", finish)
    .addEdge(START, "load_shift")
    .addEdge("load_shift", "find_candidates")
    .addConditionalEdges("find_candidates", fanOutScoring, ["score_candidate", "escalate"])
    .addEdge("score_candidate", "rank")                // fan-in: rank runs after ALL branches finish
    .addEdge("rank", "prepare_wave")
    .addEdge("review_drafts", "send_wave")
    .addEdge("send_wave", "wait")
    .addEdge("wait", "handle_event")
    .addEdge("book", "finish")
    .addEdge("finish", END);
}
