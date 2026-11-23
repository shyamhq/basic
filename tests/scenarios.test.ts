// SCENARIO TESTS - each one is an interview story. Read them like a script.
// Real Postgres (Drizzle + LangGraph checkpoints) and real graphile-worker; frozen clock.

import { and, eq } from "drizzle-orm";
import { afterAll, beforeEach, describe, expect, test } from "vitest";
import { addMinutes, clock } from "../src/clock.js";
import { db, pool } from "../src/db/index.js";
import { agencies, agentRuns, approvals, threadInbox, toolCalls } from "../src/db/schema.js";
import { faults, SimulatedCrash } from "../src/faults.js";
import { deliver } from "../src/mailbox.js";
import { addEpisode, consolidate, getMemoryStore, getPreferences, getProcedures, upsertPreference } from "../src/memory.js";
import { tomorrowAt } from "../src/seed.js";
import { escalateStaleApprovals, fireDueTimers, reapDeadRuns } from "../src/timers.js";
import {
  PHONE, decide, get, hours, lastMessageTo, nurseTexts, offerCode, pendingApprovals, post, postShift, reset, retryAndSettle,
  run, runs, settle, shift, smsTo, threadLog,
} from "./helpers.js";

beforeEach(async () => { faults.crashAt.clear(); faults.always.clear(); await reset(); });
afterAll(async () => { await getMemoryStore().stop(); await pool.end(); });

/** Fast-forward time and let the scheduler fire due timers. */
async function passTime(minutes: number) {
  clock.advanceMinutes(minutes);
  await fireDueTimers();
  return settle();
}
// A fresh day shift at Sunrise. Ranking: James, Maria (overtime) | Anita, Grace.
const postSunriseDay = (id = "sh_new") => postShift(id, "fac_sunrise", tomorrowAt(7), tomorrowAt(15), 24, 30, "geriatrics");
const DAY = "fill:sh_sunrise_day";
const EVE = "fill:sh_oak_eve";
const NEW = "fill:sh_new";

describe("orchestration", () => {
  test("1. call-out -> supervisor fans out one child thread per shift -> fan-in when all are covered", async () => {
    const { threadId } = await nurseTexts(PHONE.sofia, "Hi, I'm sick with a fever and can't make my shifts tomorrow");
    await settle();
    expect((await run(DAY)).status).toBe("waiting_event");          // paused at interrupt(), no worker held
    expect((await run(EVE)).status).toBe("waiting_event");
    expect((await run(threadId)).waitingFor).toBe("wait_children");
    expect(await lastMessageTo(PHONE.sofia)).toContain("finding cover for your 2 shift(s)");

    await nurseTexts(PHONE.james, `YES ${await offerCode(DAY, "cg_james")}`);
    await nurseTexts(PHONE.anita, `yes ${await offerCode(EVE, "cg_anita")}`);
    expect((await settle()).errors).toEqual([]);

    expect((await shift("sh_sunrise_day")).caregiverId).toBe("cg_james");
    expect((await shift("sh_oak_eve")).caregiverId).toBe("cg_anita");
    expect((await run(threadId)).status).toBe("done");
    expect(await threadLog(threadId)).toContain("All children done: 2/2 covered");
    expect((await smsTo(PHONE.maria)).filter((m) => m.body.includes("no longer available"))).toHaveLength(2);   // withdrawn twice
    expect((await get(`/api/threads/${DAY}/history`)).length).toBeGreaterThan(8);                              // every super-step checkpointed
  });

  test("2. nobody answers -> wave timeout -> next wave -> escalate to a human who assigns manually", async () => {
    await postSunriseDay();
    expect((await threadLog(NEW)).some((l) => l.startsWith("Wave 1: offered to James Okafor"))).toBe(true);
    await passTime(16);
    expect((await threadLog(NEW)).some((l) => l.startsWith("Wave 2: offered to Anita Reddy"))).toBe(true);
    await passTime(16);
    const [esc] = await pendingApprovals();
    expect(esc.kind).toBe("escalation");
    expect((await run(NEW)).status).toBe("waiting_human");
    expect((await decide(esc.id, "assign", { edits: { caregiverId: "cg_grace", rate: 25 } })).status).toBe(200);
    await settle();
    expect((await shift("sh_new")).caregiverId).toBe("cg_grace");
    expect((await threadLog(NEW)).at(-1)).toBe("Finished: filled");
  });

  test("3. hard filters in SQL are explainable; Send fan-out scores every candidate", async () => {
    await postSunriseDay();
    const log = await threadLog(NEW);
    expect(log).toContain("5 matched license/certs/availability, 4 within travel distance, 4 after memory filters");
    expect(log.filter((l) => l.startsWith("Rank #"))).toHaveLength(3);
  });

  test("4. double texting: a second call-out text joins the open conversation - no duplicate fill threads", async () => {
    const first = await nurseTexts(PHONE.sofia, "I'm sick, can't make my shifts tomorrow");
    await settle();
    const second = await nurseTexts(PHONE.sofia, "so sorry, I really can't make it tomorrow");
    expect(second.routed).toBe("intake-followup");
    await settle();
    expect((await runs("shiftfill")).map((r) => r.threadId).sort()).toEqual([DAY, EVE].sort());
    expect((await runs("intake")).map((r) => r.threadId)).toEqual([first.threadId]);
    expect(await lastMessageTo(PHONE.sofia)).toContain("already finding cover");
  });

  test("5. cancel: every pause accepts 'cancel'; open offers are withdrawn", async () => {
    await postSunriseDay();
    await post(`/api/threads/${NEW}/cancel`, { reason: "facility filled it internally" });
    await settle();
    expect((await run(NEW)).status).toBe("done");
    expect(await lastMessageTo(PHONE.james)).toContain("no longer available");
    expect((await threadLog(NEW)).at(-1)).toBe("Finished: cancelled");
  });
});

describe("human in the loop", () => {
  test("6. overtime -> approval gate -> coordinator EDITS the rate and approves (audited)", async () => {
    await postSunriseDay();
    await nurseTexts(PHONE.maria, `Yes ${await offerCode(NEW, "cg_maria")}`);
    await settle();
    const [a] = await pendingApprovals();
    expect([a.kind, a.payload.risks]).toEqual(["approval", ["overtime"]]);
    expect((await shift("sh_new")).status).toBe("open");               // nothing booked without a human
    await decide(a.id, "edit", { edits: { rate: 28 } });
    await settle();
    const s = await shift("sh_new");
    expect([s.caregiverId, s.rate]).toEqual(["cg_maria", 28]);
    const [call] = await db.select().from(toolCalls).where(eq(toolCalls.tool, "scheduler.book_shift"));
    expect(call.approvedBy).toBe("coordinator@test");
  });

  test("7. REJECT with a reason -> reflection writes procedural memory -> the learned rule gates the next booking", async () => {
    await postSunriseDay();
    await nurseTexts(PHONE.james, `NO ${await offerCode(NEW, "cg_james")}`);
    await nurseTexts(PHONE.maria, `yes ${await offerCode(NEW, "cg_maria")}`);
    await settle();
    const [a] = await pendingApprovals();
    await decide(a.id, "reject", { reason: "Never book someone's first shift at a facility without checking with me" });
    await settle();
    expect((await getProcedures("ag_bright"))[0].rule).toEqual({ type: "require_approval", when: "first_shift_at_facility" });
    expect(await lastMessageTo(PHONE.maria)).toContain("gone another way");
    await nurseTexts(PHONE.anita, `yes ${await offerCode(NEW, "cg_anita")}`);   // wave 2; Anita never worked at Sunrise
    await settle();
    expect((await pendingApprovals())[0].payload.risks).toContain("first_shift_at_facility");
  });

  test("8. emergency keywords bypass the model -> human; side effects before interrupt() happen once", async () => {
    const { threadId } = await nurseTexts(PHONE.sofia, "Resident fell in room 12 and is bleeding, what do I do");
    await settle();
    expect(await lastMessageTo(PHONE.manager)).toContain("EMERGENCY");
    const [a] = await pendingApprovals();
    expect(a.kind).toBe("emergency_ack");
    await decide(a.id, "approve", { reason: "Called the DON, resident stable" });
    await settle();
    expect((await run(threadId)).status).toBe("done");
    expect((await smsTo(PHONE.manager)).filter((m) => m.body.includes("EMERGENCY"))).toHaveLength(1);
  });

  test("9. low-confidence triage -> a human picks the intent", async () => {
    const { threadId } = await nurseTexts(PHONE.grace, "hmm about tomorrow");
    await settle();
    const [a] = await pendingApprovals();
    expect(a.kind).toBe("triage_review");
    await decide(a.id, "edit", { edits: { intent: "question" } });
    await settle();
    expect(await threadLog(threadId)).toContain("coordinator@test set intent = question");
  });

  test("10. review-every-message mode: nothing reaches a nurse until a human approves (and edits) the draft", async () => {
    await db.update(agencies).set({ reviewOutbound: true });
    await postSunriseDay();
    expect(await smsTo(PHONE.james)).toHaveLength(0);
    const [a] = await pendingApprovals();
    expect(a.kind).toBe("review_outbound");
    const drafts = a.payload.drafts.map((d: any) => ({ ...d, text: d.text.replace("Hi", "Good morning") }));
    await decide(a.id, "approve", { edits: { drafts } });
    await settle();
    expect(await lastMessageTo(PHONE.james)).toMatch(/^Good morning James/);
  });

  test("11. decisions are validated per kind: 'approve' on an escalation or an unknown caregiver -> 400, shift untouched", async () => {
    await postSunriseDay();
    await passTime(16); await passTime(16);
    const [esc] = await pendingApprovals();
    expect((await decide(esc.id, "approve")).status).toBe(400);
    expect((await decide(esc.id, "assign", { edits: { caregiverId: "cg_typo" } })).status).toBe(400);
    expect((await shift("sh_new")).status).toBe("open");
    expect((await pendingApprovals())[0].id).toBe(esc.id);                // still pending, still actionable
  });

  test("12. stale decision for an OLD interrupt is discarded; two clicks -> 409", async () => {
    await postSunriseDay();
    await nurseTexts(PHONE.maria, `yes ${await offerCode(NEW, "cg_maria")}`);
    await settle();
    const [a] = await pendingApprovals();
    await db.transaction((tx) => deliver(tx, { agencyId: "ag_bright", threadId: NEW, kind: "human_decision", payload: { action: "approve", interruptId: "old", decidedBy: "x" }, dedupeKey: "stale-1" }));
    await settle();
    expect((await db.select().from(threadInbox).where(eq(threadInbox.dedupeKey, "stale-1")))[0].status).toBe("stale");
    expect((await decide(a.id, "approve")).status).toBe(200);
    expect((await decide(a.id, "reject", { reason: "too late" })).status).toBe(409);
    await settle();
    expect((await shift("sh_new")).caregiverId).toBe("cg_maria");
  });

  test("13. approval SLA: unanswered for 10 minutes -> manager paged exactly once (flag + page job commit together)", async () => {
    await postSunriseDay();
    await nurseTexts(PHONE.maria, `yes ${await offerCode(NEW, "cg_maria")}`);
    await settle();
    clock.advanceMinutes(11);
    expect(await escalateStaleApprovals()).toBe(1);
    expect(await escalateStaleApprovals()).toBe(0);
    await settle();
    expect(await lastMessageTo(PHONE.manager)).toContain("waiting > 10 min");
  });
});

describe("long-running reliability", () => {
  test("14. MAILBOX: an event the graph can't use yet is buffered, then consumed in order", async () => {
    await postSunriseDay();
    await nurseTexts(PHONE.maria, `yes ${await offerCode(NEW, "cg_maria")}`);    // -> approval pending
    await settle();
    await nurseTexts(PHONE.james, `YES ${await offerCode(NEW, "cg_james")}`);     // arrives while waiting on a human
    await settle();
    const [buffered] = await db.select().from(threadInbox).where(and(eq(threadInbox.threadId, NEW), eq(threadInbox.kind, "nurse_reply"))).orderBy(threadInbox.id).offset(1);
    expect(buffered.status).toBe("pending");
    const [a] = await pendingApprovals();
    await decide(a.id, "reject", { reason: "Maria is too tired this week" });
    await settle();
    expect((await shift("sh_new")).caregiverId).toBe("cg_james");
  });

  test("15. duplicate webhook + nurse double-texting -> one booking; she hears 'you're all set', not 'unavailable'", async () => {
    await postSunriseDay();
    const code = await offerCode(NEW, "cg_james");
    await nurseTexts(PHONE.james, `YES ${code}`, "dup-1");
    expect((await nurseTexts(PHONE.james, `YES ${code}`, "dup-1")).duplicate).toBe(true);
    await settle();
    await nurseTexts(PHONE.james, `yes!! ${code}`, "dup-2");
    await settle();
    expect(await hours("cg_james")).toBe(32);
    expect(await db.select().from(toolCalls).where(eq(toolCalls.tool, "scheduler.book_shift"))).toHaveLength(1);
    expect(await lastMessageTo(PHONE.james)).toContain("you're booked for that shift");
  });

  test("16. crash after booking, before the node finished -> job retry resumes from the checkpoint, no double booking", async () => {
    await postSunriseDay();
    faults.crashAt.add("after_book");
    await nurseTexts(PHONE.james, `YES ${await offerCode(NEW, "cg_james")}`);
    expect((await settle()).errors[0]).toBeInstanceOf(SimulatedCrash);
    expect((await shift("sh_new")).status).toBe("filled");            // the side effect DID happen
    expect((await retryAndSettle()).errors).toEqual([]);
    expect((await run(NEW)).status).toBe("done");
    expect(await hours("cg_james")).toBe(32);                          // +8 once
    expect((await smsTo(PHONE.james)).filter((m) => m.body.startsWith("You're booked"))).toHaveLength(1);
  });

  test("17. [review C1/C2] crash right after invoke(), before ANY bookkeeping -> next drain rebuilds the approvals inbox", async () => {
    await postSunriseDay();
    faults.crashAt.add("worker.after_invoke");
    await nurseTexts(PHONE.maria, `yes ${await offerCode(NEW, "cg_maria")}`);
    await settle();
    expect(await pendingApprovals()).toHaveLength(0);                  // the crash window
    await retryAndSettle();
    const [a] = await pendingApprovals();
    expect(a.kind).toBe("approval");                                  // projection repaired from the checkpoint
    expect((await run(NEW)).status).toBe("waiting_human");
    const [yes] = await db.select().from(threadInbox).where(and(eq(threadInbox.threadId, NEW), eq(threadInbox.kind, "nurse_reply")));
    expect(yes.status).toBe("consumed");                              // not left pending -> no bogus "unavailable" later
    await decide(a.id, "approve");
    await settle();
    expect((await smsTo(PHONE.maria)).some((m) => m.body.includes("no longer available"))).toBe(false);
  });

  test("18. [review C3] crash after the offer update inside handle_event -> replay still takes the accept branch", async () => {
    await postSunriseDay();
    faults.crashAt.add("handle_event.after_offer_update");
    await nurseTexts(PHONE.james, `YES ${await offerCode(NEW, "cg_james")}`);
    await settle();
    await retryAndSettle();
    expect((await shift("sh_new")).caregiverId).toBe("cg_james");
    expect((await smsTo(PHONE.james)).some((m) => m.body.includes("no longer available"))).toBe(false);
  });

  test("19. [review C4] crash inside the booking transaction -> rolled back, retried, hours counted once", async () => {
    await postSunriseDay();
    faults.crashAt.add("tool.inside_tx");                              // first internal tool call after this is... the booking
    await nurseTexts(PHONE.james, `YES ${await offerCode(NEW, "cg_james")}`);
    await settle();
    await retryAndSettle();
    expect((await shift("sh_new")).caregiverId).toBe("cg_james");
    expect(await hours("cg_james")).toBe(32);
  });

  test("20. [review C6] the DATABASE forbids overlapping bookings: second booking conflicts, the agent moves on", async () => {
    await postSunriseDay("sh_a");
    await postShift("sh_b", "fac_sunrise", tomorrowAt(8), tomorrowAt(16), 24, 30, "geriatrics");   // overlaps sh_a
    await nurseTexts(PHONE.james, `YES ${await offerCode("fill:sh_a", "cg_james")}`);
    await nurseTexts(PHONE.james, `YES ${await offerCode("fill:sh_b", "cg_james")}`);
    await settle();
    expect((await shift("sh_a")).caregiverId).toBe("cg_james");
    expect((await shift("sh_b")).caregiverId).not.toBe("cg_james");
    expect((await threadLog("fill:sh_b")).some((l) => l.includes("overlapping"))).toBe(true);
    expect((await threadLog("fill:sh_b")).some((l) => l.startsWith("Wave 2"))).toBe(true);           // moved on
  });

  test("21. [review C9] a child that fails permanently -> page a human AND unblock the parent's fan-in", async () => {
    const { threadId } = await nurseTexts(PHONE.sofia, "I'm sick, can't make my shifts tomorrow");
    await settle();
    faults.always.add("after_book");
    await nurseTexts(PHONE.james, `YES ${await offerCode(DAY, "cg_james")}`);
    for (let i = 0; i < 6; i++) await retryAndSettle();               // exhaust max_attempts (5)
    faults.always.clear();
    expect((await run(DAY)).status).toBe("failed");
    expect(await lastMessageTo(PHONE.manager)).toContain(`${DAY} FAILED`);
    await nurseTexts(PHONE.anita, `yes ${await offerCode(EVE, "cg_anita")}`);
    await settle();
    expect(await threadLog(threadId)).toContain("All children done: 1/2 covered, 1 failed (humans paged)");
  });

  test("22. a stale timer from an old wave is ignored", async () => {
    await postSunriseDay();
    await nurseTexts(PHONE.james, `NO ${await offerCode(NEW, "cg_james")}`);
    await nurseTexts(PHONE.maria, `NO ${await offerCode(NEW, "cg_maria")}`);   // whole wave declined -> wave 2 now
    await settle();
    await db.transaction((tx) => deliver(tx, { agencyId: "ag_bright", threadId: NEW, kind: "timer", payload: { timerKind: "wave_timeout", wave: 1 }, dedupeKey: "old-timer" }));
    await settle();
    expect((await threadLog(NEW)).filter((l) => l.startsWith("Wave 3"))).toHaveLength(0);
    expect((await run(NEW)).status).toBe("waiting_event");
  });

  test("23. reaper: a 'running' thread with a stale heartbeat is force-unlocked and re-kicked", async () => {
    await postSunriseDay();
    await db.update(agentRuns).set({ status: "running", leaseOwner: "worker-that-died", heartbeatAt: clock.now() }).where(eq(agentRuns.threadId, NEW));
    clock.advanceMinutes(2);
    expect(await reapDeadRuns()).toEqual([NEW]);
    await settle();
    expect((await run(NEW)).status).toBe("waiting_event");             // projection restored on the next drain
  });

  test("24. parallel workers (concurrency 4) give the same result: named queues serialize each thread", async () => {
    await nurseTexts(PHONE.sofia, "I'm sick, can't make my shifts tomorrow");
    await settle(4);
    await nurseTexts(PHONE.james, `YES ${await offerCode(DAY, "cg_james")}`);
    await nurseTexts(PHONE.maria, `YES ${await offerCode(DAY, "cg_maria")}`);
    await nurseTexts(PHONE.anita, `yes ${await offerCode(EVE, "cg_anita")}`);
    expect((await settle(4)).errors).toEqual([]);
    expect((await shift("sh_sunrise_day")).caregiverId).toBe("cg_james");
    expect(await db.select().from(toolCalls).where(eq(toolCalls.tool, "scheduler.book_shift"))).toHaveLength(2);
  });
});

describe("memory", () => {
  test("25. preference stated in a reply ('no nights anymore') is learned and filters future night shifts", async () => {
    await postSunriseDay();
    await nurseTexts(PHONE.james, `No ${await offerCode(NEW, "cg_james")}, and I don't do nights anymore`);
    await settle();
    expect(await getPreferences("ag_bright", "cg_james")).toEqual([expect.objectContaining({ type: "avoid_shift_time", value: "night", strength: "hard", source: "nurse_stated" })]);
    const night = await postShift("sh_night", "fac_sunrise", tomorrowAt(23), addMinutes(tomorrowAt(23), 480), 26, 32);
    expect(await threadLog(night)).toContain('Excluded James Okafor: memory says "Does not work night shifts" (nurse_stated)');
  });

  test("26. episodic memory (vector search + filter) changes the ranking", async () => {
    for (let i = 0; i < 2; i++) {
      await addEpisode("ag_bright", { text: "James Okafor declined a day CNA shift at Sunrise Manor", caregiverId: "cg_james", facilityId: "fac_sunrise", shiftId: `old${i}`, signal: "declined", shiftTime: "day" });
    }
    await postSunriseDay();
    expect((await threadLog(NEW)).find((l) => l.startsWith("Rank #1"))).toContain("Maria Lopez");
  });

  test("27. consolidation promotes repeated episodes (via per-nurse jobs) - but never overrides what the nurse said", async () => {
    for (let i = 0; i < 2; i++) {
      await addEpisode("ag_bright", { text: "Grace declined a day shift", caregiverId: "cg_grace", facilityId: "fac_sunrise", shiftId: `g${i}`, signal: "declined", shiftTime: "day" });
      await addEpisode("ag_bright", { text: "Anita declined a night shift", caregiverId: "cg_anita", facilityId: "fac_oak", shiftId: `a${i}`, signal: "declined", shiftTime: "night" });
    }
    await addEpisode("ag_bright", { text: "ancient", caregiverId: "cg_tom", facilityId: "fac_oak", shiftId: "old", signal: "declined", shiftTime: "day", at: "2026-01-01T00:00:00Z" });
    await upsertPreference("ag_bright", "cg_anita", { type: "avoid_shift_time", value: "night", strength: "soft", text: "Happy to do nights now" }, "nurse_stated");
    const res = await consolidate("ag_bright");
    expect(res.forgotten).toBe(1);
    expect(res.promotions.map((p) => p.caregiverId).sort()).toEqual(["cg_anita", "cg_grace"]);
    for (const p of res.promotions) await upsertPreference("ag_bright", p.caregiverId, p.pref, "inferred");
    expect((await getPreferences("ag_bright", "cg_grace"))[0].text).toBe("Usually turns down day shifts (2 times)");
    expect((await getPreferences("ag_bright", "cg_anita"))[0].text).toBe("Happy to do nights now");   // stated beats inferred
  });
});

describe("voice (Retell)", () => {
  test("28. urgent shift -> Retell CALL with memory in the prompt -> call_analyzed books it + learns from the transcript", async () => {
    clock.freeze(new Date("2026-11-24T10:00:00Z"));
    await upsertPreference("ag_bright", "cg_anita", { type: "prefer_facility", value: "fac_oak", strength: "soft", text: "Likes the Oakridge team" }, "nurse_stated");
    const start = new Date("2026-11-24T13:00:00Z");
    const t = await postShift("sh_urgent", "fac_oak", start, addMinutes(start, 480), 26, 32, "dementia");
    const [call] = await smsTo(PHONE.anita);
    expect(call.channel).toBe("call");
    expect(call.meta.vars.known_preferences).toContain("Likes the Oakridge team");
    const code = await offerCode(t, "cg_anita");
    const res = await post("/webhooks/retell", { event: "call_analyzed", call: { call_id: "call_1", metadata: { offerCode: code },
      transcript: "Anita: Yes I'll take it. Also please don't call me before 9.", call_analysis: { custom_analysis_data: { accepted: true } } } });
    expect(res.body.routed).toBe(t);
    await settle();
    expect((await shift("sh_urgent")).caregiverId).toBe("cg_anita");
    expect((await getPreferences("ag_bright", "cg_anita")).map((p) => p.type)).toContain("contact_after_hour");
  });

  test("29. [review C5] mid-call accept_shift + post-call call_analyzed = ONE acceptance, booked once, no 'unavailable' text", async () => {
    await postSunriseDay();
    const code = await offerCode(NEW, "cg_james");
    const fn = await post("/retell/functions/accept_shift", { args: { offer_code: code }, call: { call_id: "c2", metadata: { offerCode: code } } });
    expect(fn.body.result).toContain("confirming");
    expect((await shift("sh_new")).status).toBe("open");               // the voice agent can't book directly
    await post("/webhooks/retell", { event: "call_analyzed", call: { call_id: "c2", metadata: { offerCode: code }, call_analysis: { custom_analysis_data: { accepted: true } } } });
    await settle();
    expect((await shift("sh_new")).caregiverId).toBe("cg_james");
    expect(await db.select().from(threadInbox).where(and(eq(threadInbox.threadId, NEW), eq(threadInbox.kind, "nurse_reply")))).toHaveLength(1);
    expect((await smsTo(PHONE.james)).some((m) => m.body.includes("no longer available"))).toBe(false);
  });

  test("30. bad webhook input -> 400 (not 500, which providers would retry forever)", async () => {
    expect((await post("/webhooks/sms", { from: PHONE.james })).status).toBe(400);
    expect((await post("/api/shifts", { id: "x" })).status).toBe(400);
  });
});

void approvals;
