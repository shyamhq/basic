// SCENARIO TESTS - each one is an interview story. Read them like a script.
// Real Postgres + LangGraph checkpoints; the bus is in-memory and the clock is frozen.

import { afterAll, beforeEach, describe, expect, test } from "vitest";
import { addMinutes, clock } from "../src/clock.js";
import { one, pool, q } from "../src/db.js";
import { faults, SimulatedCrash } from "../src/faults.js";
import { addEpisode, consolidate, getPreferences, getProcedures, getMemoryStore, upsertPreference } from "../src/memory.js";
import { deliverToThread } from "../src/outbox.js";
import { tomorrowAt } from "../src/seed.js";
import { escalateStaleApprovals, fireDueTimers, reapDeadRuns } from "../src/timers.js";
import {
  PHONE, decide, get, lastMessageTo, nurseTexts, offerCode, pendingApprovals, post, postShift, reset, run, settle, shift, smsTo, threadLog,
} from "./helpers.js";

beforeEach(reset);
afterAll(async () => { await getMemoryStore().stop(); await pool.end(); });

/** Fast-forward time and let the scheduler fire due timers. */
async function passTime(minutes: number) {
  clock.advanceMinutes(minutes);
  await fireDueTimers();
  return settle();
}

// A fresh day shift at Sunrise. Ranking (see logs): James, Maria (overtime) | Anita, Grace.
const postSunriseDay = () => postShift("sh_new", "fac_sunrise", tomorrowAt(7), tomorrowAt(15), 24, 30, "geriatrics");

describe("orchestration", () => {
  test("1. call-out -> supervisor fans out one child thread per shift -> fan-in when all are covered", async () => {
    const { threadId } = await nurseTexts(PHONE.sofia, "Hi, I'm sick with a fever and can't make my shifts tomorrow");
    await settle();
    const day = `fill:sh_sunrise_day:${threadId}`;
    const eve = `fill:sh_oak_eve:${threadId}`;
    expect((await run(day))!.status).toBe("waiting_event");          // paused at interrupt(), no worker held
    expect((await run(eve))!.status).toBe("waiting_event");
    expect((await run(threadId))!.waiting_for).toBe("wait_children");
    expect(await lastMessageTo(PHONE.sofia)).toContain("finding cover for your 2 shift(s)");

    await nurseTexts(PHONE.james, `YES ${await offerCode(day, "cg_james")}`);
    await nurseTexts(PHONE.anita, `yes ${await offerCode(eve, "cg_anita")}`);
    expect((await settle()).errors).toEqual([]);

    expect((await shift("sh_sunrise_day"))!.caregiver_id).toBe("cg_james");
    expect((await shift("sh_oak_eve"))!.caregiver_id).toBe("cg_anita");
    expect((await run(threadId))!.status).toBe("done");
    expect(await threadLog(threadId, "intake")).toContain("All children done: 2/2 covered");
    // Maria was offered both shifts and got a "filled" message for each
    expect((await smsTo(PHONE.maria)).filter((m) => m.body.includes("has been filled"))).toHaveLength(2);
    // every super-step was checkpointed -> full history ("time travel" / audit)
    const hist = await get(`/api/threads/${day}/history`);
    expect(hist.length).toBeGreaterThan(8);
  });

  test("2. nobody answers -> wave timeout -> next wave -> escalate to a human who assigns manually", async () => {
    const t = await postSunriseDay();
    expect(await threadLog(t)).toContain("Wave 1: offered to James Okafor (sms, code " + (await offerCode(t, "cg_james")) + "), Maria Lopez (sms, code " + (await offerCode(t, "cg_maria")) + ")");
    await passTime(16);                                              // wave 1 timer fires
    expect((await threadLog(t)).some((l) => l.startsWith("Wave 2: offered to Anita Reddy"))).toBe(true);
    await passTime(16);                                              // wave 2 timer fires, MAX_WAVES reached
    const [esc] = await pendingApprovals();
    expect(esc.kind).toBe("escalation");
    expect((await run(t))!.status).toBe("waiting_human");

    await decide(esc.id, "assign", { edits: { caregiverId: "cg_grace", rate: 25 } });
    await settle();
    expect((await shift("sh_new"))!.caregiver_id).toBe("cg_grace");
    expect((await threadLog(t)).at(-1)).toBe("Finished: filled");
  });

  test("3. Send fan-out scores every candidate in parallel; hard filters are explainable", async () => {
    const t = await postSunriseDay();
    const log = await threadLog(t);
    // Brian too far, Tom no TB test, Priya is an RN: filtered in SQL before any LLM/scoring
    expect(log).toContain("5 matched license/certs/availability, 4 within travel distance, 4 after memory filters");
    expect(log.filter((l) => l.startsWith("Rank #"))).toHaveLength(3);
  });
});

describe("human in the loop", () => {
  test("4. overtime -> approval gate -> coordinator EDITS the rate and approves", async () => {
    const t = await postSunriseDay();
    await nurseTexts(PHONE.maria, `Yes ${await offerCode(t, "cg_maria")}`);
    await settle();
    const [a] = await pendingApprovals();
    expect(a.kind).toBe("approval");
    expect(a.payload.risks).toEqual(["overtime"]);
    expect((await shift("sh_new"))!.status).toBe("open");            // nothing booked without a human

    await decide(a.id, "edit", { edits: { rate: 28 } });
    await settle();
    const s = await shift("sh_new");
    expect([s!.caregiver_id, s!.rate]).toEqual(["cg_maria", 28]);
    const call = await one(`SELECT approved_by FROM tool_calls WHERE tool = 'scheduler.book_shift'`);
    expect(call.approved_by).toBe("coordinator@test");               // audit trail
  });

  test("5. coordinator REJECTS with a reason -> reflection writes procedural memory -> new policy applies", async () => {
    const t = await postSunriseDay();
    await nurseTexts(PHONE.james, `NO ${await offerCode(t, "cg_james")}`);
    await nurseTexts(PHONE.maria, `yes ${await offerCode(t, "cg_maria")}`);
    await settle();
    const [a] = await pendingApprovals();
    await decide(a.id, "reject", { reason: "Never book someone's first shift at a facility without checking with me" });
    await settle();                                                    // memory.reflect runs in the background
    const procs = await getProcedures("ag_bright");
    expect(procs[0].rule).toEqual({ type: "require_approval", when: "first_shift_at_facility" });
    expect(await lastMessageTo(PHONE.maria)).toContain("gone another way");

    // wave 2 went out (Anita, Grace). Anita has never worked at Sunrise -> the LEARNED rule now gates her.
    await nurseTexts(PHONE.anita, `yes ${await offerCode(t, "cg_anita")}`);
    await settle();
    const [b] = await pendingApprovals();
    expect(b.payload.risks).toContain("first_shift_at_facility");
  });

  test("6. emergency keywords bypass the model and go straight to a human", async () => {
    const { threadId } = await nurseTexts(PHONE.sofia, "Resident fell in room 12 and is bleeding, what do I do");
    await settle();
    expect(await lastMessageTo(PHONE.manager)).toContain("EMERGENCY");
    expect(await lastMessageTo(PHONE.sofia)).toContain("call 911");
    const [a] = await pendingApprovals();
    expect(a.kind).toBe("emergency_ack");
    await decide(a.id, "approve", { reason: "Called the DON, resident stable" });
    await settle();
    expect((await run(threadId))!.status).toBe("done");
    // the page happened exactly once even though the node re-ran on resume (idempotent tools)
    expect((await smsTo(PHONE.manager)).filter((m) => m.body.includes("EMERGENCY"))).toHaveLength(1);
  });

  test("7. low-confidence triage -> a human picks the intent -> the graph continues", async () => {
    const { threadId } = await nurseTexts(PHONE.grace, "hmm about tomorrow");
    await settle();
    const [a] = await pendingApprovals();
    expect(a.kind).toBe("triage_review");
    await decide(a.id, "edit", { edits: { intent: "question" } });
    await settle();
    expect(await threadLog(threadId, "intake")).toContain("coordinator@test set intent = question");
    expect(await lastMessageTo(PHONE.grace)).toContain("coordinator will get back to you");
  });

  test("8. review-every-message mode: nothing reaches a nurse until a human approves (and edits) the draft", async () => {
    await q(`UPDATE agencies SET review_outbound = true`);
    const t = await postSunriseDay();
    expect(await smsTo(PHONE.james)).toHaveLength(0);
    const [a] = await pendingApprovals();
    expect(a.kind).toBe("review_outbound");
    const drafts = a.payload.drafts.map((d: any) => ({ ...d, text: d.text.replace("Hi", "Good morning") }));
    await decide(a.id, "approve", { edits: { drafts } });
    await settle();
    expect(await lastMessageTo(PHONE.james)).toMatch(/^Good morning James/);
    expect((await run(t))!.status).toBe("waiting_event");
  });

  test("9. two coordinators click at once -> first wins; a decision for an OLD interrupt is discarded", async () => {
    const t = await postSunriseDay();
    await nurseTexts(PHONE.maria, `yes ${await offerCode(t, "cg_maria")}`);
    await settle();
    const [a] = await pendingApprovals();
    // a late decision for an OLD interrupt (e.g. a previous approval round) must not resume this one
    await deliverToThread(t, "human_decision", { action: "approve", interruptId: "old-interrupt", decidedBy: "x" }, "stale-1");
    await settle();
    expect((await one(`SELECT status FROM thread_inbox WHERE dedupe_key = 'stale-1'`)).status).toBe("stale");
    expect((await shift("sh_new"))!.status).toBe("open");
    // two coordinators: first click wins, the second gets 409
    expect((await decide(a.id, "approve")).status).toBe(200);
    expect((await decide(a.id, "reject", { reason: "x" })).status).toBe(409);
    await settle();
    expect((await shift("sh_new"))!.caregiver_id).toBe("cg_maria");
  });

  test("10. approval SLA: nobody answers in 10 minutes -> the on-call manager is paged once", async () => {
    const t = await postSunriseDay();
    await nurseTexts(PHONE.maria, `yes ${await offerCode(t, "cg_maria")}`);
    await settle();
    clock.advanceMinutes(11);
    expect(await escalateStaleApprovals()).toBe(1);
    expect(await escalateStaleApprovals()).toBe(0);
    expect(await lastMessageTo(PHONE.manager)).toContain("waiting > 10 min");
  });
});

describe("long-running reliability", () => {
  test("11. MAILBOX: an event the graph can't use yet is buffered, then consumed later in order", async () => {
    const t = await postSunriseDay();
    await nurseTexts(PHONE.maria, `yes ${await offerCode(t, "cg_maria")}`);   // -> approval pending
    await settle();
    await nurseTexts(PHONE.james, `YES ${await offerCode(t, "cg_james")}`);    // arrives while waiting on a human
    await settle();
    const buffered = await one(`SELECT status FROM thread_inbox WHERE thread_id = $1 AND kind = 'nurse_reply' ORDER BY id DESC LIMIT 1`, [t]);
    expect(buffered.status).toBe("pending");                          // held, not lost, not misapplied
    const [a] = await pendingApprovals();
    await decide(a.id, "reject", { reason: "Maria is too tired this week" });
    await settle();                                                    // back at wait -> James's YES is consumed
    expect((await shift("sh_new"))!.caregiver_id).toBe("cg_james");
  });

  test("12. duplicate webhooks + double replies never double-book", async () => {
    const t = await postSunriseDay();
    const code = await offerCode(t, "cg_james");
    await nurseTexts(PHONE.james, `YES ${code}`, "dup-1");
    expect((await nurseTexts(PHONE.james, `YES ${code}`, "dup-1")).duplicate).toBe(true);   // provider retry
    await settle();
    await nurseTexts(PHONE.james, `yes!! ${code}`, "dup-2");                                // nurse double-texts
    await settle();
    expect((await one(`SELECT hours_this_week FROM caregivers WHERE id = 'cg_james'`)).hours_this_week).toBe(32);
    expect((await q(`SELECT 1 FROM tool_calls WHERE tool = 'scheduler.book_shift'`)).length).toBe(1);
    expect(await lastMessageTo(PHONE.james)).toContain("no longer available");
  });

  test("13. crash AFTER booking, BEFORE the node finished -> reaper resumes from checkpoint, no double booking", async () => {
    const t = await postSunriseDay();
    faults.crashAt.add("after_book");
    await nurseTexts(PHONE.james, `YES ${await offerCode(t, "cg_james")}`);
    const { errors } = await settle();
    expect(errors[0]).toBeInstanceOf(SimulatedCrash);
    expect((await run(t))!.status).toBe("running");                  // "the process died"
    expect((await shift("sh_new"))!.status).toBe("filled");          // the side effect DID happen

    clock.advanceMinutes(2);                                          // heartbeat is now stale
    expect(await reapDeadRuns()).toEqual([t]);
    expect((await settle()).errors).toEqual([]);
    expect((await run(t))!.status).toBe("done");
    expect((await one(`SELECT hours_this_week FROM caregivers WHERE id = 'cg_james'`)).hours_this_week).toBe(32);   // +8 once
    expect((await smsTo(PHONE.james)).filter((m) => m.body.startsWith("You're booked"))).toHaveLength(1);
  });

  test("14. a stale timer from an old wave is ignored", async () => {
    const t = await postSunriseDay();
    await nurseTexts(PHONE.james, `NO ${await offerCode(t, "cg_james")}`);
    await nurseTexts(PHONE.maria, `NO ${await offerCode(t, "cg_maria")}`);   // whole wave declined -> wave 2 immediately
    await settle();
    expect((await threadLog(t)).some((l) => l.startsWith("Wave 2"))).toBe(true);
    await deliverToThread(t, "timer", { timerKind: "wave_timeout", wave: 1 }, "old-timer");
    await settle();
    expect((await threadLog(t)).filter((l) => l.startsWith("Wave 3"))).toHaveLength(0);
    expect((await run(t))!.status).toBe("waiting_event");
  });
});

describe("memory", () => {
  test("15. preference stated in a reply ('no nights anymore') is learned and filters future night shifts", async () => {
    const t = await postSunriseDay();
    await nurseTexts(PHONE.james, `No ${await offerCode(t, "cg_james")}, and I don't do nights anymore`);
    await settle();
    const prefs = await getPreferences("ag_bright", "cg_james");
    expect(prefs).toEqual([expect.objectContaining({ type: "avoid_shift_time", value: "night", strength: "hard", source: "nurse_stated" })]);

    const night = await postShift("sh_night", "fac_sunrise", tomorrowAt(23), addMinutes(tomorrowAt(23), 480), 26, 32);
    expect(await threadLog(night)).toContain('Excluded James Okafor: memory says "Does not work night shifts" (nurse_stated)');
  });

  test("16. episodic memory: past outcomes at a facility change the ranking (semantic search + filter)", async () => {
    for (let i = 0; i < 2; i++) {
      await addEpisode("ag_bright", { text: "James Okafor declined a day CNA shift at Sunrise Manor", caregiverId: "cg_james",
        facilityId: "fac_sunrise", shiftId: `old${i}`, signal: "declined", shiftTime: "day" });
    }
    const t = await postSunriseDay();
    const rank1 = (await threadLog(t)).find((l) => l.startsWith("Rank #1"))!;
    expect(rank1).toContain("Maria Lopez");                          // James dropped from #1
    expect((await threadLog(t)).find((l) => l.includes("James Okafor score"))).toContain("episode: declined here before");
  });

  test("17. consolidation promotes repeated episodes into a soft preference - but never overrides what the nurse said", async () => {
    for (let i = 0; i < 2; i++) {
      await addEpisode("ag_bright", { text: "Grace declined a day shift", caregiverId: "cg_grace", facilityId: "fac_sunrise", shiftId: `g${i}`, signal: "declined", shiftTime: "day" });
      await addEpisode("ag_bright", { text: "Anita declined a night shift", caregiverId: "cg_anita", facilityId: "fac_oak", shiftId: `a${i}`, signal: "declined", shiftTime: "night" });
    }
    await addEpisode("ag_bright", { text: "ancient", caregiverId: "cg_tom", facilityId: "fac_oak", shiftId: "old", signal: "declined", shiftTime: "day", at: "2026-01-01T00:00:00Z" });
    // Anita herself said she is fine with nights -> stated beats inferred
    await upsertPreference("ag_bright", "cg_anita", { type: "avoid_shift_time", value: "night", strength: "soft", text: "Happy to do nights now" }, "nurse_stated");

    const res = await consolidate("ag_bright");
    expect(res.forgotten).toBe(1);                                    // decay: older than 90 days
    expect(res.promoted).toEqual(["cg_grace: Usually turns down day shifts (2 times)"]);
    expect((await getPreferences("ag_bright", "cg_anita"))[0].text).toBe("Happy to do nights now");
  });
});

describe("voice (Retell)", () => {
  test("18. urgent shift -> agent CALLS via Retell with memory in the prompt -> call_analyzed webhook books it + learns from transcript", async () => {
    clock.freeze(new Date("2026-11-24T10:00:00Z"));
    await upsertPreference("ag_bright", "cg_anita", { type: "prefer_facility", value: "fac_oak", strength: "soft", text: "Likes the Oakridge team" }, "nurse_stated");
    const start = new Date("2026-11-24T13:00:00Z");                  // 3h away -> urgent
    const t = await postShift("sh_urgent", "fac_oak", start, addMinutes(start, 480), 26, 32, "dementia");
    const call = await one(`SELECT * FROM comms_log WHERE phone = $1`, [PHONE.anita]);
    expect(call.channel).toBe("call");
    expect(call.meta.vars.known_preferences).toContain("Likes the Oakridge team");

    const code = await offerCode(t, "cg_anita");
    const res = await post("/webhooks/retell", {
      event: "call_analyzed",
      call: { call_id: "call_1", metadata: { offerCode: code }, transcript: "Agent: ... Anita: Yes I'll take it. Also please don't call me before 9.",
        call_analysis: { custom_analysis_data: { accepted: true } } },
    });
    expect(res.body.routed).toBe(t);
    await settle();
    expect((await shift("sh_urgent"))!.caregiver_id).toBe("cg_anita");
    expect((await getPreferences("ag_bright", "cg_anita")).map((p) => p.type)).toContain("contact_after_hour");
  });

  test("19. Retell mid-call function: accept_shift becomes a mailbox event (the voice agent can't book directly)", async () => {
    const t = await postSunriseDay();
    const code = await offerCode(t, "cg_james");
    const res = await post("/retell/functions/accept_shift", { args: { offer_code: code }, call: { call_id: "c2", metadata: { offerCode: code } } });
    expect(res.body.result).toContain("confirming");
    expect((await shift("sh_new"))!.status).toBe("open");            // not booked by the voice agent itself
    await settle();
    expect((await shift("sh_new"))!.caregiver_id).toBe("cg_james");  // booked by the graph
  });
});
