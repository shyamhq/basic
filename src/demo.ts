// A narrated, deterministic walkthrough (frozen clock, real Postgres, real graphile-worker jobs).
//   npm run demo
// Read the output top to bottom - it's the story you tell in the interview.

import { and, desc, eq, sql } from "drizzle-orm";
import { addMinutes, clock } from "./clock.js";
import { db, pool } from "./db/index.js";
import { agentRuns, commsLog, offers } from "./db/schema.js";
import { faults } from "./faults.js";
import { app } from "./gateway.js";
import { getGraph } from "./graphs/index.js";
import { usage } from "./llm.js";
import { getMemoryStore, getPreferences } from "./memory.js";
import { AGENCY_PHONE, seed, tomorrowAt } from "./seed.js";
import { retryFailedJobsNow, runUntilIdle } from "./tasks.js";
import { fireDueTimers } from "./timers.js";

const settle = () => runUntilIdle();
let n = 0;
const C = { b: "\x1b[1m", d: "\x1b[2m", c: "\x1b[36m", y: "\x1b[33m", g: "\x1b[32m", r: "\x1b[0m" };
const section = (t: string) => console.log(`\n${C.b}${C.c}━━ ${t} ━━${C.r}`);
const say = (t: string) => console.log(`${C.y}» ${t}${C.r}`);

async function api(method: string, path: string, body?: unknown) {
  const res = await app.request(path, { method, headers: { "content-type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  return res.json() as Promise<any>;
}
const text = async (phone: string, body: string) => {
  say(`${phone} texts: "${body}"`);
  return api("POST", "/webhooks/sms", { from: phone, to: AGENCY_PHONE, body, messageId: `demo-${++n}` });
};
async function showLog(threadId: string, from = 0) {
  const [r] = await db.select().from(agentRuns).where(eq(agentRuns.threadId, threadId));
  const snap = await getGraph(r.graph, r.graphVersion).getState({ configurable: { thread_id: threadId } });
  const log = ((snap.values as any).log ?? []) as string[];
  log.slice(from).forEach((l) => console.log(`  ${C.d}[${threadId.split(":").slice(0, 2).join(":")}]${C.r} ${l}`));
  return log.length;
}
async function lastTo(phone: string) {
  const [m] = await db.select().from(commsLog).where(and(eq(commsLog.phone, phone), eq(commsLog.direction, "out"))).orderBy(desc(commsLog.id)).limit(1);
  console.log(`  ${C.g}→ ${phone} (${m?.channel}): ${m?.body}${C.r}`);
}
const code = async (thread: string, cg: string) => (await db.select().from(offers).where(and(eq(offers.threadId, thread), eq(offers.caregiverId, cg))))[0].code;
const pending = () => api("GET", "/api/approvals?status=pending");

clock.freeze(new Date("2026-11-24T06:00:00Z"));
await seed();
const DAY = "fill:sh_sunrise_day", EVE = "fill:sh_oak_eve";

section("1. A nurse calls out sick (Intake / Supervisor agent)");
const { threadId: intake } = await text("+15550001001", "Hi, I'm sick with a fever and can't make my shifts tomorrow");
await settle();
await showLog(intake);
await lastTo("+15550001001");

section("2. Two ShiftFill child threads: SQL hard filters -> memory -> Send fan-out scoring -> wave 1");
const dl = await showLog(DAY);
const el = await showLog(EVE);
console.log(`  ${C.d}Both threads are PAUSED at interrupt(); no worker is held while they wait.${C.r}`);

section("3. She texts again ('double texting') -> joins the open conversation, no duplicate work");
const again = await text("+15550001001", "so sorry, I really can't come in tomorrow");
await settle();
console.log(`  routed: ${again.routed}`);
await lastTo("+15550001001");

section("4. James says yes -> no risks -> auto-approved by policy -> booked. But the worker CRASHES mid-node...");
faults.crashAt.add("after_book");
await text("+15550001004", `YES ${await code(DAY, "cg_james")}`);
const { errors } = await settle();
console.log(`  ${C.b}crash:${C.r} ${errors[0]?.message} (the booking committed; the confirmation text did not)`);
say("graphile-worker retries the job; the thread resumes from its last checkpoint");
await retryFailedJobsNow();
await settle();
await showLog(DAY, dl);
await lastTo("+15550001004");
const hrs = (await db.execute<{ h: number }>(sql`SELECT hours_this_week AS h FROM app.caregivers WHERE id = 'cg_james'`)).rows[0].h;
console.log(`  James' hours: 24 -> ${hrs} (booked exactly once: idempotent tool + transactional booking)`);

section("5. Oakridge: Anita declines (and states a preference); Maria counters at $30 -> HUMAN approval");
await text("+15550001003", `No ${await code(EVE, "cg_anita")}, sorry - and I don't do nights anymore`);
await text("+15550001002", `I can do it for $30 ${await code(EVE, "cg_maria")}`);
await settle();
await showLog(EVE, el);
const [appr] = await pending();
console.log(`  ${C.b}Approvals inbox:${C.r} #${appr.id} ${appr.payload.title} risks=${JSON.stringify(appr.payload.risks)}`);
const bad = await api("POST", `/api/approvals/${appr.id}/decision`, { action: "assign", edits: { caregiverId: "cg_x" }, decidedBy: "dana@agency" });
console.log(`  a wrong action for this kind of approval is refused: ${bad.error}`);

section("6. Coordinator EDITS the rate to $29 and approves -> booking -> parent fan-in completes");
await api("POST", `/api/approvals/${appr.id}/decision`, { action: "edit", edits: { rate: 29 }, decidedBy: "dana@agency" });
await settle();
await showLog(EVE, el + 3);
await showLog(intake, 4);

section("7. Memory written in the background (preference learned from Anita's text)");
for (const p of await getPreferences("ag_bright", "cg_anita")) console.log(`  cg_anita: ${p.text}  {type=${p.type}, strength=${p.strength}, source=${p.source}}`);

section("8. A facility posts a NIGHT shift -> memory excludes Anita -> nobody answers -> escalation");
const s = tomorrowAt(23);
const { threadId: night } = await api("POST", "/api/shifts", { id: "sh_oak_night", facilityId: "fac_oak", startsAt: s.toISOString(), endsAt: addMinutes(s, 480).toISOString(), rate: 28, maxRate: 34, specialty: "dementia" });
await settle();
let nl = await showLog(night);
clock.advanceMinutes(16);
say("16 minutes pass with no replies... the cron tick fires the durable wave timer");
await fireDueTimers();
await settle();
nl = await showLog(night, nl);
const [esc] = await pending();
console.log(`  ${C.b}Approvals inbox:${C.r} #${esc.id} ${esc.kind}: ${esc.payload.title}`);
await api("POST", `/api/approvals/${esc.id}/decision`, { action: "cancel", reason: "Facility will use internal staff", decidedBy: "dana@agency" });
await settle();
await showLog(night, nl);

section("9. Under the hood");
const stats = (await db.execute(sql`SELECT
  (SELECT count(*) FROM public.checkpoints)::int AS checkpoints,
  (SELECT count(*) FROM app.thread_inbox)::int AS mailbox_events,
  (SELECT count(*) FROM app.tool_calls)::int AS tool_calls,
  (SELECT count(*) FROM app.approvals)::int AS approvals,
  (SELECT count(*) FROM app.comms_log WHERE direction = 'out')::int AS messages_sent`)).rows[0];
console.log(" ", stats);
console.log("  runs:", (await db.select().from(agentRuns).orderBy(agentRuns.createdAt)).map((r) => `${r.threadId}=${r.status}`).join("  "));
console.log(`  LLM: ${usage.calls ? `${usage.calls} calls` : "offline heuristics (set ANTHROPIC_API_KEY to use Claude)"}`);

await getMemoryStore().stop();
await pool.end();
