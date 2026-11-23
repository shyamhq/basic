// A narrated, deterministic walkthrough (frozen clock, in-memory bus, real Postgres).
//   npm run demo
// Read the output top to bottom - it's the story you tell in the interview.

import { InMemoryBus } from "./bus.js";
import { addMinutes, clock } from "./clock.js";
import { one, pool, q } from "./db.js";
import { app } from "./gateway.js";
import { GRAPHS } from "./graphs/index.js";
import { usage } from "./llm.js";
import { getMemoryStore, getPreferences } from "./memory.js";
import { runUntilIdle } from "./runtime.js";
import { seed, tomorrowAt } from "./seed.js";
import { fireDueTimers } from "./timers.js";

const bus = new InMemoryBus();
const settle = () => runUntilIdle(bus);
let n = 0;
const C = { b: "\x1b[1m", d: "\x1b[2m", c: "\x1b[36m", y: "\x1b[33m", g: "\x1b[32m", r: "\x1b[0m" };

const section = (t: string) => console.log(`\n${C.b}${C.c}━━ ${t} ━━${C.r}`);
const say = (t: string) => console.log(`${C.y}» ${t}${C.r}`);
async function api(method: string, path: string, body?: unknown) {
  const res = await app.request(path, { method, headers: { "content-type": "application/json" }, body: body ? JSON.stringify(body) : undefined });
  return res.json() as Promise<any>;
}
const text = async (phone: string, body: string) => { say(`${phone} texts: "${body}"`); return api("POST", "/webhooks/sms", { from: phone, body, messageId: `demo-${++n}` }); };
async function showLog(threadId: string, graph: "intake" | "shiftfill" = "shiftfill", from = 0) {
  const snap = await GRAPHS[graph].getState({ configurable: { thread_id: threadId } });
  const log = ((snap.values as any).log ?? []) as string[];
  log.slice(from).forEach((l) => console.log(`  ${C.d}[${threadId.split(":").slice(0, 2).join(":")}]${C.r} ${l}`));
  return log.length;
}
async function outbox(phone: string) {
  const m = await one(`SELECT channel, body FROM comms_log WHERE phone = $1 AND direction = 'out' ORDER BY id DESC LIMIT 1`, [phone]);
  console.log(`  ${C.g}→ ${phone} (${m?.channel}): ${m?.body}${C.r}`);
}
const code = async (thread: string, cg: string) => (await one(`SELECT code FROM offers WHERE thread_id = $1 AND caregiver_id = $2`, [thread, cg])).code;
const pending = () => api("GET", "/api/approvals?status=pending");

clock.freeze(new Date("2026-11-24T06:00:00Z"));
await seed();

section("1. A nurse calls out sick (Intake / Supervisor agent)");
const { threadId: intake } = await text("+15550001001", "Hi, I'm sick with a fever and can't make my shifts tomorrow");
await settle();
await showLog(intake, "intake");
await outbox("+15550001001");
const day = `fill:sh_sunrise_day:${intake}`, eve = `fill:sh_oak_eve:${intake}`;

section("2. Two ShiftFill child threads ran in parallel: hard filters -> memory -> Send fan-out scoring -> wave 1");
const dl = await showLog(day);
const el = await showLog(eve);
console.log(`  ${C.d}Both threads are now PAUSED at interrupt(); no worker or memory is held while they wait.${C.r}`);

section("3. James says yes to Sunrise -> no risks -> auto-approved by policy -> booked");
await text("+15550001004", `YES ${await code(day, "cg_james")}`);
await settle();
await showLog(day, "shiftfill", dl);
await outbox("+15550001004");
await outbox("+15550001002");

section("4. Oakridge: Anita declines (and states a preference); Maria counters at $30 -> HUMAN approval");
await text("+15550001003", `No ${await code(eve, "cg_anita")}, sorry - and I don't do nights anymore`);
await text("+15550001002", `I can do it for $30 ${await code(eve, "cg_maria")}`);
await settle();
await showLog(eve, "shiftfill", el);
const [appr] = await pending();
console.log(`  ${C.b}Approvals inbox:${C.r} #${appr.id} ${appr.payload.title} risks=${JSON.stringify(appr.payload.risks)}`);

section("5. Coordinator EDITS the rate to $29 and approves -> booking -> parent fan-in completes");
await api("POST", `/api/approvals/${appr.id}/decision`, { action: "edit", edits: { rate: 29 }, decidedBy: "dana@agency" });
await settle();
await showLog(eve, "shiftfill", el + 3);
await showLog(intake, "intake", 4);

section("6. Memory written in the background (preference learned from Anita's text)");
for (const p of await getPreferences("ag_bright", "cg_anita")) console.log(`  cg_anita: ${p.text}  {type=${p.type}, strength=${p.strength}, source=${p.source}}`);

section("7. A facility posts a NIGHT shift at Oakridge -> memory excludes Anita -> nobody answers -> waves -> escalation");
const s = tomorrowAt(23);
const { threadId: night } = await api("POST", "/api/shifts", { id: "sh_oak_night", facilityId: "fac_oak", startsAt: s.toISOString(), endsAt: addMinutes(s, 480).toISOString(), rate: 28, maxRate: 34, specialty: "dementia" });
await settle();
let nl = await showLog(night);
clock.advanceMinutes(16);
say(`16 minutes pass with no replies... the scheduler fires the durable wave timer`);
await fireDueTimers();
await settle();
nl = await showLog(night, "shiftfill", nl);
const [esc] = await pending();
console.log(`  ${C.b}Approvals inbox:${C.r} #${esc.id} ${esc.kind}: ${esc.payload.title}`);
await api("POST", `/api/approvals/${esc.id}/decision`, { action: "cancel", reason: "Facility will use internal staff", decidedBy: "dana@agency" });
await settle();
await showLog(night, "shiftfill", nl);

section("8. Under the hood");
const stats = await one(`SELECT
  (SELECT count(*) FROM checkpoints)::int AS checkpoints,
  (SELECT count(*) FROM thread_inbox)::int AS mailbox_events,
  (SELECT count(*) FROM outbox)::int AS outbox_msgs,
  (SELECT count(*) FROM tool_calls)::int AS tool_calls,
  (SELECT count(*) FROM approvals)::int AS approvals,
  (SELECT count(*) FROM comms_log WHERE direction = 'out')::int AS messages_sent`);
console.log(" ", stats);
console.log("  runs:", (await q(`SELECT thread_id, status FROM agent_runs ORDER BY created_at`)).map((r) => `${r.thread_id}=${r.status}`).join("  "));
console.log(`  LLM: ${usage.calls ? `${usage.calls} calls` : "offline heuristics (set ANTHROPIC_API_KEY to use Claude)"}`);

await getMemoryStore().stop();
await pool.end();
