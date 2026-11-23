// Test harness: real Postgres, in-memory bus, frozen clock, gateway called in-process.

import { InMemoryBus } from "../src/bus.js";
import { clock } from "../src/clock.js";
import { one, q } from "../src/db.js";
import { app } from "../src/gateway.js";
import { GRAPHS } from "../src/graphs/index.js";
import { runUntilIdle } from "../src/runtime.js";
import { seed } from "../src/seed.js";

export const T0 = new Date("2026-11-24T06:00:00Z");   // 6am UTC, the day before the seeded shifts
export const bus = new InMemoryBus();
let msgSeq = 0;

export const PHONE = {
  sofia: "+15550001001", maria: "+15550001002", anita: "+15550001003", james: "+15550001004",
  grace: "+15550001005", manager: "+15550000001",
};

export async function reset() {
  clock.freeze(T0);
  bus.popAll();
  await seed();
}

/** Let every queued job run until the system is quiet. Returns errors that happened. */
export const settle = () => runUntilIdle(bus);

export async function post(path: string, body: unknown) {
  const res = await app.request(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  return { status: res.status, body: (await res.json()) as any };
}

export async function get(path: string) {
  return (await (await app.request(path)).json()) as any;
}

export async function nurseTexts(phone: string, body: string, messageId = `m${++msgSeq}`) {
  return (await post("/webhooks/sms", { from: phone, body, messageId })).body;
}

export async function decide(approvalId: number, action: string, extra: Record<string, unknown> = {}) {
  return post(`/api/approvals/${approvalId}/decision`, { action, decidedBy: "coordinator@test", ...extra });
}

export const smsTo = (phone: string) =>
  q<{ body: string; channel: string }>(`SELECT body, channel FROM comms_log WHERE phone = $1 AND direction = 'out' ORDER BY id`, [phone]);

export async function lastMessageTo(phone: string) {
  const all = await smsTo(phone);
  return all[all.length - 1]?.body ?? "";
}

export const offerCode = async (threadId: string, caregiverId: string) =>
  (await one(`SELECT code FROM offers WHERE thread_id = $1 AND caregiver_id = $2`, [threadId, caregiverId]))?.code as string;

export const pendingApprovals = () => q(`SELECT * FROM approvals WHERE status = 'pending' ORDER BY id`);
export const run = (threadId: string) => one(`SELECT * FROM agent_runs WHERE thread_id = $1`, [threadId]);
export const shift = (id: string) => one(`SELECT * FROM shifts WHERE id = $1`, [id]);

export async function threadLog(threadId: string, graph: "intake" | "shiftfill" = "shiftfill") {
  const snap = await GRAPHS[graph].getState({ configurable: { thread_id: threadId } });
  return ((snap.values as any).log ?? []) as string[];
}

/** Post an open shift through the gateway and let the agent run its first wave. */
export async function postShift(id: string, facilityId: string, startsAt: Date, endsAt: Date, rate: number, maxRate: number, specialty: string | null = null) {
  const res = await post("/api/shifts", { id, facilityId, startsAt: startsAt.toISOString(), endsAt: endsAt.toISOString(), rate, maxRate, specialty });
  await settle();
  return res.body.threadId as string;
}
