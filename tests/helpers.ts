// Test harness: real Postgres, real graphile-worker (runOnce), frozen clock, gateway called in-process.

import { and, asc, eq } from "drizzle-orm";
import { clock } from "../src/clock.js";
import { db } from "../src/db/index.js";
import { agentRuns, approvals, caregivers, commsLog, offers, shifts } from "../src/db/schema.js";
import { app } from "../src/gateway.js";
import { getGraph } from "../src/graphs/index.js";
import { AGENCY_PHONE, seed } from "../src/seed.js";
import { retryFailedJobsNow, runUntilIdle } from "../src/tasks.js";

export const T0 = new Date("2026-11-24T06:00:00Z");   // 6am UTC, the day before the seeded shifts
let msgSeq = 0;

export const PHONE = {
  sofia: "+15550001001", maria: "+15550001002", anita: "+15550001003", james: "+15550001004",
  grace: "+15550001005", manager: "+15550000001",
};

export async function reset() {
  clock.freeze(T0);
  await seed();
}

/** Run every ready job until the system is quiet. Returns errors that happened. */
export const settle = (concurrency = 1) => runUntilIdle(concurrency);
/** Failed jobs back off for seconds; in tests, make them due now and settle again. */
export async function retryAndSettle() {
  await retryFailedJobsNow();
  return settle();
}

export async function post(path: string, body: unknown) {
  const res = await app.request(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  return { status: res.status, body: (await res.json()) as any };
}
export async function get(path: string) {
  return (await (await app.request(path)).json()) as any;
}

export async function nurseTexts(phone: string, body: string, messageId = `m${++msgSeq}`) {
  return (await post("/webhooks/sms", { from: phone, to: AGENCY_PHONE, body, messageId })).body;
}

export async function decide(approvalId: number, action: string, extra: Record<string, unknown> = {}) {
  return post(`/api/approvals/${approvalId}/decision`, { action, decidedBy: "coordinator@test", ...extra });
}

export const smsTo = (phone: string) =>
  db.select({ body: commsLog.body, channel: commsLog.channel, meta: commsLog.meta }).from(commsLog)
    .where(and(eq(commsLog.phone, phone), eq(commsLog.direction, "out"))).orderBy(asc(commsLog.id));

export async function lastMessageTo(phone: string) {
  const all = await smsTo(phone);
  return all[all.length - 1]?.body ?? "";
}

export const offerCode = async (threadId: string, caregiverId: string) =>
  (await db.select().from(offers).where(and(eq(offers.threadId, threadId), eq(offers.caregiverId, caregiverId))))[0]?.code as string;

export const pendingApprovals = () => db.select().from(approvals).where(eq(approvals.status, "pending")).orderBy(asc(approvals.id));
export const run = async (threadId: string) => (await db.select().from(agentRuns).where(eq(agentRuns.threadId, threadId)))[0];
export const shift = async (id: string) => (await db.select().from(shifts).where(eq(shifts.id, id)))[0];
export const hours = async (id: string) => (await db.select().from(caregivers).where(eq(caregivers.id, id)))[0].hoursThisWeek;
export const runs = (graph: string) => db.select().from(agentRuns).where(eq(agentRuns.graph, graph));

export async function threadLog(threadId: string) {
  const r = await run(threadId);
  const snap = await getGraph(r.graph, r.graphVersion).getState({ configurable: { thread_id: threadId } });
  return ((snap.values as any).log ?? []) as string[];
}

/** Post an open shift through the gateway and let the agent run its first wave. */
export async function postShift(id: string, facilityId: string, startsAt: Date, endsAt: Date, rate: number, maxRate: number, specialty: string | null = null) {
  const res = await post("/api/shifts", { id, facilityId, startsAt: startsAt.toISOString(), endsAt: endsAt.toISOString(), rate, maxRate, specialty });
  await settle();
  return res.body.threadId as string;
}
