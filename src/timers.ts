// DURABLE TIMERS, APPROVAL SLA and the REAPER: the "time" side of long-running agents.
//
// A graph never sleeps. It writes a timer row, then pauses at interrupt(). Every minute a cron job
// (timers_tick) claims due timers and drops {kind:"timer"} into the thread's mailbox. It survives
// deploys and crashes, and costs nothing while waiting (a nurse may reply in 30s or 3h).
//
// Why our own table instead of graphile run_at jobs? Timers are DOMAIN state: the UI can show
// "next wave at 07:15", tests can fast-forward the clock, and the graph can cancel them by id.

import { and, eq, isNull, lt, sql } from "drizzle-orm";
import { addMinutes, clock } from "./clock.js";
import { APPROVAL_SLA_MIN, LEASE_SECONDS } from "./config.js";
import { db, type DbOrTx } from "./db/index.js";
import { agentRuns, approvals, timers } from "./db/schema.js";
import { addJob, kickThread } from "./jobs.js";
import { deliver } from "./mailbox.js";

/** Idempotent: deterministic id ("<thread>:wave:2"), so a node re-run doesn't double-schedule. */
export async function setTimer(t: { id: string; agencyId: string; threadId: string; kind: string; fireAt: Date; payload?: Record<string, unknown> }, tx: DbOrTx = db) {
  await tx.insert(timers).values({ ...t, payload: t.payload ?? {} }).onConflictDoNothing();
}

export async function cancelTimers(threadId: string, tx: DbOrTx = db) {
  await tx.update(timers).set({ cancelled: true }).where(and(eq(timers.threadId, threadId), isNull(timers.firedAt)));
}

/** Claim due timers with SKIP LOCKED (safe with N replicas) and deliver them - in one transaction. */
export async function fireDueTimers(now = clock.now()): Promise<number> {
  return db.transaction(async (tx) => {
    const due = await tx.execute<{ id: string; agency_id: string; thread_id: string; kind: string; payload: any }>(sql`
      UPDATE app.timers SET fired_at = ${now.toISOString()}::timestamptz WHERE id IN (
        SELECT id FROM app.timers WHERE fired_at IS NULL AND NOT cancelled AND fire_at <= ${now.toISOString()}::timestamptz
        ORDER BY fire_at LIMIT 100 FOR UPDATE SKIP LOCKED)
      RETURNING id, agency_id, thread_id, kind, payload`);
    for (const t of due.rows) {
      await deliver(tx, { agencyId: t.agency_id, threadId: t.thread_id, kind: "timer", payload: { timerId: t.id, timerKind: t.kind, ...t.payload }, dedupeKey: `timer:${t.id}` });
    }
    return due.rows.length;
  });
}

/** Approvals unanswered within the SLA -> page the on-call manager. The flag and the page job commit
 *  TOGETHER, so a crash can't leave "escalated" set with no page sent (or page twice). */
export async function escalateStaleApprovals(now = clock.now()): Promise<number> {
  return db.transaction(async (tx) => {
    const stale = await tx.update(approvals).set({ escalated: true })
      .where(and(eq(approvals.status, "pending"), eq(approvals.escalated, false), lt(approvals.createdAt, addMinutes(now, -APPROVAL_SLA_MIN))))
      .returning();
    for (const a of stale) {
      await addJob(tx, "send_page", {
        agencyId: a.agencyId, threadId: a.threadId, key: `sla:${a.id}`,
        message: `Approval #${a.id} (${a.kind}) waiting > ${APPROVAL_SLA_MIN} min: ${a.payload.title ?? ""}`,
      });
    }
    return stale.length;
  });
}

/** REAPER: a thread "running" with a stale heartbeat means its worker died mid-step. graphile-worker
 *  would only unlock that job after 4 hours, so we force-unlock the dead worker and re-kick.
 *  If the "dead" worker was only slow, the lease version (fencing token) stops its control-plane writes. */
export async function reapDeadRuns(now = clock.now()): Promise<string[]> {
  return db.transaction(async (tx) => {
    const dead = await tx.update(agentRuns).set({ status: "queued" })
      .where(and(eq(agentRuns.status, "running"), lt(agentRuns.heartbeatAt, new Date(now.getTime() - LEASE_SECONDS * 1000))))
      .returning();
    const owners = dead.map((r) => r.leaseOwner).filter(Boolean) as string[];
    if (owners.length) await tx.execute(sql`SELECT graphile_worker.force_unlock_workers(ARRAY[${sql.join(owners.map((o) => sql`${o}`), sql`, `)}]::text[])`);
    for (const r of dead) await kickThread(tx, r.threadId);
    return dead.map((r) => r.threadId);
  });
}

export const pendingTimers = (threadId: string) =>
  db.select().from(timers).where(and(eq(timers.threadId, threadId), isNull(timers.firedAt), eq(timers.cancelled, false)));
