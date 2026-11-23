// DURABLE TIMERS + SLA ESCALATION + REAPER (the "time" side of long-running agents).
//
// A graph never sleeps. It writes a timer row, then pauses at interrupt().
// scheduler loop -> fireDueTimers() -> mailbox event {kind: "timer"} -> worker resumes the graph.
// This survives deploys and crashes, and costs zero compute while waiting
// (a nurse may answer in 30 seconds or 3 hours).

import { addMinutes, clock } from "./clock.js";
import { APPROVAL_SLA_MIN, LEASE_SECONDS } from "./config.js";
import { q, tx } from "./db.js";
import { deliverToThread, enqueue } from "./outbox.js";
import { callTool } from "./tools.js";

/** Idempotent: the id is deterministic (e.g. "<thread>:wave:2"), so a node re-run doesn't double-schedule. */
export async function setTimer(id: string, threadId: string, kind: string, fireAt: Date, payload: Record<string, unknown> = {}) {
  await q(
    `INSERT INTO timers (id, thread_id, kind, payload, fire_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (id) DO NOTHING`,
    [id, threadId, kind, payload, fireAt],
  );
}

export async function cancelTimers(threadId: string) {
  await q(`UPDATE timers SET cancelled = true WHERE thread_id = $1 AND fired_at IS NULL`, [threadId]);
}

/** Claim due timers with SKIP LOCKED (safe with N scheduler replicas) and deliver them to mailboxes. */
export async function fireDueTimers(now = clock.now()): Promise<number> {
  return tx(async (c) => {
    const due = await q(
      `UPDATE timers SET fired_at = $1 WHERE id IN (
         SELECT id FROM timers WHERE fired_at IS NULL AND NOT cancelled AND fire_at <= $1
         ORDER BY fire_at LIMIT 100 FOR UPDATE SKIP LOCKED)
       RETURNING *`, [now], c,
    );
    for (const t of due) {
      await deliverToThread(t.thread_id, "timer", { timerId: t.id, timerKind: t.kind, ...t.payload }, `timer:${t.id}`, c);
    }
    return due.length;
  });
}

/** Approvals nobody answered within the SLA -> page the on-call manager (once). */
export async function escalateStaleApprovals(now = clock.now()): Promise<number> {
  const stale = await q(
    `UPDATE approvals SET escalated = true
     WHERE status = 'pending' AND NOT escalated AND created_at <= $1 RETURNING *`,
    [addMinutes(now, -APPROVAL_SLA_MIN)],
  );
  for (const a of stale) {
    await callTool("ops.page_manager", { message: `Approval #${a.id} (${a.kind}) waiting > ${APPROVAL_SLA_MIN} min: ${a.payload.title ?? ""}` },
      { threadId: a.thread_id, idempotencyKey: `sla:${a.id}` });
  }
  return stale.length;
}

/** REAPER: a thread stuck in "running" with an old heartbeat means its worker died mid-step.
 *  Re-kick it; the worker will resume from the last checkpoint (graph.invoke(null)). */
export async function reapDeadRuns(now = clock.now()): Promise<string[]> {
  const dead = await q<{ thread_id: string }>(
    `UPDATE agent_runs SET status = 'queued', updated_at = now()
     WHERE status = 'running' AND heartbeat_at < $1 RETURNING thread_id`,
    [new Date(now.getTime() - LEASE_SECONDS * 1000)],
  );
  for (const r of dead) await enqueue("thread.kick", { threadId: r.thread_id });
  return dead.map((r) => r.thread_id);
}
