// JOB QUEUE: graphile-worker (jobs live in Postgres, schema graphile_worker).
//
// Why a Postgres queue instead of RabbitMQ on the main path?
//   * add_job() is a SQL function -> enqueueing happens INSIDE the same transaction as the state
//     change. The jobs table IS the transactional outbox; no relay process, no dual write.
//   * queue_name = "thread:<id>"  -> jobs in one named queue run strictly one at a time
//                                    (replaces our hand-rolled per-thread advisory lock).
//   * job_key = "kick:<id>"       -> duplicate kicks coalesce into one pending job.
//   * retries with exponential backoff + attempt counts are built in (replaces delay queues / DLQ).
//   * cron items replace the scheduler loop.
// The RabbitMQ + outbox version is kept in src/reference/rabbitmq.ts for comparison.

import { sql } from "drizzle-orm";
import { MAX_JOB_ATTEMPTS } from "./config.js";
import type { DbOrTx } from "./db/index.js";

export type TaskName =
  | "thread_kick" | "send_page" | "timers_tick" | "consolidate_all" | "checkpoint_retention"
  | "memory_extract" | "memory_episode" | "memory_reflect" | "memory_upsert_pref" | "memory_consolidate";

type JobOpts = { queue?: string; jobKey?: string; runAt?: Date; maxAttempts?: number };

/** Enqueue a job using the caller's connection/transaction -> atomic with whatever else it writes. */
export async function addJob(tx: DbOrTx, task: TaskName, payload: Record<string, unknown>, o: JobOpts = {}) {
  await tx.execute(sql`
    SELECT graphile_worker.add_job(
      ${task}, ${JSON.stringify(payload)}::json,
      queue_name => ${o.queue ?? null}::text,
      run_at => ${o.runAt?.toISOString() ?? null}::timestamptz,
      max_attempts => ${o.maxAttempts ?? MAX_JOB_ATTEMPTS}::int,
      job_key => ${o.jobKey ?? null}::text)`);
}

/** "Something new is in thread X's mailbox." Serialized per thread, de-duplicated by key. */
export const kickThread = (tx: DbOrTx, threadId: string) =>
  addJob(tx, "thread_kick", { threadId }, { queue: `thread:${threadId}`, jobKey: `kick:${threadId}` });

/** Memory writes for one caregiver run one at a time (queue per caregiver) -> no read-modify-write races. */
export const addMemoryJob = (tx: DbOrTx, task: TaskName, payload: Record<string, unknown> & { caregiverId?: string }, jobKey?: string) =>
  addJob(tx, task, payload, { queue: payload.caregiverId ? `memory:${payload.caregiverId}` : "memory", jobKey });
