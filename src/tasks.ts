// graphile-worker TASK LIST + cron schedule. This file is the whole "background processing" surface.

import { run, runOnce, type Runner, type Task, type TaskList } from "graphile-worker";
import { and, eq, lt, sql } from "drizzle-orm";
import { addMinutes, clock } from "./clock.js";
import { db, pool } from "./db/index.js";
import { agencies, agentRuns } from "./db/schema.js";
import { checkpointer } from "./graphs/index.js";
import { addMemoryJob } from "./jobs.js";
import { log } from "./log.js";
import { consolidate, memoryTasks } from "./memory.js";
import { escalateStaleApprovals, fireDueTimers, reapDeadRuns } from "./timers.js";
import { callTool } from "./tools.js";
import { onThreadDead, processThread } from "./worker.js";

/** Errors seen by tasks (tests assert on these; production logs them). */
export const jobErrors: Error[] = [];

/** Wrap a task: log errors and, on the LAST attempt, run the dead-letter handler. */
const task = (fn: (payload: any, helpers: Parameters<Task>[1]) => Promise<unknown>): Task => async (payload, helpers) => {
  try {
    await fn(payload, helpers);
  } catch (e) {
    jobErrors.push(e as Error);
    log.error({ task: helpers.job.task_identifier, attempt: helpers.job.attempts, err: String(e) }, "job failed");
    if (helpers.job.attempts >= helpers.job.max_attempts && (payload as any)?.threadId && helpers.job.task_identifier === "thread_kick") {
      await onThreadDead((payload as any).threadId, String(e));           // DLQ equivalent + fan-in failure
    }
    throw e;                                                               // graphile retries with exponential backoff
  }
};

export const taskList: TaskList = {
  thread_kick: task((p, h) => processThread(p.threadId, String(h.job.locked_by ?? "local"))),

  send_page: task((p) => callTool("ops.page_manager", { message: p.message }, { agencyId: p.agencyId, threadId: p.threadId, idempotencyKey: p.key })),

  // cron: every minute
  timers_tick: task(async () => { await fireDueTimers(); await escalateStaleApprovals(); await reapDeadRuns(); }),

  // cron: hourly. Promotions are enqueued per caregiver so they serialize with extractions.
  consolidate_all: task(async () => {
    for (const a of await db.select({ id: agencies.id }).from(agencies)) await addMemoryJob(db, "memory_consolidate", { agencyId: a.id }, `consolidate:${a.id}`);
  }),
  memory_consolidate: task(async (p) => {
    const { promotions } = await consolidate(p.agencyId);
    for (const pr of promotions) await addMemoryJob(db, "memory_upsert_pref", { agencyId: p.agencyId, caregiverId: pr.caregiverId, pref: pr.pref, source: "inferred" });
  }),

  // cron: daily. Checkpoints hold conversation state forever unless you delete them (privacy + cost).
  checkpoint_retention: task(async () => {
    const old = await db.select({ id: agentRuns.threadId }).from(agentRuns)
      .where(and(eq(agentRuns.status, "done"), lt(agentRuns.createdAt, addMinutes(clock.now(), -30 * 24 * 60))));
    for (const r of old) await checkpointer.deleteThread(r.id);
  }),

  memory_extract: task(memoryTasks.memory_extract),
  memory_episode: task(memoryTasks.memory_episode),
  memory_reflect: task(memoryTasks.memory_reflect),
  memory_upsert_pref: task(memoryTasks.memory_upsert_pref),
};

export const crontab = [
  "* * * * * timers_tick",
  "0 * * * * consolidate_all",
  "30 3 * * * checkpoint_retention",
].join("\n");

/** Long-running worker process (services.ts). Named queues give per-thread serial execution. */
export function startWorker(concurrency = 10): Promise<Runner> {
  return run({ pgPool: pool, taskList, crontab, concurrency, noHandleSignals: false, pollInterval: 500 });
}

/** Tests & demo: run every job that's ready now, repeatedly, until the queue is quiet. */
export async function runUntilIdle(concurrency = 1) {
  const before = jobErrors.length;
  for (let i = 0; i < 50; i++) {
    await runOnce({ pgPool: pool, taskList, concurrency, logger: quietLogger });
    await waitForUnlocked();                       // runOnce can return before a failed job's unlock is written
    const { rows } = await db.execute<{ n: number }>(sql`
      SELECT count(*)::int AS n FROM graphile_worker._private_jobs WHERE run_at <= now() AND locked_at IS NULL AND attempts < max_attempts`);
    if (rows[0].n === 0) return { errors: jobErrors.slice(before) };
  }
  throw new Error("runUntilIdle: did not settle");
}

async function waitForUnlocked() {
  for (let i = 0; i < 200; i++) {
    const { rows } = await db.execute<{ n: number }>(sql`SELECT count(*)::int AS n FROM graphile_worker._private_jobs WHERE locked_at IS NOT NULL`);
    if (rows[0].n === 0) return;
    await new Promise((r) => setTimeout(r, 10));
  }
}

/** Tests: failed jobs are retried after a backoff (seconds); make them due now instead of sleeping. */
export async function retryFailedJobsNow() {
  await waitForUnlocked();
  await db.execute(sql`UPDATE graphile_worker._private_jobs SET run_at = now() WHERE locked_at IS NULL AND last_error IS NOT NULL AND attempts < max_attempts`);
}

import { Logger } from "graphile-worker";
const quietLogger = new Logger(() => () => {});
