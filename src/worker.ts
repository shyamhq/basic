// AGENT WORKER: turns "thread.kick" jobs into LangGraph runs.
//
// processThread(threadId):
//   1. take a per-thread Postgres ADVISORY LOCK (no two workers ever run the same thread;
//      if the worker dies, its DB session ends and the lock is released automatically)
//   2. DRAIN THE MAILBOX:
//        no checkpoint yet          -> invoke(startInput)
//        paused at an interrupt     -> pick the oldest pending event whose kind the interrupt
//                                      `accepts` -> invoke(new Command({ resume: event }))
//                                      (events it doesn't accept stay BUFFERED, e.g. a nurse
//                                      text arriving while a human approval is pending)
//        has pending tasks          -> it crashed mid-run -> invoke(null) resumes from the last checkpoint
//        finished                   -> remaining events are ignored
//   3. after each run, record what the thread now waits on (agent_runs) and, for human
//      interrupts, create a row in the approvals inbox.

import { Command } from "@langchain/langgraph";
import { RetryLater } from "./bus.js";
import { clock } from "./clock.js";
import { one, pool, q } from "./db.js";
import { GRAPHS, type GraphName } from "./graphs/index.js";
import { SimulatedCrash } from "./faults.js";
import { callTool } from "./tools.js";

const HUMAN_INTERRUPTS = new Set(["approval", "escalation", "triage_review", "review_outbound", "emergency_ack"]);


export async function processThread(threadId: string) {
  const lockConn = await pool.connect();
  try {
    const { rows } = await lockConn.query(`SELECT pg_try_advisory_lock(hashtext($1)) AS ok`, [threadId]);
    if (!rows[0].ok) throw new RetryLater(`thread ${threadId} is busy`);   // another worker has it -> retry later
    try {
      await drainMailbox(threadId);
    } finally {
      await lockConn.query(`SELECT pg_advisory_unlock(hashtext($1))`, [threadId]);
    }
  } finally {
    lockConn.release();
  }
}

async function nextEvent(threadId: string, kinds: string[]) {
  return one(
    `SELECT * FROM thread_inbox WHERE thread_id = $1 AND status = 'pending' AND kind = ANY($2) ORDER BY id LIMIT 1`,
    [threadId, kinds],
  );
}
const markEvent = (id: string, status: string, note: string | null = null) =>
  q(`UPDATE thread_inbox SET status = $2, note = $3, consumed_at = now() WHERE id = $1`, [id, status, note]);

const pendingInterrupt = (snap: any) => snap.tasks.flatMap((t: any) => t.interrupts ?? [])[0] as { id: string; value: any } | undefined;

async function drainMailbox(threadId: string) {
  for (let step = 0; step < 100; step++) {
    const run = await one(`SELECT * FROM agent_runs WHERE thread_id = $1`, [threadId]);
    if (!run) return;
    const graph = GRAPHS[run.graph as GraphName];
    const cfg = { configurable: { thread_id: threadId } };
    const snap = await graph.getState(cfg);
    const started = Boolean(snap.config?.configurable?.checkpoint_id);

    if (run.status === "done" || run.status === "failed") return ignoreLeftovers(threadId, run.status);

    let input: unknown;
    let event: any = null;
    const intr = started ? pendingInterrupt(snap) : undefined;

    if (!started) {
      event = await nextEvent(threadId, ["start"]);
      if (!event) return;
      input = event.payload;
    } else if (intr) {
      event = await nextEvent(threadId, intr.value.accepts);
      if (!event) return;                                     // nothing it can use yet -> stay paused
      // STALE-DECISION GUARD: a human decision only applies to the interrupt it was made for.
      if (event.kind === "human_decision" && event.payload.interruptId !== intr.id) {
        await markEvent(event.id, "stale", "decision was for an older interrupt");
        continue;
      }
      input = new Command({ resume: { id: String(event.id), kind: event.kind, payload: event.payload } });
    } else if (snap.next.length) {
      input = null;                                           // crash recovery: continue from last checkpoint
    } else {
      await q(`UPDATE agent_runs SET status = 'done', waiting_for = NULL, updated_at = now() WHERE thread_id = $1`, [threadId]);
      continue;
    }

    await q(`UPDATE agent_runs SET status = 'running', heartbeat_at = $2, updated_at = now() WHERE thread_id = $1`, [threadId, clock.now()]);
    const beat = setInterval(() => void q(`UPDATE agent_runs SET heartbeat_at = now() WHERE thread_id = $1`, [threadId]), 10_000);
    try {
      // durability "sync": the checkpoint is written before the next step starts -> safest for long-running work
      await graph.invoke(input as any, { ...cfg, durability: "sync" });
    } catch (e) {
      if (event) await markIfApplied(graph, cfg, event, intr?.id);
      if (e instanceof SimulatedCrash) throw e;               // "process died": leave status = running for the reaper
      await q(`UPDATE agent_runs SET status = 'queued', error = $2, updated_at = now() WHERE thread_id = $1`, [threadId, String(e)]);
      throw e;                                                // the bus retries with backoff, then DLQ
    } finally {
      clearInterval(beat);
    }
    if (event) await markEvent(event.id, "consumed");
    await recordWaitState(threadId, run, graph, cfg);
  }
}

/** If the run failed AFTER the event was applied (checkpoint moved past it), don't re-deliver it. */
async function markIfApplied(graph: any, cfg: any, event: any, interruptId?: string) {
  const snap = await graph.getState(cfg);
  const now = pendingInterrupt(snap);
  const applied = event.kind === "start" ? Boolean(snap.config?.configurable?.checkpoint_id) : now?.id !== interruptId;
  if (applied) await markEvent(event.id, "consumed", "applied before failure");
}

async function recordWaitState(threadId: string, run: any, graph: any, cfg: any) {
  const snap = await graph.getState(cfg);
  const intr = pendingInterrupt(snap);
  if (!intr) {
    if (!snap.next.length) await q(`UPDATE agent_runs SET status = 'done', waiting_for = NULL, updated_at = now() WHERE thread_id = $1`, [threadId]);
    return;
  }
  const human = HUMAN_INTERRUPTS.has(intr.value.type);
  await q(`UPDATE agent_runs SET status = $2, waiting_for = $3, error = NULL, updated_at = now() WHERE thread_id = $1`,
    [threadId, human ? "waiting_human" : "waiting_event", intr.value.type]);
  if (human) {
    // HITL inbox row. interrupt_id is UNIQUE -> re-running this is harmless.
    await q(
      `INSERT INTO approvals (thread_id, interrupt_id, kind, payload, created_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (interrupt_id) DO NOTHING`,
      [threadId, intr.id, intr.value.type, intr.value, clock.now()],
    );
  }
}

async function ignoreLeftovers(threadId: string, status: string) {
  const left = await q(`UPDATE thread_inbox SET status = 'ignored', note = $2, consumed_at = now()
                        WHERE thread_id = $1 AND status = 'pending' RETURNING *`, [threadId, `thread ${status}`]);
  for (const ev of left) {
    if (ev.kind !== "nurse_reply") continue;   // be polite to late repliers
    const cg = await one(`SELECT phone, name FROM caregivers WHERE id = $1`, [ev.payload.caregiverId]);
    if (cg) await callTool("comms.send_sms", { to: cg.phone, body: `Thanks ${cg.name.split(" ")[0]}! That shift is no longer available.` },
      { threadId, idempotencyKey: `${threadId}:late:${ev.id}` });
  }
}

/** Called by the bus after MAX_JOB_ATTEMPTS: the run is failed and a human is paged (HITL for failures). */
export async function onDeadJob(topic: string, payload: any, err: string) {
  await q(`INSERT INTO dead_letters (topic, payload, error) VALUES ($1,$2,$3)`, [topic, payload, err]);
  if (payload.threadId) {
    await q(`UPDATE agent_runs SET status = 'failed', error = $2 WHERE thread_id = $1`, [payload.threadId, err]);
    await callTool("ops.page_manager", { message: `Agent thread ${payload.threadId} FAILED: ${err.slice(0, 200)}` },
      { threadId: payload.threadId, idempotencyKey: `dead:${payload.threadId}` });
  }
}
