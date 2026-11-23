// AGENT WORKER: runs one thread's mailbox. Called by the "thread_kick" job.
//
// graphile-worker runs all jobs of queue "thread:<id>" one at a time, so two workers never run
// the same thread concurrently. On top of that we take a LEASE (lease_version = fencing token)
// so a zombie worker can't overwrite control-plane state after the reaper reassigned the thread.
//
// THE CHECKPOINT IS THE SOURCE OF TRUTH. Everything else is derived from it on EVERY pass:
//   reconcileInflight(): an event handed to the graph is "consumed" iff the checkpoint moved past
//                        the interrupt it resumed; otherwise it goes back to "pending" (retry).
//   project():           agent_runs.status / waiting_for and the approvals inbox are rebuilt from the
//                        current interrupt. A crash between invoke() and these writes self-heals on
//                        the next drain - there is no "after a clean run" step to miss.
//
// drain loop:
//   no checkpoint yet        -> invoke(start input)
//   paused at an interrupt   -> oldest pending event whose kind the interrupt `accepts`
//                               -> invoke(Command({ resume: event }))   (others stay BUFFERED)
//   has next tasks, no pause -> it crashed mid-run -> invoke(null) continues from the last checkpoint
//   finished / failed        -> close leftovers

import { Command } from "@langchain/langgraph";
import { and, eq, inArray, ne, sql } from "drizzle-orm";
import { clock } from "./clock.js";
import { db } from "./db/index.js";
import { agentRuns, approvals, shifts, threadInbox } from "./db/schema.js";
import { maybeCrash } from "./faults.js";
import { getGraph } from "./graphs/index.js";
import { addJob } from "./jobs.js";
import { log } from "./log.js";
import { deliver } from "./mailbox.js";
import { callTool } from "./tools.js";

const HUMAN_INTERRUPTS = new Set(["approval", "escalation", "triage_review", "review_outbound", "emergency_ack"]);
export class LostLease extends Error {}

type View = {
  started: boolean;
  interrupt?: { id: string; value: { type: string; accepts: string[]; [k: string]: any } };
  hasNext: boolean;
  finished: boolean;
};
type Run = typeof agentRuns.$inferSelect;
type Event = typeof threadInbox.$inferSelect;

async function viewOf(run: Run): Promise<View> {
  const snap = await getGraph(run.graph, run.graphVersion).getState({ configurable: { thread_id: run.threadId } });
  const started = Boolean(snap.config?.configurable?.checkpoint_id);
  const interrupt = snap.tasks.flatMap((t: any) => t.interrupts ?? [])[0];
  return { started, interrupt, hasNext: snap.next.length > 0, finished: started && snap.next.length === 0 };
}

export async function processThread(threadId: string, workerId = "local") {
  // Take the lease: bump the fencing token. Every control-plane write below is conditional on it.
  const [lease] = await db.update(agentRuns)
    .set({ leaseOwner: workerId, leaseVersion: sql`${agentRuns.leaseVersion} + 1`, heartbeatAt: clock.now() })
    .where(eq(agentRuns.threadId, threadId)).returning();
  if (!lease) return;
  const fence = lease.leaseVersion;

  for (let step = 0; step < 100; step++) {
    const [run] = await db.select().from(agentRuns).where(eq(agentRuns.threadId, threadId));
    if (run.leaseVersion !== fence) throw new LostLease(threadId);
    const view = await viewOf(run);
    await reconcileInflight(threadId, view);
    await project(run, view, fence);
    if (run.status === "failed" || view.finished) return closeLeftovers(run);

    let input: unknown;
    let event: Event | undefined;
    if (!view.started) {
      event = await nextEvent(threadId, ["start"]);
      if (!event) return;
      input = event.payload;
    } else if (view.interrupt) {
      event = await nextEvent(threadId, view.interrupt.value.accepts);
      if (!event) return;                                           // nothing it can use yet -> stay paused
      // STALE-DECISION GUARD: a human decision only applies to the interrupt it was made for.
      if (event.kind === "human_decision" && event.payload.interruptId !== view.interrupt.id) {
        await setEvent(event.id, "stale", "decision was for an older interrupt");
        continue;
      }
      input = new Command({ resume: { id: String(event.id), kind: event.kind, payload: event.payload } });
    } else {
      input = null;                                                 // crash recovery: continue from last checkpoint
    }

    if (event) {
      await db.update(threadInbox).set({ status: "inflight", resumedInterruptId: view.interrupt?.id ?? "start" }).where(eq(threadInbox.id, event.id));
    }
    await db.update(agentRuns).set({ status: "running", heartbeatAt: clock.now(), error: null })
      .where(and(eq(agentRuns.threadId, threadId), eq(agentRuns.leaseVersion, fence)));
    log.info({ threadId, event: event?.kind }, "invoke");
    const beat = setInterval(() => void db.update(agentRuns).set({ heartbeatAt: clock.now() }).where(eq(agentRuns.threadId, threadId)), 10_000);
    try {
      // durability "sync": every checkpoint is written before the next step starts
      await getGraph(run.graph, run.graphVersion).invoke(input as any, { configurable: { thread_id: threadId }, durability: "sync" });
    } finally {
      clearInterval(beat);
    }
    maybeCrash("worker.after_invoke");   // tests: die before ANY bookkeeping -> the next drain must repair it
  }
}

const nextEvent = async (threadId: string, kinds: string[]) =>
  (await db.select().from(threadInbox)
    .where(and(eq(threadInbox.threadId, threadId), eq(threadInbox.status, "pending"), inArray(threadInbox.kind, kinds as Event["kind"][])))
    .orderBy(threadInbox.id).limit(1))[0];

const setEvent = (id: number, status: Event["status"], note: string | null = null) =>
  db.update(threadInbox).set({ status, note }).where(eq(threadInbox.id, id));

/** inflight -> consumed if the checkpoint moved past the interrupt it resumed, else back to pending. */
async function reconcileInflight(threadId: string, view: View) {
  const inflight = await db.select().from(threadInbox).where(and(eq(threadInbox.threadId, threadId), eq(threadInbox.status, "inflight")));
  for (const ev of inflight) {
    const applied = ev.resumedInterruptId === "start" ? view.started : view.interrupt?.id !== ev.resumedInterruptId;
    await setEvent(ev.id, applied ? "consumed" : "pending", applied ? null : "retrying: not applied before failure");
  }
}

/** Rebuild agent_runs + approvals from the checkpoint. Idempotent, fenced. */
async function project(run: Run, view: View, fence: number) {
  const human = view.interrupt && HUMAN_INTERRUPTS.has(view.interrupt.value.type);
  const status: Run["status"] = run.status === "failed" ? "failed"
    : view.finished ? "done"
    : view.interrupt ? (human ? "waiting_human" : "waiting_event")
    : "queued";
  await db.transaction(async (tx) => {
    await tx.update(agentRuns).set({ status, waitingFor: view.interrupt?.value.type ?? null })
      .where(and(eq(agentRuns.threadId, run.threadId), eq(agentRuns.leaseVersion, fence)));
    if (human) {
      await tx.insert(approvals).values({
        agencyId: run.agencyId, threadId: run.threadId, interruptId: view.interrupt!.id,
        kind: view.interrupt!.value.type as any, payload: view.interrupt!.value, createdAt: clock.now(),
      }).onConflictDoNothing();
    }
    // approvals for interrupts the thread is no longer paused at can't be acted on any more
    const closed = view.finished || status === "failed" ? "cancelled" : "stale";
    await tx.update(approvals).set({ status: closed })
      .where(and(eq(approvals.threadId, run.threadId), eq(approvals.status, "pending"), ne(approvals.interruptId, view.interrupt?.id ?? "")));
  });
}

async function closeLeftovers(run: Run) {
  const left = await db.update(threadInbox).set({ status: "ignored", note: `thread ${run.status}` })
    .where(and(eq(threadInbox.threadId, run.threadId), eq(threadInbox.status, "pending"))).returning();
  if (run.status === "failed") return;          // a human owns the shift now: don't tell nurses anything misleading
  for (const ev of left) {
    if (ev.kind !== "nurse_reply") continue;
    const [sh] = await db.select().from(shifts).where(eq(shifts.id, String(ev.payload.shiftId ?? "")));
    const { caregiverId, phone, firstName } = ev.payload;
    const body = sh?.caregiverId === caregiverId ? `You're all set, ${firstName} - you're booked for that shift.` : `Thanks ${firstName}! That shift is no longer available.`;
    await callTool("comms.send_sms", { to: phone, body }, { agencyId: run.agencyId, threadId: run.threadId, idempotencyKey: `${run.threadId}:late:${ev.id}` });
  }
}

/** Last attempt failed: mark failed, close approvals, page a human, and tell the parent (fan-in must not hang). */
export async function onThreadDead(threadId: string, err: string) {
  await db.transaction(async (tx) => {
    const [run] = await tx.update(agentRuns).set({ status: "failed", error: err.slice(0, 500) }).where(eq(agentRuns.threadId, threadId)).returning();
    if (!run) return;
    await tx.update(approvals).set({ status: "cancelled" }).where(and(eq(approvals.threadId, threadId), eq(approvals.status, "pending")));
    await addJob(tx, "send_page", { agencyId: run.agencyId, threadId, key: `dead:${threadId}`, message: `Agent thread ${threadId} FAILED: ${err.slice(0, 200)}` });
    if (run.parentThreadId) {
      await deliver(tx, { agencyId: run.agencyId, threadId: run.parentThreadId, kind: "child_done",
        payload: { childThreadId: threadId, shiftId: run.subject, outcome: "failed", caregiverId: null }, dedupeKey: `child_done:${threadId}` });
    }
  });
}
