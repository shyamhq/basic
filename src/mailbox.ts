// The MAILBOX (actor model): every stimulus for a thread is written here first, together with a
// "kick" job, in ONE transaction. The worker then drains the mailbox in order (src/worker.ts).

import { db, type DbOrTx } from "./db/index.js";
import { agentRuns, threadInbox } from "./db/schema.js";
import { GRAPH_VERSIONS, type GraphName } from "./graphs/versions.js";
import { kickThread } from "./jobs.js";

type Kind = (typeof threadInbox.$inferInsert)["kind"];

/** Returns false if this exact event was already delivered (same dedupeKey) -> idempotent. */
export async function deliver(
  tx: DbOrTx,
  e: { agencyId: string; threadId: string; kind: Kind; payload: Record<string, unknown>; dedupeKey?: string | null },
): Promise<boolean> {
  const [row] = await tx.insert(threadInbox)
    .values({ agencyId: e.agencyId, threadId: e.threadId, kind: e.kind, payload: e.payload, dedupeKey: e.dedupeKey ?? null })
    .onConflictDoNothing({ target: threadInbox.dedupeKey })
    .returning({ id: threadInbox.id });
  if (!row) return false;
  await kickThread(tx, e.threadId);
  return true;
}

/** Create a durable thread + its start event. Returns false if a thread with this id already exists. */
export async function startThread(
  args: { threadId: string; graph: GraphName; agencyId: string; subject: string; input: Record<string, unknown>; parentThreadId?: string | null },
  tx?: DbOrTx,
): Promise<boolean> {
  const run = async (t: DbOrTx) => {
    const [created] = await t.insert(agentRuns).values({
      threadId: args.threadId, agencyId: args.agencyId, graph: args.graph, graphVersion: GRAPH_VERSIONS[args.graph],
      subject: args.subject, parentThreadId: args.parentThreadId ?? null,
    }).onConflictDoNothing().returning({ id: agentRuns.threadId });
    if (!created) return false;
    await deliver(t, { agencyId: args.agencyId, threadId: args.threadId, kind: "start", payload: args.input, dedupeKey: `start:${args.threadId}` });
    return true;
  };
  return tx ? run(tx) : db.transaction(run);
}
