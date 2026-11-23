// Transactional outbox + thread mailbox.
//
// THE DUAL-WRITE PROBLEM (classic interview question):
//   await db.update(shift); await rabbit.publish(job);  // crash in between -> job lost
//   await rabbit.publish(job); await db.update(shift);  // crash in between -> job for a change that never happened
//
// FIX: insert the message into `outbox` in the SAME transaction as the business change.
// A relay publishes outbox rows to RabbitMQ afterwards. Delivery becomes
// at-least-once, so every consumer must be idempotent.

import type pg from "pg";
import { one, pool, q, tx } from "./db.js";

type Queryable = pg.Pool | pg.PoolClient;

export interface Bus {
  publish(topic: string, payload: unknown, messageId?: string): Promise<void>;
}

export async function enqueue(topic: string, payload: unknown, dedupeKey: string | null = null, c: Queryable = pool) {
  await q(
    `INSERT INTO outbox (topic, payload, dedupe_key) VALUES ($1, $2, $3) ON CONFLICT (dedupe_key) DO NOTHING`,
    [topic, JSON.stringify(payload), dedupeKey], c,
  );
}

/**
 * Put an event in a thread's MAILBOX and enqueue a "kick" so a worker drains it.
 * Returns false if the event was already delivered (same dedupeKey) -> idempotent.
 */
export async function deliverToThread(
  threadId: string, kind: string, payload: Record<string, unknown>,
  dedupeKey: string | null = null, c: Queryable = pool,
): Promise<boolean> {
  const row = await one(
    `INSERT INTO thread_inbox (thread_id, kind, payload, dedupe_key) VALUES ($1, $2, $3, $4)
     ON CONFLICT (dedupe_key) DO NOTHING RETURNING id`,
    [threadId, kind, JSON.stringify(payload), dedupeKey], c,
  );
  if (!row) return false;
  await enqueue("thread.kick", { threadId }, null, c);
  return true;
}

/** Create a new agent thread and give it its start input - atomically. */
export async function startThread(
  args: { threadId: string; graph: string; agencyId: string; input: Record<string, unknown>; parentThreadId?: string },
  c?: Queryable,
) {
  const run = async (cx: Queryable) => {
    const created = await one(
      `INSERT INTO agent_runs (thread_id, agency_id, graph, parent_thread_id) VALUES ($1, $2, $3, $4)
       ON CONFLICT (thread_id) DO NOTHING RETURNING thread_id`,
      [args.threadId, args.agencyId, args.graph, args.parentThreadId ?? null], cx,
    );
    if (created) await deliverToThread(args.threadId, "start", args.input, `start:${args.threadId}`, cx);
    return Boolean(created);
  };
  return c ? run(c) : tx(run);
}

/** Publish unpublished outbox rows. Safe to run in many processes (SKIP LOCKED). */
export async function relayOnce(bus: Bus, batch = 100): Promise<number> {
  return tx(async (c) => {
    const rows = await q<{ id: string; topic: string; payload: any }>(
      `SELECT id, topic, payload FROM outbox WHERE published_at IS NULL
       ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, [batch], c,
    );
    // Crash after publish but before COMMIT -> rows re-published next time -> duplicates
    // are possible by design. Consumers dedupe (mailbox dedupe keys, idempotent tools).
    for (const r of rows) await bus.publish(r.topic, r.payload, r.id);
    if (rows.length) await q(`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, [rows.map((r) => r.id)], c);
    return rows.length;
  });
}
