// Integration test against a REAL RabbitMQ: delayed retry -> dead-letter queue.
// Skipped automatically when RabbitMQ isn't reachable (e.g. CI without docker-compose).

import amqp from "amqplib";
import { afterAll, beforeAll, expect, test } from "vitest";
import { RabbitBus, RetryLater } from "../src/bus.js";
import { MAX_JOB_ATTEMPTS, RABBITMQ_URL } from "../src/config.js";

let up = false;
try { const c = await amqp.connect(RABBITMQ_URL); await c.close(); up = true; } catch { up = false; }

const bus = new RabbitBus();
beforeAll(async () => {
  if (!up) return;
  await bus.setup();
  const c = await amqp.connect(RABBITMQ_URL);
  const ch = await c.createChannel();
  for (const qn of ["agent.jobs", "agent.jobs.delay", "agent.jobs.dlq"]) await ch.purgeQueue(qn);
  await c.close();
});
afterAll(async () => { if (up) await bus.close(); });

test.skipIf(!up)("poison job: retried with delay MAX_JOB_ATTEMPTS times, then parked in the DLQ; RetryLater doesn't count", async () => {
  const attempts: number[] = [];
  let busyOnce = true;
  const dead: unknown[] = [];
  await bus.consume("agent.jobs", async (_topic, payload) => {
    if (payload.kind === "busy" && busyOnce) { busyOnce = false; throw new RetryLater("locked"); }
    if (payload.kind === "busy") { attempts.push(-1); return; }
    attempts.push(Date.now());
    throw new Error("always fails");
  }, async (_t, p) => { dead.push(p); });

  await bus.publish("thread.kick", { kind: "poison" });
  await bus.publish("thread.kick", { kind: "busy" });
  const deadline = Date.now() + 30_000;
  while (dead.length === 0 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 250));

  expect(dead).toEqual([{ kind: "poison" }]);
  expect(attempts.filter((a) => a > 0)).toHaveLength(MAX_JOB_ATTEMPTS);
  expect(attempts).toContain(-1);                                      // the "busy" job succeeded after one delayed retry
  const c = await amqp.connect(RABBITMQ_URL);
  const ch = await c.createChannel();
  expect((await ch.checkQueue("agent.jobs.dlq")).messageCount).toBe(1);
  await c.close();
}, 40_000);
