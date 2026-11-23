// Wiring: topic -> handler, plus an in-process "run until idle" loop for tests and the demo.
// In production, services.ts runs the same dispatch() behind RabbitMQ consumers.

import { InMemoryBus, RetryLater } from "./bus.js";
import { handleMemoryJob } from "./memory.js";
import { relayOnce } from "./outbox.js";
import { processThread } from "./worker.js";

export async function dispatch(topic: string, payload: any) {
  if (topic === "thread.kick") return processThread(payload.threadId);
  if (topic.startsWith("memory.")) return handleMemoryJob(topic, payload);
  throw new Error(`no handler for ${topic}`);
}

/**
 * Deterministic event loop: relay outbox -> deliver messages -> repeat until nothing moves.
 * Errors are collected (not thrown) so tests can assert on crashes.
 */
export async function runUntilIdle(bus: InMemoryBus, maxRounds = 200) {
  const errors: Error[] = [];
  for (let round = 0; round < maxRounds; round++) {
    const relayed = await relayOnce(bus);
    const msgs = bus.popAll();
    if (!relayed && !msgs.length) return { errors };
    const kicked = new Set<string>();
    for (const m of msgs) {
      if (m.topic === "thread.kick") {                     // collapse duplicate kicks within a round
        if (kicked.has(m.payload.threadId)) continue;
        kicked.add(m.payload.threadId);
      }
      try {
        await dispatch(m.topic, m.payload);
      } catch (e) {
        if (e instanceof RetryLater) bus.messages.push(m);
        else errors.push(e as Error);
      }
    }
  }
  throw new Error("runUntilIdle: did not settle");
}
