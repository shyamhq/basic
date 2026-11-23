// Message bus: RabbitMQ in "production", an in-memory array in tests.
//
// RabbitMQ topology (RabbitBus.setup):
//
//   publisher --(topic exchange "vali")--+-- "thread.#" --> [agent.jobs]  --> agent workers (competing consumers)
//                                        +-- "memory.#" --> [memory.jobs] --> memory workers
//
//   For each work queue Q we also declare:
//     Q.delay : no consumers; x-message-ttl = RETRY_DELAY_MS; dead-letters back into Q
//               => "retry in 2s" without blocking a consumer or sleeping
//     Q.dlq   : poison messages after MAX_JOB_ATTEMPTS -> a human looks at them
//
// Why RabbitMQ and not Kafka? These are JOBS ("do once, ack, retry on failure"),
// not an event LOG you replay. RabbitMQ gives per-message ack, TTL and
// dead-lettering for free. Kafka fits replayable streams, many consumer groups,
// huge throughput (telemetry, audio chunks). Per-key ordering in Kafka comes from
// the partition key; here we get per-thread ordering from a Postgres lock + mailbox.

import amqp from "amqplib";
import { MAX_JOB_ATTEMPTS, RABBITMQ_URL, RETRY_DELAY_MS } from "./config.js";
import type { Bus } from "./outbox.js";

const EXCHANGE = "vali";
export const QUEUES: Record<string, string> = { "agent.jobs": "thread.#", "memory.jobs": "memory.#" };

/** Throw from a handler to re-deliver after a delay WITHOUT counting a failure (e.g. thread locked). */
export class RetryLater extends Error {}

export type Handler = (topic: string, payload: any) => Promise<void>;

export class InMemoryBus implements Bus {
  messages: { topic: string; payload: any }[] = [];
  async publish(topic: string, payload: unknown) {
    this.messages.push({ topic, payload: JSON.parse(JSON.stringify(payload)) });
  }
  popAll() {
    const out = this.messages;
    this.messages = [];
    return out;
  }
}

export class RabbitBus implements Bus {
  private conn!: amqp.ChannelModel;
  private ch!: amqp.ConfirmChannel;

  async setup(prefetch = 10) {
    this.conn = await amqp.connect(RABBITMQ_URL);
    this.ch = await this.conn.createConfirmChannel();          // publisher confirms
    await this.ch.prefetch(prefetch);                          // back-pressure: max un-acked per consumer
    await this.ch.assertExchange(EXCHANGE, "topic", { durable: true });
    for (const [qname, pattern] of Object.entries(QUEUES)) {
      await this.ch.assertQueue(qname, { durable: true });
      await this.ch.bindQueue(qname, EXCHANGE, pattern);
      await this.ch.assertQueue(`${qname}.delay`, {
        durable: true,
        arguments: {
          "x-message-ttl": RETRY_DELAY_MS,
          "x-dead-letter-exchange": "",          // the default exchange...
          "x-dead-letter-routing-key": qname,    // ...routes it straight back to Q
        },
      });
      await this.ch.assertQueue(`${qname}.dlq`, { durable: true });
    }
  }

  async close() {
    await this.ch?.close();
    await this.conn?.close();
  }

  async publish(topic: string, payload: unknown, messageId?: string) {
    this.ch.publish(EXCHANGE, topic, Buffer.from(JSON.stringify(payload)), {
      persistent: true, messageId, contentType: "application/json", headers: { topic, attempt: 0 },
    });
    await this.ch.waitForConfirms();
  }

  private async sendToQueue(qname: string, topic: string, payload: unknown, attempt: number) {
    this.ch.sendToQueue(qname, Buffer.from(JSON.stringify(payload)), { persistent: true, headers: { topic, attempt } });
    await this.ch.waitForConfirms();
  }

  /**
   * Competing-consumer loop with manual ack, delayed retry and a DLQ.
   * We ALWAYS ack the original and re-publish a copy to .delay or .dlq when needed.
   * (nack+requeue would retry instantly in a hot loop.)
   */
  async consume(qname: string, handler: Handler, onDead?: (topic: string, payload: any, err: string) => Promise<void>) {
    await this.ch.consume(qname, async (msg) => {
      if (!msg) return;
      const topic = String(msg.properties.headers?.topic ?? "");
      const attempt = Number(msg.properties.headers?.attempt ?? 0);
      const payload = JSON.parse(msg.content.toString());
      try {
        await handler(topic, payload);
      } catch (e) {
        if (e instanceof RetryLater) {
          await this.sendToQueue(`${qname}.delay`, topic, payload, attempt);
        } else if (attempt + 1 >= MAX_JOB_ATTEMPTS) {
          console.error(`[bus] DEAD ${topic}`, e);
          await this.sendToQueue(`${qname}.dlq`, topic, payload, attempt + 1);
          await onDead?.(topic, payload, String(e));
        } else {
          console.warn(`[bus] retry ${topic} attempt=${attempt + 1}`, (e as Error).message);
          await this.sendToQueue(`${qname}.delay`, topic, payload, attempt + 1);
        }
      }
      this.ch.ack(msg);
    });
  }
}
