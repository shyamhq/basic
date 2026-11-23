// Structured logs. Every line about a thread carries threadId so you can grep one thread's story.
// In production, add OpenTelemetry and put the traceId on mailbox rows / job payloads too.
import pino from "pino";

export const log = pino({ level: process.env.LOG_LEVEL ?? (process.env.VITEST ? "silent" : "info") });
