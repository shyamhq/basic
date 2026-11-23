// Drizzle schema for OUR tables. They all live in the Postgres schema "app".
//
// Three owners, three schemas, nobody steps on anybody:
//   app.*             -> this file (Drizzle owns it; migrations in ./drizzle)
//   public.checkpoint*, public.store* -> LangGraph (PostgresSaver / PostgresStore run their own migrations)
//   graphile_worker.* -> graphile-worker (job queue; runs its own migrations)
// drizzle.config.ts sets schemaFilter: ["app"], so drizzle-kit never sees the other two.

import { sql } from "drizzle-orm";
import {
  bigserial, boolean, index, integer, jsonb, pgSchema, real, text, timestamp, unique,
} from "drizzle-orm/pg-core";

export const app = pgSchema("app");
const ts = (name: string) => timestamp(name, { withTimezone: true, mode: "date" });
const now = () => ts("created_at").notNull().defaultNow();

// ============================ DOMAIN (mock of the facility scheduling system) ============================

export const agencies = app.table("agencies", {
  id: text().primaryKey(),
  name: text().notNull(),
  phone: text().notNull().unique(),                         // the number nurses text -> tells us the tenant
  reviewOutbound: boolean("review_outbound").notNull().default(false),   // HITL: approve every outbound message
});

export const facilities = app.table("facilities", {
  id: text().primaryKey(),
  agencyId: text("agency_id").notNull(),
  name: text().notNull(),
  x: real().notNull(), y: real().notNull(),                // fake map coordinates in miles
  requirements: text().array().notNull().default(sql`'{}'`),
  notes: text(),
});

/** The nurse PROFILE: structured, staff-owned. Learned preferences live in long-term memory instead. */
export const caregivers = app.table("caregivers", {
  id: text().primaryKey(),
  agencyId: text("agency_id").notNull(),
  name: text().notNull(),
  phone: text().notNull().unique(),
  license: text().notNull(),                                // RN | LVN | CNA
  certs: text().array().notNull().default(sql`'{}'`),
  specialties: text().array().notNull().default(sql`'{}'`),
  languages: text().array().notNull().default(sql`'{}'`),
  bio: text(),
  x: real().notNull(), y: real().notNull(),
  minRate: real("min_rate").notNull(),
  maxDistanceMi: real("max_distance_mi").notNull(),
  shiftPrefs: text("shift_prefs").array().notNull().default(sql`'{}'`),   // day | evening | night
  preferredChannel: text("preferred_channel").notNull().default("sms"),
  contactStart: integer("contact_start").notNull().default(8),
  contactEnd: integer("contact_end").notNull().default(21),
  hoursThisWeek: real("hours_this_week").notNull().default(0),
  reliability: real().notNull().default(0.9),
  active: boolean().notNull().default(true),
});

// The "no double booking" invariant is an EXCLUSION constraint (see the custom migration):
// one caregiver can't hold two scheduled/filled shifts whose time ranges overlap.
export const shifts = app.table("shifts", {
  id: text().primaryKey(),
  agencyId: text("agency_id").notNull(),
  facilityId: text("facility_id").notNull(),
  role: text().notNull(),
  specialty: text(),
  startsAt: ts("starts_at").notNull(),
  endsAt: ts("ends_at").notNull(),
  rate: real().notNull(),
  maxRate: real("max_rate").notNull(),
  status: text({ enum: ["scheduled", "open", "filled", "cancelled", "completed"] }).notNull(),
  caregiverId: text("caregiver_id"),
});

// ============================ AGENT CONTROL PLANE ============================

/** One row per durable thread. LangGraph's checkpoint is the source of truth; status/waiting_for
 *  are a PROJECTION of it, recomputed by the worker on every drain. */
export const agentRuns = app.table("agent_runs", {
  threadId: text("thread_id").primaryKey(),
  agencyId: text("agency_id").notNull(),
  graph: text().notNull(),
  graphVersion: integer("graph_version").notNull(),
  subject: text().notNull(),                                // shift id or phone: natural key for "is one already running?"
  parentThreadId: text("parent_thread_id"),
  status: text({ enum: ["queued", "running", "waiting_event", "waiting_human", "done", "failed"] }).notNull().default("queued"),
  waitingFor: text("waiting_for"),
  leaseOwner: text("lease_owner"),                          // graphile worker id holding the thread right now
  leaseVersion: integer("lease_version").notNull().default(0),   // fencing token, bumped on each drain
  heartbeatAt: ts("heartbeat_at"),
  error: text(),
  createdAt: now(),
}, (t) => [index("agent_runs_subject").on(t.graph, t.subject)]);

/** The MAILBOX. Every stimulus for a thread lands here first. */
export const threadInbox = app.table("thread_inbox", {
  id: bigserial({ mode: "number" }).primaryKey(),
  agencyId: text("agency_id").notNull(),
  threadId: text("thread_id").notNull(),
  kind: text({ enum: ["start", "nurse_reply", "nurse_message", "timer", "human_decision", "child_done", "cancel"] }).notNull(),
  payload: jsonb().$type<Record<string, any>>().notNull(),
  dedupeKey: text("dedupe_key").unique(),
  // pending -> inflight (handed to the graph) -> consumed (checkpoint moved past it)
  status: text({ enum: ["pending", "inflight", "consumed", "ignored", "stale"] }).notNull().default("pending"),
  resumedInterruptId: text("resumed_interrupt_id"),         // which interrupt we resumed with it ("start" for start events)
  note: text(),
  createdAt: now(),
}, (t) => [index("inbox_open").on(t.threadId, t.id).where(sql`status in ('pending','inflight')`)]);

/** DURABLE TIMERS: "wake me in 15 minutes" is a row, not a setTimeout. */
export const timers = app.table("timers", {
  id: text().primaryKey(),
  agencyId: text("agency_id").notNull(),
  threadId: text("thread_id").notNull(),
  kind: text().notNull(),
  payload: jsonb().$type<Record<string, any>>().notNull().default({}),
  fireAt: ts("fire_at").notNull(),
  firedAt: ts("fired_at"),
  cancelled: boolean().notNull().default(false),
}, (t) => [index("timers_due").on(t.fireAt).where(sql`fired_at is null and not cancelled`)]);

/** HUMAN-IN-THE-LOOP inbox (also a projection of the checkpoint's current interrupt). */
export const approvals = app.table("approvals", {
  id: bigserial({ mode: "number" }).primaryKey(),
  agencyId: text("agency_id").notNull(),
  threadId: text("thread_id").notNull(),
  interruptId: text("interrupt_id").notNull().unique(),
  kind: text({ enum: ["approval", "escalation", "triage_review", "review_outbound", "emergency_ack"] }).notNull(),
  payload: jsonb().$type<Record<string, any>>().notNull(),
  status: text({ enum: ["pending", "decided", "stale", "cancelled"] }).notNull().default("pending"),
  decision: jsonb().$type<Record<string, any>>(),
  decidedBy: text("decided_by"),
  escalated: boolean().notNull().default(false),
  createdAt: ts("created_at").notNull(),
  decidedAt: ts("decided_at"),
});

/** Offers sent to nurses; the 4-digit code correlates a reply back to its thread. */
export const offers = app.table("offers", {
  code: text().primaryKey(),
  agencyId: text("agency_id").notNull(),
  threadId: text("thread_id").notNull(),
  shiftId: text("shift_id").notNull(),
  caregiverId: text("caregiver_id").notNull(),
  wave: integer().notNull(),
  channel: text().notNull(),
  status: text({ enum: ["sent", "accepted", "countered", "declined", "no_response", "withdrawn"] }).notNull().default("sent"),
  lastEventId: text("last_event_id"),                       // which mailbox event moved it last -> replay-safe transitions
  createdAt: now(),
}, (t) => [unique().on(t.threadId, t.caregiverId)]);

/** TOOL GATEWAY: audit log AND idempotency cache. */
export const toolCalls = app.table("tool_calls", {
  idempotencyKey: text("idempotency_key").primaryKey(),
  agencyId: text("agency_id").notNull(),
  threadId: text("thread_id"),
  tool: text().notNull(),
  args: jsonb().notNull(),
  result: jsonb().$type<{ value?: unknown; error?: string }>(),
  approvedBy: text("approved_by"),
  createdAt: now(),
});

/** Everything sent/received (the simulator's "phone", and an audit trail). */
export const commsLog = app.table("comms_log", {
  id: bigserial({ mode: "number" }).primaryKey(),
  agencyId: text("agency_id").notNull(),
  channel: text().notNull(),
  direction: text({ enum: ["in", "out"] }).notNull(),
  phone: text().notNull(),
  body: text().notNull(),
  meta: jsonb().$type<Record<string, any>>().notNull().default({}),
  createdAt: now(),
});

export const inboundRequests = app.table("inbound_requests", {   // webhook de-duplication
  idempotencyKey: text("idempotency_key").primaryKey(),
  createdAt: now(),
});

/** Only used by the RabbitMQ reference implementation (src/reference). graphile-worker's
 *  jobs table already IS a transactional outbox on the main path. */
export const outbox = app.table("outbox", {
  id: bigserial({ mode: "number" }).primaryKey(),
  topic: text().notNull(),
  payload: jsonb().notNull(),
  createdAt: now(),
  publishedAt: ts("published_at"),
});
