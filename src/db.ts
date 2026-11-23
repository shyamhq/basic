// Postgres: ONE database for everything.
//
//   domain data (facilities, nurses, shifts)      -> plain tables (mock of the facility's scheduling system)
//   agent short-term memory (graph checkpoints)   -> LangGraph PostgresSaver tables
//   agent long-term memory (prefs/episodes/facts) -> LangGraph PostgresStore + pgvector
//   control plane (runs, mailbox, timers, HITL)   -> plain tables below
//   transactional outbox -> RabbitMQ              -> plain table below
//
// Because it is one database, "change business state + enqueue the follow-up job"
// is ONE transaction. That single fact removes a whole class of distributed bugs.

import pg from "pg";
import { DATABASE_URL } from "./config.js";

export const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 20 });

type Queryable = pg.Pool | pg.PoolClient;

/** Run a query, return all rows. Pass `c` to run inside a transaction. */
export async function q<T = any>(sql: string, params: unknown[] = [], c: Queryable = pool): Promise<T[]> {
  const res = await c.query(sql, params as any[]);
  return res.rows as T[];
}

/** Run a query, return the first row (or null). */
export async function one<T = any>(sql: string, params: unknown[] = [], c: Queryable = pool): Promise<T | null> {
  return (await q<T>(sql, params, c))[0] ?? null;
}

/** `await tx(async (c) => { ...q(sql, params, c)... })` -> all-or-nothing. */
export async function tx<T>(fn: (c: pg.PoolClient) => Promise<T>): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    const out = await fn(c);
    await c.query("COMMIT");
    return out;
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    c.release();
  }
}

export const SCHEMA = /* sql */ `
CREATE EXTENSION IF NOT EXISTS vector;

-- ======================= DOMAIN (mock facility scheduling system) =======================
CREATE TABLE IF NOT EXISTS agencies (
  id text PRIMARY KEY, name text NOT NULL,
  review_outbound boolean NOT NULL DEFAULT false   -- HITL mode: a human approves every outbound message
);
CREATE TABLE IF NOT EXISTS facilities (
  id text PRIMARY KEY, agency_id text NOT NULL, name text NOT NULL,
  x real NOT NULL, y real NOT NULL,                -- fake map coordinates in miles
  requirements text[] NOT NULL DEFAULT '{}',       -- certs every nurse must hold, e.g. TB_TEST
  notes text
);
-- The nurse / caregiver PROFILE (structured, editable by staff). Learned preferences live in memory.
CREATE TABLE IF NOT EXISTS caregivers (
  id text PRIMARY KEY, agency_id text NOT NULL, name text NOT NULL, phone text UNIQUE NOT NULL,
  license text NOT NULL,                           -- RN | LVN | CNA
  certs text[] NOT NULL DEFAULT '{}', specialties text[] NOT NULL DEFAULT '{}',
  languages text[] NOT NULL DEFAULT '{}', bio text,
  x real NOT NULL, y real NOT NULL,
  min_rate real NOT NULL,                          -- will not work below this $/hr
  max_distance_mi real NOT NULL,
  shift_prefs text[] NOT NULL DEFAULT '{}',        -- day | evening | night
  preferred_channel text NOT NULL DEFAULT 'sms',   -- sms | call
  contact_start int NOT NULL DEFAULT 8, contact_end int NOT NULL DEFAULT 21,  -- local hours OK to contact
  hours_this_week real NOT NULL DEFAULT 0,
  reliability real NOT NULL DEFAULT 0.9,           -- show-up rate
  active boolean NOT NULL DEFAULT true
);
CREATE TABLE IF NOT EXISTS shifts (
  id text PRIMARY KEY, agency_id text NOT NULL, facility_id text NOT NULL,
  role text NOT NULL, specialty text,
  starts_at timestamptz NOT NULL, ends_at timestamptz NOT NULL,
  rate real NOT NULL, max_rate real NOT NULL,      -- posted rate / most the facility will pay
  status text NOT NULL,                            -- scheduled | open | filled | cancelled | completed
  caregiver_id text
);

-- ======================= AGENT CONTROL PLANE =======================
-- LangGraph owns a thread's STATE; this table owns its LIFECYCLE.
CREATE TABLE IF NOT EXISTS agent_runs (
  thread_id text PRIMARY KEY, agency_id text NOT NULL, graph text NOT NULL,
  parent_thread_id text,
  status text NOT NULL DEFAULT 'queued',  -- queued | running | waiting_event | waiting_human | done | failed
  waiting_for text, heartbeat_at timestamptz, error text,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);

-- MAILBOX (actor model). Every stimulus for a thread lands here first:
-- start input, nurse replies, timer firings, human decisions, child results.
CREATE TABLE IF NOT EXISTS thread_inbox (
  id bigserial PRIMARY KEY, thread_id text NOT NULL, kind text NOT NULL, payload jsonb NOT NULL,
  dedupe_key text UNIQUE,
  status text NOT NULL DEFAULT 'pending',     -- pending | consumed | ignored | stale
  note text, created_at timestamptz NOT NULL DEFAULT now(), consumed_at timestamptz
);
CREATE INDEX IF NOT EXISTS inbox_pending ON thread_inbox (thread_id, id) WHERE status = 'pending';

-- TRANSACTIONAL OUTBOX -> relayed to RabbitMQ (at-least-once).
CREATE TABLE IF NOT EXISTS outbox (
  id bigserial PRIMARY KEY, topic text NOT NULL, payload jsonb NOT NULL, dedupe_key text UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- DURABLE TIMERS: "wake me in 15 min" must survive deploys/crashes -> it is a row, not setTimeout.
CREATE TABLE IF NOT EXISTS timers (
  id text PRIMARY KEY, thread_id text NOT NULL, kind text NOT NULL, payload jsonb NOT NULL DEFAULT '{}',
  fire_at timestamptz NOT NULL, fired_at timestamptz, cancelled boolean NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS timers_due ON timers (fire_at) WHERE fired_at IS NULL AND NOT cancelled;

-- HUMAN-IN-THE-LOOP INBOX (what staffing coordinators see).
CREATE TABLE IF NOT EXISTS approvals (
  id bigserial PRIMARY KEY, thread_id text NOT NULL, interrupt_id text UNIQUE NOT NULL,
  kind text NOT NULL, payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'pending',     -- pending | decided | stale
  decision jsonb, decided_by text, escalated boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(), decided_at timestamptz
);

-- Offers sent to nurses. The code ("YES 4821") correlates an inbound reply to a thread.
CREATE TABLE IF NOT EXISTS offers (
  code text PRIMARY KEY, thread_id text NOT NULL, shift_id text NOT NULL, caregiver_id text NOT NULL,
  wave int NOT NULL, channel text NOT NULL, status text NOT NULL DEFAULT 'sent',  -- sent | accepted | declined | countered | withdrawn
  created_at timestamptz NOT NULL DEFAULT now(), UNIQUE (thread_id, caregiver_id)
);

-- TOOL GATEWAY: audit log AND idempotency cache in one row.
CREATE TABLE IF NOT EXISTS tool_calls (
  idempotency_key text PRIMARY KEY, thread_id text, tool text NOT NULL, args jsonb NOT NULL,
  result jsonb, approved_by text, created_at timestamptz NOT NULL DEFAULT now()
);

-- Everything "sent" by the simulator (and real sends, for audit).
CREATE TABLE IF NOT EXISTS comms_log (
  id bigserial PRIMARY KEY, channel text NOT NULL, direction text NOT NULL,
  phone text NOT NULL, body text NOT NULL, meta jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS inbound_requests (    -- webhook de-duplication at the gateway
  idempotency_key text PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dead_letters (
  id bigserial PRIMARY KEY, topic text, payload jsonb, error text, created_at timestamptz NOT NULL DEFAULT now()
);
`;

const APP_TABLES = [
  "agencies", "facilities", "caregivers", "shifts", "agent_runs", "thread_inbox", "outbox", "timers",
  "approvals", "offers", "tool_calls", "comms_log", "inbound_requests", "dead_letters",
];
// LangGraph-owned tables are TRUNCATED, not dropped: the saver/store objects remember
// that they already ran their migrations and would not recreate dropped tables.
const LANGGRAPH_DATA_TABLES = ["checkpoints", "checkpoint_blobs", "checkpoint_writes", "store", "store_vectors"];

export async function initSchema({ reset = false } = {}) {
  if (reset) {
    for (const t of APP_TABLES) await q(`DROP TABLE IF EXISTS ${t} CASCADE`);
    for (const t of LANGGRAPH_DATA_TABLES) {
      if ((await one(`SELECT to_regclass($1) AS t`, [`public.${t}`]))?.t) await q(`TRUNCATE ${t} CASCADE`);
    }
  }
  await q(SCHEMA);
}
