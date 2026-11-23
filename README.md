# Vali mock: long-running nurse-staffing agents (TypeScript + LangGraph.js)

A **study project** for interview prep. It mocks what was built at Vali Health:
AI agents that **text and call nurses to fill open shifts**, wait hours for replies,
retry in waves, negotiate, **ask a human before anything risky**, book the shift
in the facility's system, and **remember** what they learn.

It's built to be read, not deployed: flat files, plain functions, lots of comments.
The architecture is real, though, and it went through a principal-engineer review. The 9
high-severity bugs that review reproduced each have a regression test now.
**30 scenario tests** run against real Postgres and a real job queue.

## Stack
| Concern | Library | Notes |
|---|---|---|
| Agent state machines | **LangGraph.js** | `interrupt`, `Command`, `Send`, Postgres checkpointer, `PostgresStore` (pgvector) |
| App tables | **Drizzle ORM** + drizzle-kit | All in Postgres schema `app`; LangGraph keeps `public`, the queue keeps `graphile_worker` |
| Jobs | **graphile-worker** | Enqueued *inside* the business transaction; a named queue per thread runs it one job at a time; retries; cron |
| HTTP | **Hono** + `@hono/zod-validator` | Webhooks, approvals inbox, console UI |
| LLM | **Anthropic SDK** (`claude-opus-5-5`) | Structured outputs; offline fallback rules when there's no key |
| Voice | **Retell** adapter + local simulator | Outbound calls with memory in the prompt; post-call webhook |
| Logs / tests | pino · vitest | |
| Reference only | RabbitMQ (`amqplib`) + transactional outbox | `src/reference/rabbitmq.ts`: what you'd use with non-Postgres consumers |

## Quick start
```bash
docker compose up -d          # Postgres (pgvector) + RabbitMQ (only the reference test uses it)
npm install
npm run seed                  # runs all migrations, seeds 2 facilities, 8 nurse profiles, shifts
npm run demo                  # narrated end-to-end story (incl. a crash and recovery)
npm test                      # 30 scenarios + 2 RabbitMQ reference tests
npm run services              # gateway + workers + cron; console at http://localhost:3000
npm run db:studio             # browse the app tables (drizzle-kit studio)
```
No LLM key is needed. Set `ANTHROPIC_API_KEY` to use Claude, or set `COMMS_PROVIDER=retell`
plus the `RETELL_*` vars for real phone calls (see `.env.example`).

## Reading order (about 2 hours)
| # | Read | You'll learn |
|---|---|---|
| 1 | `npm run demo` output | The whole story on one screen |
| 2 | [docs/01_ARCHITECTURE.md](docs/01_ARCHITECTURE.md) | Components, the life of one SMS, data model, key choices |
| 3 | `src/graphs/shiftFill.ts` | The long-running agent: waves, `interrupt()`, `Command`, `Send`, approval gate, replay safety |
| 4 | `src/graphs/intake.ts` | Supervisor: triage, safety rails, fan-out to child threads, fan-in, double texting |
| 5 | `src/worker.ts` | Draining the mailbox; the checkpoint as source of truth; reconciliation; lease |
| 6 | `src/mailbox.ts`, `src/jobs.ts`, `src/tasks.ts`, `src/timers.ts` | Transactional enqueue, per-thread queues, cron, durable timers, reaper |
| 7 | [docs/02_LONG_RUNNING_AGENTS.md](docs/02_LONG_RUNNING_AGENTS.md) | Why each reliability piece exists |
| 8 | `src/memory.ts` + [docs/03_MEMORY.md](docs/03_MEMORY.md) | Profile, preference, episodic, semantic and procedural memory |
| 9 | `src/tools.ts`, `src/llm.ts` | Tool gateway (risk tiers, exactly-once booking, audit); LLM gateway |
| 10 | [docs/04_HUMAN_IN_THE_LOOP.md](docs/04_HUMAN_IN_THE_LOOP.md) | Six HITL patterns, per-kind decision validation, learning from corrections |
| 11 | `src/db/schema.ts`, `drizzle/0001_invariants.sql` | The ORM schema; invariants enforced by the database |
| 12 | `tests/scenarios.test.ts` | 30 stories that prove it all; read them like a script |
| 13 | [docs/05_INTERVIEW_QA.md](docs/05_INTERVIEW_QA.md) | Your Vali story, Q&A, system-design walkthrough, review findings |
| 14 | [docs/06_ADALAT_AI.md](docs/06_ADALAT_AI.md) | Mapping all of this to Adalat AI |

## File map
```
src/
  config.ts, clock.ts, log.ts, faults.ts
  db/schema.ts       Drizzle schema (app.*): domain + control plane (runs, mailbox, timers, approvals, offers, tool_calls)
  db/index.ts        one pg.Pool shared by Drizzle, LangGraph and graphile-worker; migrations; truncate for tests
  mailbox.ts         deliver(): mailbox row + kick job in the caller's transaction; startThread()
  jobs.ts            graphile add_job helpers: per-thread queues, per-caregiver memory queues
  tasks.ts           graphile task list + crontab; runUntilIdle for tests and the demo
  worker.ts          processThread: lease, reconcile, project, drain mailbox, invoke/resume; onThreadDead
  timers.ts          durable timers, approval SLA (transactional page), reaper
  tools.ts           tool gateway: zod, risk tiers, verified approvals, idempotency, exactly-once internal tools
  llm.ts             LLM gateway: Claude structured outputs + offline heuristics
  memory.ts          long-term memory on PostgresStore + pgvector; consolidation; memory tasks
  comms.ts           Comms adapter: simulator | Retell
  gateway.ts         Hono API: SMS/Retell webhooks, shifts, cancel, approvals (per-kind validation), threads, UI
  graphs/            intake.ts (supervisor), shiftFill.ts (long-running), index.ts (versioned registry), versions.ts
  reference/rabbitmq.ts   outbox relay + RabbitMQ topology (delay queue, DLQ): the non-Postgres alternative
  seed.ts, demo.ts, services.ts
drizzle/             generated migrations + 0001_invariants.sql (pgvector, btree_gist, no-overlap EXCLUDE)
tests/scenarios.test.ts, tests/rabbit.test.ts
```

## What the tests prove
| Tests | Concept |
|---|---|
| 1–5 | Supervisor fan-out/fan-in, wave timers → escalation → manual assign, explainable SQL filters + `Send` scoring, double texting, cancel |
| 6–13 | Approval gate with edit; reject → reflection → procedural memory → new policy; emergency rail; low-confidence triage; draft-review mode; per-kind decision validation (400s); stale decision + double click (409); approval SLA |
| 14–16, 22 | Mailbox buffering, duplicate webhooks, crash after booking → resume from checkpoint, stale timers |
| 17–21 | **Review findings:** crash before bookkeeping (C1/C2), replay of `handle_event` (C3), crash inside the booking transaction (C4), DB-enforced no-overlap (C6), failed child unblocks parent (C9) |
| 23–24 | Reaper force-unlocks a dead worker; 4 parallel workers give the same result |
| 25–27 | Preference learned from an SMS filters future shifts; episodic memory changes ranking; consolidation + authority rules |
| 28–30 | Retell call with memory in the prompt + transcript learning; mid-call accept + post-call webhook = one acceptance (C5); bad input → 400 |
