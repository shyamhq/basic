# Vali mock: long-running nurse-staffing agents (TypeScript + LangGraph.js)

A **study project** for interview prep. It mocks what was built at Vali Health:
AI agents that **text and call nurses to fill open shifts**, wait hours for replies,
retry in waves, negotiate, **ask a human before anything risky**, book the shift
in the facility's system, and **remember** what they learn.

It's built to be read, not deployed: flat files, plain functions, lots of comments.
The architecture is real, though. It covers durable execution, a mailbox per thread,
outbox, durable timers, per-thread locks, idempotent tools, retries and DLQ,
four types of long-term memory, and six kinds of human-in-the-loop (HITL).
**20 scenario tests** prove each piece works.

## Quick start

```bash
docker compose up -d          # Postgres (pgvector) + RabbitMQ (UI on :15672, guest/guest)
npm install
npm run seed                  # 2 facilities, 8 nurse profiles, shifts
npm run demo                  # narrated end-to-end story in your terminal (no network needed)
npm test                      # 20 scenario tests (real Postgres; RabbitMQ test auto-skips if it's down)
npm run services              # the real thing: gateway + workers + relay + scheduler over RabbitMQ
open http://localhost:3000    # console: be the nurse (SMS simulator) + approvals inbox
```

There's no LLM key? No problem. Every model call has an offline fallback, so everything
runs as is. Set `ANTHROPIC_API_KEY` to use Claude (`claude-opus-5-5`) for triage, reply
parsing, memory extraction and reflection. For real phone calls, set
`COMMS_PROVIDER=retell` plus the `RETELL_*` vars in `.env.example`.

## Reading order (about 2 hours)

| # | Read | You'll learn |
|---|---|---|
| 1 | `npm run demo` output | The whole story in one screen |
| 2 | [docs/01_ARCHITECTURE.md](docs/01_ARCHITECTURE.md) | Components, the life of one SMS, data model |
| 3 | `src/graphs/shiftFill.ts` | The long-running agent: waves, `interrupt()`, `Command`, `Send`, approval gate |
| 4 | `src/graphs/intake.ts` | Supervisor: triage, safety rails, fan-out to child threads, fan-in |
| 5 | `src/worker.ts` | Mailbox drain, per-thread lock, crash recovery, HITL inbox rows |
| 6 | `src/outbox.ts`, `src/bus.ts`, `src/timers.ts` | Outbox, RabbitMQ retry/DLQ, durable timers, reaper |
| 7 | [docs/02_LONG_RUNNING_AGENTS.md](docs/02_LONG_RUNNING_AGENTS.md) | Why each reliability piece exists |
| 8 | `src/memory.ts` + [docs/03_MEMORY.md](docs/03_MEMORY.md) | Profile / preference / episodic / semantic / procedural memory |
| 9 | `src/tools.ts`, `src/llm.ts` | Tool gateway (risk tiers, idempotency, audit), LLM gateway (schemas, fallbacks) |
| 10 | [docs/04_HUMAN_IN_THE_LOOP.md](docs/04_HUMAN_IN_THE_LOOP.md) | Six HITL patterns, and learning from corrections |
| 11 | `tests/scenarios.test.ts` | 19 stories that prove it all; read them like a script |
| 12 | [docs/05_INTERVIEW_QA.md](docs/05_INTERVIEW_QA.md) | Your Vali story, 40+ Q&A, a system-design walkthrough |
| 13 | [docs/06_ADALAT_AI.md](docs/06_ADALAT_AI.md) | Mapping all of this to Adalat AI (courts, transcription, judge-in-the-loop) |

## File map

```
src/
  config.ts        knobs (wave size, timeouts, overtime, SLA)
  clock.ts         controllable clock (tests fast-forward time)
  db.ts            Postgres pool + the whole schema (domain + control plane)
  outbox.ts        transactional outbox, mailbox delivery, startThread
  bus.ts           RabbitMQ topology (work queues, delay queues, DLQ) + in-memory bus
  llm.ts           LLM gateway: Claude structured outputs + offline heuristics
  memory.ts        long-term memory on LangGraph PostgresStore + pgvector, consolidation
  comms.ts         Comms adapter: local simulator | Retell outbound calls
  tools.ts         tool gateway: zod schemas, risk tiers, idempotency keys, audit
  timers.ts        durable timers, approval SLA escalation, reaper
  faults.ts        fault injection (crash tests)
  graphs/
    intake.ts      supervisor agent (triage, emergency, fan-out/fan-in)
    shiftFill.ts   long-running shift-fill agent
    index.ts       compile graphs with Postgres checkpointer + store
  worker.ts        mailbox-draining worker (per-thread advisory lock)
  runtime.ts       topic -> handler; runUntilIdle for tests/demo
  gateway.ts       Hono API: webhooks (SMS, Retell), approvals, threads, UI
  services.ts      run gateway / workers / relay / scheduler over RabbitMQ
  seed.ts, demo.ts
tests/
  scenarios.test.ts  19 end-to-end scenarios
  rabbit.test.ts     real RabbitMQ retry -> DLQ
```

## What each test proves

| Test | Concept |
|---|---|
| 1 | Supervisor fan-out to child threads, fan-in barrier, checkpoint history |
| 2 | Durable wave timers, next wave, escalation handoff, manual assign |
| 3 | Hard filters in SQL (explainable), `Send` map-reduce scoring |
| 4 | Risk-tiered approval gate, human **edit**, audit trail of who approved |
| 5 | Human **reject** + reason → reflection → procedural memory → new policy applies |
| 6 | Safety rail in code (emergency bypasses the model), idempotent side effects on resume |
| 7 | Low-confidence triage → human picks the intent → graph continues |
| 8 | "Review every outbound message" mode, with the human editing drafts |
| 9 | Stale-decision guard (interrupt id), two coordinators clicking at once (409) |
| 10 | Approval SLA → page manager exactly once |
| 11 | **Mailbox**: an event that arrives during an approval is buffered, then consumed |
| 12 | Duplicate webhooks + double replies → never double-book |
| 13 | **Crash** after booking → reaper → resume from checkpoint → no double booking |
| 14 | Stale timer from an old wave ignored |
| 15 | Preference learned from an SMS ("no nights anymore") filters a future shift |
| 16 | Episodic memory (vector search + filter) changes the ranking |
| 17 | Consolidation: episodes → soft preference; decay; what the nurse said beats an inference |
| 18 | Urgent shift → Retell **call** with memory in the prompt → `call_analyzed` webhook → booked + learned |
| 19 | Retell mid-call function becomes a mailbox event (the voice agent can't book directly) |
| rabbit | Real RabbitMQ: delayed retry, `RetryLater`, poison message → DLQ |
