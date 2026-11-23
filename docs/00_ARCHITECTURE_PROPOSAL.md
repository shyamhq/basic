# Architecture proposal: a mock of Vali Health's nurse-shift agents (TypeScript)

Status: **proposal, waiting for review**. No code yet.

Goal: a mock system, small enough to read, that covers everything an interviewer
might ask about agent orchestration, long-running agents, memory and human-in-the-loop (HITL).
It is not production code: flat files, simple functions, lots of comments.

## Domain
Care facilities post open nursing shifts. The agents text and call nurses or
caregivers, wait for replies, retry, negotiate, ask a human to approve risky actions,
and book the shift in the facility's scheduling system.

## Stack
| Concern | Choice | Why |
|---|---|---|
| Agent state machines | **LangGraph.js** (`interrupt`, `Command`, `Send`, Postgres checkpointer, `PostgresStore`) | The terms interviewers use. Mastra is the alternative and is compared in the docs |
| Database | **Postgres + pgvector** | One database for domain data, checkpoints, memory, outbox, mailbox, timers and approvals, so a change and its job commit in one transaction |
| Queue | **RabbitMQ** (`amqplib`) | Job semantics: ack, delayed retry, dead-letter queue (DLQ). Kafka is discussed as the event-log alternative |
| HTTP gateway | **Hono** + **zod** | Small, typed, strict schemas |
| LLM | **Anthropic SDK** (`claude-opus-5-5`) behind a small LLM gateway | Falls back to offline rules when there is no API key |
| Voice/SMS | **Retell** adapter + local **simulator** | $10 free credit, outbound call API, mid-call tool calls, `call_analyzed` webhook. Real US SMS needs A2P 10DLC registration (2–3 weeks) |
| Tests | **vitest** scenario tests | |

## Diagram
```
Nurse phone ⇄ Retell (voice/SMS) ── webhooks + mid-call tools ──┐
Scheduler UI (approval inbox) ──────────────────────────────────┤
Facility scheduling system (posts open shifts) ─────────────────┤
                                                                ▼
                  GATEWAY (Hono): auth, tenant, signature check, idempotency
                  ONE transaction: mailbox event + outbox row
                                                                ▼
   Postgres ──outbox relay──► RabbitMQ  agent.jobs / memory.jobs (+delay, +DLQ)
                                                                ▼
   AGENT WORKERS ×N: per-thread lock → drain mailbox → LangGraph start/resume
                     → TOOL GATEWAY (risk tiers, idempotency keys, audit log)
                     → Retell / scheduling system
   Background: TIMER SCHEDULER · REAPER (dead runs) · MEMORY WORKER · nightly CONSOLIDATION
```

## Agents (graphs)
1. **Intake/Supervisor:** sorts each inbound message: call-out, facility request,
   preference update, question, or emergency. Emergencies always go to a human. Low-confidence
   classifications go to a human for review.
2. **ShiftFill** (long-running, one thread per open shift):
   - Recall memory, then filter candidates on hard rules (license, availability, distance, overtime).
   - Score candidates in parallel with `Send`, then reach out in waves on each nurse's preferred channel.
   - `interrupt()` waits for a reply or a timer.
   - Negotiate: a pay bump needs a human.
   - Approval gate, then book the shift in the facility's system.
   - Withdraw the other offers, write an episode to memory, end.
   - Escalate to a human after N waves or at the deadline.
3. **Parent→children:** a nurse calls out of 3 shifts → 3 ShiftFill threads; the parent waits for all of them.

## Long-running mechanics
- **Mailbox:** every stimulus (start, reply, timer, human decision, child result) goes
  into a per-thread inbox. The worker drains it one event at a time, and an event the
  graph isn't waiting for is held until it is.
- **Durable timers:** rows in Postgres, not `setTimeout`.
- **Per-thread advisory lock** so two workers never run the same thread.
- **Transactional outbox** to RabbitMQ: at-least-once delivery, idempotent consumers.
- **Idempotency keys** on every side-effecting tool call, so a replay is safe.
- **Reaper:** finds runs whose heartbeat has gone stale and resumes them from the last checkpoint.
- **Retry with backoff → DLQ** for jobs that keep failing; the run is marked failed and a human is alerted.

## Memory (stored in LangGraph's `PostgresStore` with pgvector)
| Type | Example | Write | Read |
|---|---|---|---|
| Working | thread state, messages | checkpointer | every step |
| Profile | bio, licenses, specialties, languages, pay floor, max distance, shift-time preferences, best channel, contact hours | admin / onboarding | filter, personalize |
| Preferences (learned) | "no nights anymore", "don't call before 9" | extracted from SMS and call transcripts, with provenance; what the nurse said beats an inference | ranking, outreach |
| Episodic | "declined Sunrise twice, too far", "no-show 3/4" | end of each run | vector search before ranking |
| Semantic | facility facts ("requires TB test") | extraction | outreach answers |
| Procedural | playbook rules learned from human corrections | reflection on HITL edits and rejections | policy gate, prompts |

A nightly consolidation job turns repeated episodes into preferences, merges duplicates and lets stale memories fade.

## Human in the loop
- Approve / edit / reject gates on risky tool calls (overtime, rate above cap, first shift at a facility).
- Optional "review every outbound message" mode.
- Escalation handoff with a context summary when the agent can't fill a shift.
- Human review for low-confidence triage; emergencies always go to a human.
- Approval deadlines that escalate to a manager.
- A stale-approval guard: a decision only resumes the thread if its `interrupt_id` is still the current interrupt.
- Human corrections become procedural memory.

## Scenario tests (planned)
- First-wave fill.
- Timeout → next wave → escalation.
- Overtime approval edited by a human.
- Emergency goes straight to a human.
- Low-confidence triage goes to human review.
- Duplicate webhook.
- An event that arrives during a pending approval is buffered.
- Crash recovery by the reaper.
- Memory blocks a candidate.
- Stale approval rejected.
- Fan-out / fan-in.
- Consolidation.
- A Retell transcript updates preferences.

## Open decisions
1. Domain: facility nurse shift filling (matches the résumé) or home-care agencies (Vali's press coverage)?
2. Voice/SMS: Retell + simulator (recommended) or Vapi?
3. Framework: LangGraph.js (recommended) or Mastra?
