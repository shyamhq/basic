# 02 · Long-running agents

A shift-fill run lasts **minutes to a day**. In that time nurses reply whenever they like,
humans approve slowly, deploys happen and pods die. "Long-running" means the agent's
**progress lives in the database, not in a process**.

## 1. Checkpoints: the agent's short-term memory
`graph.invoke(..., { durability: "sync" })` with `PostgresSaver` writes a checkpoint after
**every super-step**: state values, the next nodes, and pending writes.

- `durability: "sync"` → the checkpoint is persisted before the next step starts (safest).
  `"async"` overlaps writing with the next step (faster; a crash can lose the last step).
  `"exit"` writes only at the end or at an interrupt (fastest; least durable).
- `getState(cfg)` → current values, `next`, and `tasks[].interrupts`.
- `getStateHistory(cfg)` → every checkpoint ("time travel", audit, debugging). See `GET /api/threads/:id/history`.

> **Checkpointing is not full durable execution.** LangGraph saves state *between*
> nodes. If a process dies *inside* a node, that node re-runs from the top. That's why
> side effects need idempotency keys (§5). Temporal-style engines also record
> *activity results* inside a step, which is the main difference to mention.

## 2. `interrupt()`: pausing without holding anything
```ts
const ev = interrupt({ type: "wait_event", accepts: ["nurse_reply", "timer"] });
```
- The first time through, `interrupt` throws a special signal. LangGraph saves the
  checkpoint and returns `__interrupt__` to the caller. **The worker is now free.**
- To resume, call `graph.invoke(new Command({ resume: value }), cfg)`. The **whole node
  re-runs from its first line**, and this time `interrupt()` *returns* `value`.
- Consequences (see the rules at the top of `shiftFill.ts`):
  - Put `interrupt()` first. Side effects go *after* it, or must be idempotent
    (`emergency` in `intake.ts` deliberately pages *before* the interrupt and relies on the
    tool's idempotency key, and test 6 proves the page goes out only once).
  - Several `interrupt()` calls in one node are matched **by order**. Don't put them in
    conditional loops; loop with edges instead (`wait → handle_event → wait`).
- Each interrupt payload declares `accepts`, which is the contract the worker uses to choose
  which mailbox event may resume it.

## 3. The mailbox (actor model)
Every stimulus goes to `thread_inbox` first, then a `thread.kick` job.
`drainMailbox` (`src/worker.ts`) then decides:

| Thread state | Action |
|---|---|
| No checkpoint yet | `invoke(start input)` |
| Paused at an interrupt | Oldest pending event whose `kind ∈ accepts` → `invoke(Command({resume}))` |
| Has `next` but no interrupt | It **crashed mid-run** → `invoke(null)` continues from the last checkpoint |
| Finished | Leftover events → `ignored` (late nurse replies get a polite SMS) |

Why not resume straight from the webhook?
- **Buffering.** A nurse texts while the thread waits on a human approval. The approval
  interrupt doesn't accept `nurse_reply`, so the event waits its turn (test 11).
- **Ordering and serialization.** One thread processes one event at a time, in order.
- **Dedupe.** `dedupe_key` is unique (provider retries, double clicks).
- **Debuggability.** The mailbox *is* the thread's input log.

## 4. One worker per thread at a time: advisory locks
`pg_try_advisory_lock(hashtext(thread_id))` is held on a dedicated connection for the run.
- If another worker holds it, throw `RetryLater` and the job goes to `agent.jobs.delay` (TTL 2s) → back.
- If the worker dies, its DB session ends and Postgres **releases the lock automatically**.
  Compare a Redis lock, which needs a TTL and fencing tokens.
- Kafka alternative: partition by `thread_id`, so one consumer owns a partition.

## 5. Idempotency everywhere (at-least-once world)
| Layer | Mechanism |
|---|---|
| Webhooks | `inbound_requests` unique key (`sms:<messageId>`, `retell:<call_id>`) |
| Mailbox | `thread_inbox.dedupe_key` unique |
| Outbox | `outbox.dedupe_key` unique |
| Tools | `tool_calls.idempotency_key` = `<thread>:<purpose>` (deterministic), so a re-run returns the cached result |
| Timers | deterministic ids `<thread>:wave:<n>`, `ON CONFLICT DO NOTHING` |
| Offers | deterministic code from `hash(thread, caregiver)` |
| Graph | `seenEvents` in state (idempotent consumer), wave number on timers (stale timer ignored, test 14) |
| HITL | approval `status` checked `FOR UPDATE` (409 on double click), `interruptId` must match the current interrupt |

Test 13 crashes *after* the booking API call but *before* the node finishes. On recovery
the node re-runs and the tool gateway returns the cached result. Result: one booking,
one confirmation SMS, and the nurse's hours bumped once.

## 6. The transactional outbox
Never "write the DB, then publish". Write both in **one transaction** (the outbox row is the
"publish"). `relayOnce` publishes with `FOR UPDATE SKIP LOCKED`, so many relays are safe. If a
relay crashes between publishing and commit, the message is sent twice, which is fine
because every consumer is idempotent. To lower latency, use `LISTEN/NOTIFY` or CDC
(Debezium) instead of polling.

## 7. Durable timers
A `setTimeout` dies with the pod. `timers` rows survive deploys. `fireDueTimers` claims due
rows with `SKIP LOCKED` and delivers `{kind:"timer"}` to the mailbox. Timers used here:
wave timeout, hard deadline (2h before shift start), approval SLA (`escalateStaleApprovals`).
At large scale: bucketed timer wheels, or the delayed messages of SQS or Temporal.

## 8. Failure handling ladder
1. **In-node retry**: `retryPolicy: { maxAttempts: 3 }` on `load_shift`, `send_wave`, `classify`
   (transient API errors, with backoff and jitter).
2. **Job retry**: the worker throws → bus re-publishes to `.delay` with `attempt+1`.
3. **Dead-letter**: after `MAX_JOB_ATTEMPTS`, the job goes to `.dlq`, the `dead_letters` row
   is written, the run is marked `failed` and a **human is paged** (`onDeadJob`). Test `rabbit.test.ts` covers this.
4. **Reaper**: a run stuck in `running` with a stale heartbeat means the process died →
   re-kick → `invoke(null)` (test 13).
5. **Business conflicts** (`ToolConflict`, e.g. the shift was filled elsewhere) are *not*
   retried. The graph handles them (`book` → `filled_elsewhere`).

## 9. Deploying new code while threads are paused (versioning)
Threads may sit at an interrupt for hours across a deploy. Checkpoints store **state and
node names**, not code. Rules of thumb:
- Never rename or remove a node that live threads may be paused in. Add new nodes and keep
  old ones as shims until in-flight threads drain.
- Make state changes additive and optional (new keys with defaults).
- Put a `graphVersion` on `agent_runs`, run old versions side by side or migrate state
  explicitly, and alert on threads paused in unknown nodes. (A third-party report says a
  pending interrupt can be silently dropped after a node rename, so test this.)

## 10. Queue choice cheat-sheet
| | RabbitMQ (here) | Kafka | SQS | Postgres queue (pg-boss) |
|---|---|---|---|---|
| Model | Jobs, per-message ack | Ordered log, offsets | Jobs | Jobs in a table |
| Retry/DLQ | TTL + dead-letter exchange | DIY (retry topics) | Built-in redrive | Built-in |
| Ordering | Per queue (weak with many consumers) | Per partition key | FIFO queues | Per query |
| Replay | No | Yes | No | Kind of |
| Use for | Agent jobs, memory jobs | Telemetry, audio chunks, event sourcing, many consumers | Managed jobs on AWS | Small or medium scale, fewer moving parts |

## 11. Compared with durable-execution engines
| | This mock (LangGraph + harness) | Temporal | Mastra workflows |
|---|---|---|---|
| Unit of durability | Super-step checkpoint | Every activity result (event history) | Workflow snapshot on suspend |
| Waiting for humans/events | `interrupt()` + mailbox | Signals + `await condition` | `suspend()` / `resume()` |
| Timers | `timers` table | Built-in durable timers | Sleep / scheduling primitives |
| You must build | Mailbox, locks, outbox, timers, reaper | Workers + activities | Storage adapter, triggers |

In one line: LangGraph gives you the **agent brain** and its state. The harness around it
(mailbox, outbox, timers, locks, reaper) gives you **durability in the real world**.
That's what "built the agent harness from scratch" means.
