# 02 · Long-running agents

A shift-fill run lasts **minutes to a day**. In that time nurses reply whenever they like,
humans approve slowly, deploys happen and pods die. "Long-running" means the agent's
**progress lives in the database, not in a process**.

## 1. Checkpoints: the source of truth
`graph.invoke(..., { durability: "sync" })` with `PostgresSaver` writes a checkpoint after **every
super-step**: state values, the next nodes, and pending writes.
- `durability`: `"sync"` persists before the next step starts (safest). `"async"` overlaps the write with the next step. `"exit"` writes only at the end or at an interrupt.
- `getState()` gives values, `next` and `tasks[].interrupts`. `getStateHistory()` gives every checkpoint (`GET /api/threads/:id/history`).
- **Everything else is a projection of the checkpoint.** `agent_runs.status/waiting_for` and the
  `approvals` inbox are recomputed by `project()` at the start of **every** drain. A crash between
  `invoke()` and our bookkeeping heals on the next pass. Test 17 crashes exactly there.

> Checkpointing is not full durable execution. LangGraph saves state *between* nodes. A process that
> dies *inside* a node re-runs that node from the top. Hence §5. Temporal-style engines also journal
> each activity result inside a step, which is the difference to name in an interview.

## 2. `interrupt()`: pausing without holding anything
```ts
const ev = interrupt({ type: "wait_event", accepts: ["nurse_reply", "timer", "cancel"] });
```
- **First pass**: `interrupt` throws a signal. LangGraph saves the checkpoint and the run ends. **The worker is free.**
- **Resume**: `invoke(new Command({ resume: value }))` re-runs the **whole node** from line one, and this time `interrupt()` returns `value`.
- **Interrupt ids**: an id is stable across re-runs of the same pause and new for each new pause. The worker relies on this, both to reconcile and to guard against stale decisions.
- **Rules** (top of `shiftFill.ts`):
  - Put `interrupt()` first.
  - Side effects go after it, or must be idempotent. `emergency` pages *before* its interrupt and relies on the tool key (test 8).
  - Loop with edges (`wait → interpret → handle_event → wait`), never with `interrupt()` inside a loop.
- **Non-determinism**: an LLM may classify the same text differently on a replay. So the model call
  lives in its own node (`interpret`), and its output is checkpointed *before* `handle_event` decides
  and writes anything.
- Every interrupt payload declares `accepts`: the contract the worker uses to pick which mailbox event may resume it.

## 3. The mailbox (actor model) + reconciliation
Every stimulus lands in `app.thread_inbox`, together with a kick job, in one transaction.
`processThread` (`src/worker.ts`) then loops:

| Step | What |
|---|---|
| lease | `lease_version += 1`. Every control-plane write below is conditional on it (fencing token) |
| reconcile | Each `inflight` event becomes `consumed` if the checkpoint moved past `resumed_interrupt_id`; otherwise it goes back to `pending` |
| project | Rebuild run status and approvals from the current interrupt; close approvals for pauses the thread left |
| pick | No checkpoint → `start`. Paused → oldest pending event whose `kind ∈ accepts`. Next tasks but no pause → it crashed mid-run → `invoke(null)`. Finished → close leftovers |
| invoke | Mark the event `inflight` + interrupt id, then `invoke` |

Why not resume straight from the webhook?
- **Buffering**: a nurse texts while the thread waits on a human; the approval doesn't accept `nurse_reply`, so the event waits (test 14).
- **Ordering**: one event at a time per thread.
- **Dedupe**: `dedupe_key` is unique.
- **Debuggability**: the mailbox is the thread's input log.
- **Double texting**: a follow-up text joins the open conversation as `nurse_message` (test 4). LangGraph Platform calls the strategies reject / enqueue / interrupt / rollback; this is "enqueue".

## 4. One worker per thread: named queues + a lease
- graphile-worker runs jobs of the queue `thread:<id>` **one at a time**, so two workers never drain the same thread.
- `job_key = kick:<id>` coalesces duplicate kicks. If a kick arrives while one is running, a new job is queued behind it, so no wake-up is lost.
- **Fencing**: no lock is perfect. If a worker is presumed dead and its job force-unlocked, but it was only slow, two runs could overlap.
  - `lease_version` stops the zombie's control-plane writes (`LostLease`).
  - LangGraph's checkpoint writes aren't fenced: the saver doesn't compare-and-set. Say that honestly.
  - Kafka gets this from partition ownership; Temporal gets it from its event history.
- **Reaper** (`reapDeadRuns`): if a run sits in `running` with a stale heartbeat, its worker died. graphile would only unlock that job after 4h, so the reaper calls `force_unlock_workers` and re-kicks (test 23).

## 5. Idempotency and exactly-once where it matters
| Layer | Mechanism |
|---|---|
| Webhooks | `inbound_requests` key (`sms:<messageId>`, `retell:<call>:analyzed`) |
| Meaning-level dedupe | Retell mid-call `accept_shift` and post-call `call_analyzed` both use `offer:<code>:accept` → one acceptance (test 29) |
| Mailbox / jobs | `dedupe_key` unique / `job_key` |
| Internal tools | Claim + effect + result in **one transaction** → exactly-once (`book_shift`; test 19 crashes inside it) |
| External tools | Claim → call → record: **at-least-once** (a provider can't join our transaction). Use provider idempotency keys where they exist |
| Graph writes | `offers.last_event_id`: a transition applies if the offer is in the expected state **or was moved by this same event**, so a replayed node takes the same branch (test 18) |
| Timers | Deterministic ids (`<thread>:wave:<n>`), `ON CONFLICT DO NOTHING`; wave number carried on the timer (stale ones are ignored, test 22) |
| Invariants | The DB, not the agent: EXCLUDE constraint `no_overlapping_bookings` (test 20), natural thread ids `fill:<shiftId>` |

## 6. Transactional enqueue (the outbox, built in)
The dual-write problem: `db.update(); queue.publish()`. A crash in between either loses the job or
creates a job for a change that never happened. With graphile-worker, `add_job` runs **in the same
transaction** as the state change, so the jobs table *is* the outbox and there's no relay.
`src/reference/rabbitmq.ts` shows the classic version: an `outbox` table + `relayOnce`
(`FOR UPDATE SKIP LOCKED`) + RabbitMQ. You need that when consumers live outside Postgres (Go
services, other teams) or the queue must fan out to many consumer types.

## 7. Durable timers
- A `setTimeout` dies with the pod; a timer row survives deploys.
- The cron job `timers_tick` runs every minute. `fireDueTimers` claims due rows with `SKIP LOCKED` and delivers `{kind:"timer"}` to the mailbox in the same transaction.
- Timers here: wave timeout, hard deadline (2h before start), the parent's children deadline, and the approval SLA.
- Why a table and not graphile `run_at` jobs? Timers are domain state: the UI can show the next wave, tests can fast-forward the clock, and the graph cancels them by id.

## 8. Failure handling ladder
1. **In-node retry**: `retryPolicy: { maxAttempts: 3 }` on `load_shift`, `send_wave`, `interpret`, `classify`.
2. **Job retry**: graphile retries with exponential backoff (`max_attempts = 5`).
3. **Dead letter**: on the last attempt, `onThreadDead`, in one transaction:
   - mark the run failed;
   - cancel its approvals;
   - enqueue a page to a human;
   - deliver `child_done{failed}` to the parent, so its fan-in never hangs (test 21).
4. **Reaper** for workers that vanished (§4).
5. **Business conflicts** (`ToolConflict`: shift taken, nurse double-booked) are not retried. The graph routes them: an overlap moves on to the next nurse; a shift that's gone finishes the thread.

## 9. Deploying new code while threads are paused
- `agent_runs.graph_version` + `getGraph(name, version)`: a paused thread resumes on the code it started with.
- Bump the version for breaking changes: a renamed or removed node, or a changed state shape. Keep the old compiled graph registered until no thread uses it. A missing version throws loudly, which should alert someone.
- State changes should be additive, with defaults.
- **Retention**: the daily `checkpoint_retention` job deletes checkpoints of threads done more than 30 days ago (privacy + cost).

## 10. Queue choice cheat-sheet
| | graphile-worker (here) | RabbitMQ (reference) | Kafka | SQS |
|---|---|---|---|---|
| Enqueue atomic with DB change | **Yes** (SQL function) | Needs an outbox | Needs an outbox / CDC | Needs an outbox |
| Per-key ordering | Named queue | Lock or consistent hashing | Partition key | FIFO group id |
| Retry / DLQ | Built-in backoff, failed jobs kept | TTL + dead-letter exchange | DIY retry topics | Redrive policy |
| Replay | No | No | **Yes** | No |
| Best for | Postgres-centric apps up to high thousands of jobs/s | Polyglot job consumers | Event streams, audio chunks, many consumer groups | Managed AWS jobs |

## 11. Compared with durable-execution engines
| | This mock (LangGraph + harness) | Temporal | Mastra workflows |
|---|---|---|---|
| Unit of durability | Super-step checkpoint | Every activity result | Snapshot on suspend |
| Waiting for humans/events | `interrupt()` + mailbox | Signals + `await condition` | `suspend()` / `resume()` |
| Timers | `timers` table + cron | Built-in | Scheduling primitives |
| You build | Mailbox, reconciliation, projections, timers, reaper | Workers + activities | Storage adapter, triggers |

In one line: LangGraph is the agent's **brain and state**; the harness around it makes it
**durable in the real world**. That's what "built the agent harness from scratch" means.
