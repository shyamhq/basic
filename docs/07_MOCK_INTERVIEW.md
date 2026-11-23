# 07 · Mock interview: a principal engineer grills the design

Answers you can say out loud. Each one ends with where it lives in the code.

---

### Q1. "Walk me through how agent orchestration works in your system."
Two layers.

**Inside one agent, LangGraph is the state machine.** The ShiftFill agent is a graph:
- load the shift, filter candidates in SQL;
- fan out with `Send` to score each nurse in parallel, then fan in to rank;
- send a wave of offers, then **pause** with `interrupt()` until a reply or a timer arrives;
- interpret the reply with the LLM in its own node, so that output is checkpointed;
- decide with `Command(goto=…)`: book, approve, retry the next wave, or escalate.

Routing is explicit code, not "let the model pick the next tool". The model does judgment
(classify a text, extract preferences); code does constraints and control flow.

**Across agents, a supervisor spawns child threads.** When a nurse calls out sick:
- The Intake agent triages the text and finds her shifts.
- It starts one ShiftFill **thread** per shift, with id `fill:<shiftId>`, all in one transaction.
- It then waits on `child_done` events: a fan-in barrier with a deadline timer, so it can't hang.

I use separate threads rather than subgraphs because each shift needs independent durability: its
own queue, retries, timeline and approvals. One failed shift shouldn't block its sibling. The natural
id also makes a second text or a facility post unable to start a duplicate.

*Code: `src/graphs/intake.ts`, `src/graphs/shiftFill.ts`, `src/mailbox.ts`.*

### Q2. "How is a long-running agent actually long-running? Where does it live while it waits?"
- **It doesn't live in a process.** State is checkpointed to Postgres after every step (`durability: "sync"`).
- **Pausing**: `interrupt()` saves the checkpoint and ends the run, so the worker is free.
- **Waking up**: everything that can wake a thread goes into its **mailbox** table, together with a
  kick job, in one transaction. That covers a nurse's text, a Retell call result, a timer, a human
  decision and a child finishing.
- **Draining**: the worker drains the mailbox in order and resumes the graph with
  `Command({ resume: event })`. Only events the current pause `accepts` are used; others stay
  buffered. For example, a nurse texting while a coordinator decides waits until the approval is done.
- **Time** is rows, not `setTimeout`. Wave timeouts and deadlines are timer rows, and a cron tick every
  minute delivers due ones to mailboxes.
- **Deploys**: each run records its graph version, so a thread paused for six hours resumes on the code it started with.

*Code: `src/worker.ts`, `src/timers.ts`, `src/graphs/versions.ts`.*

### Q3. "What if the process dies halfway?"
- **Source of truth**: the checkpoint. Run status and the approvals inbox are *projections*,
  recomputed from it at the start of every drain, so a crash between any two writes repairs itself.
- **Mailbox events** move pending → inflight (tagged with the interrupt they resumed) → consumed.
  An event is consumed only once the checkpoint has moved past that interrupt.
- **Inside a node**: LangGraph re-runs the whole node, so every side effect is idempotent and
  every DB write a later branch depends on is replay-safe. Offers remember the event that last
  moved them.
- **The booking itself** is exactly-once: claim, update and audit row happen in one transaction.
- **A worker that vanishes**: a reaper sees the stale heartbeat, force-unlocks the job and re-kicks
  the thread. A lease version (fencing token) stops a zombie worker's control-plane writes.

*Tests 16–19 and 23 crash at each of these points.*

### Q4. "How do retries work?"
A ladder, from smallest scope to largest:
1. **LLM / HTTP client**: the Anthropic SDK retries 429/5xx twice. Retell calls have a 10 s timeout.
2. **Node retry policy**: `retryPolicy: { maxAttempts: 3 }` on nodes that call flaky things
   (`load_shift`, `send_wave`, `interpret`, `classify`). Retried in-process, from the last checkpoint.
3. **Job retry**: if the whole drain throws, graphile-worker retries the job with exponential
   backoff, up to 5 attempts. Because jobs for a thread live in one named queue, a retry can't race the next event.
4. **Dead letter**: on the last attempt `onThreadDead`, in one transaction:
   - marks the run failed and cancels its approvals;
   - enqueues a page to a human;
   - tells the parent thread "child failed".
5. **Not retried**: business conflicts (`ToolConflict`, e.g. "shift already filled", "nurse
   double-booked"). The graph routes them: an overlap moves on to the next nurse.

Retries are safe only because of idempotency (Q5). I'd never stack them without it.

### Q5. "What idempotency keys do you use?"
At every boundary where at-least-once delivery can duplicate something:

| Boundary | Key |
|---|---|
| Inbound SMS webhook | `sms:<providerMessageId>` (`inbound_requests`) |
| Retell post-call webhook | `retell:<callId>:analyzed` |
| Meaning-level dedupe | `offer:<code>:accept` (or `:decline`, `:counter:<rate>`). The mid-call `accept_shift` function and the post-call webhook are the *same* acceptance |
| Thread start | `start:<threadId>`; thread ids are natural keys (`fill:<shiftId>`) |
| Human decision | `decision:<approvalId>`, plus `interrupt_id` unique on approvals |
| Timer firing | `timer:<timerId>`; timer ids are deterministic (`<thread>:wave:<n>`, `<thread>:deadline`) |
| Fan-in / cancel | `child_done:<childThreadId>`, `cancel:<threadId>` |
| Job coalescing | graphile `job_key = kick:<threadId>` |
| Tool calls | `<threadId>:<purpose>`, e.g. `fill:sh_x:book`, `fill:sh_x:sms:offer:cg_james`, `fill:sh_x:sms:confirm:cg_james` |
| Memory jobs | `extract:<eventId>`, `episode:<thread>:<caregiver>`, `reflect:<approvalId>` |
| Offer codes | `hash(threadId, caregiverId)`: deterministic, so a re-run sends the same code |

Tool keys are **deterministic from the thread and the intent, never random**. A replayed node
produces the same key and gets the cached result back. For external calls I say it plainly: that's
at-least-once. Our transaction can't include Twilio, so you use the provider's idempotency key when it has one.

### Q6. "How do you handle memory?"
Five kinds, each with a clear owner:
- **Working memory**: the thread's checkpointed state. It holds ids and scores, not phone numbers.
- **Profile**: the nurse's structured record in `caregivers` (licenses, rates, distance, preferred
  shift times, contact hours). Staff own it.
- **Preferences**: learned from texts and call transcripts ("no nights anymore", "don't call
  before 9"). Each has a source; staff beat what the nurse said, which beats an inference.
- **Episodic**: what happened ("declined Sunrise twice"), searchable by meaning with pgvector plus exact filters.
- **Semantic**: facility facts.
- **Procedural**: playbook rules learned when a coordinator rejects with a reason. They become policy:
  "ask before a nurse's first shift at a facility" turns into a real approval gate.

**Write path**: background jobs, so the reply moves the shift forward immediately. They run in a
queue per nurse, so two writes for the same nurse never race.

**Read path**:
- Before ranking: hard preferences exclude a nurse, with the reason logged; soft ones adjust the score.
- Before each new wave: hard preferences are re-checked.
- Before every call: preferences go into the Retell agent's prompt variables.

**Hourly consolidation**:
- Repeated episodes become soft preferences; an inference never overrides what the nurse stated.
- Episodes older than 90 days are forgotten.

**Risks I'd raise myself**:
- Prompt injection through stored text. I extract into typed schemas and let structured rules, not free text, drive policy.
- PII: retention jobs on checkpoints and episodes.

*Code: `src/memory.ts`; tests 25–27.*

### Q7. "Do you have centralized logs? How would I debug one nurse's shift at 2 a.m.?"
What exists today, honestly:
- **Structured JSON logs** (pino) with `threadId` and event kind on every invoke.
- **A per-thread story you can query, no log search needed**:
  - the agent's own timeline in its checkpoint (`GET /api/threads/:id`) and the full checkpoint history (`/history`);
  - the mailbox (every input, with status and notes);
  - `tool_calls` (every side effect, with args, result and who approved it);
  - `approvals`, `comms_log` (every message in and out), and graphile's job table (`last_error`, attempts).

So "what happened to shift X" is a few `WHERE thread_id = …` queries. The console shows the timeline.

What I'd add for production:
- Ship stdout to a central store (Loki, Datadog or ELK) via the platform's log collector.
- **OpenTelemetry tracing**: put a `traceparent` on each mailbox row and job payload, so one trace spans
  webhook → job → graph node → LLM call → tool call. Plus LLM traces in Langfuse or LangSmith.
- **Metrics and alerts**: time-to-fill, mailbox depth and oldest pending age, approval latency,
  dead letters, LLM fallback rate, and "thread paused at an unknown graph version".

### Q8. "And logins / auth?"
- **Webhooks**: verify provider signatures (Retell's `x-retell-signature` via their SDK, Twilio's
  signature header); dedupe by message id. The mock stubs signature checks; I'd call that out.
- **Tenancy**: the dialed number identifies the agency. Every app table carries `agency_id`. The
  next step is Postgres row-level security with `SET LOCAL app.agency_id` per request.
- **Humans**: every decision records `decidedBy`. The tool gateway refuses a high-risk tool unless a
  *decided approval row* exists for that thread; it doesn't trust a string from the caller.
  Production adds SSO for coordinators and role checks: who may approve overtime, who may cancel.
- **Secrets**: API keys come from env / a secret manager and never go into prompts or checkpoints.

### Q9. "What breaks first at 10×?"
- **The Postgres queue**: graphile handles thousands of jobs/s, but named queues add per-queue locking
  cost. I'd measure, and partition by tenant, or move kicks to Kafka with partition key = threadId.
  The mailbox and checkpoint model stays the same.
- **Checkpoint volume**: one per super-step. Use retention, and durability `"async"` for low-risk graphs.
- **The LLM**: cost and latency on the reply path. Use structured outputs at low effort, cache the
  playbook prompt, and route easy messages to deterministic rules first.
- **Fencing**: LangGraph's saver has no compare-and-set. If zombie writers became a real problem,
  I'd add a fenced checkpointer, or move the harness onto Temporal or Restate.

### Q10. "What did a review of your design find?"
Nine real bugs. Almost all were crash windows between two separate writes, or duplicates arriving
through different channels. The fixes became principles:
- **The checkpoint is the source of truth; everything else is a projection.**
- **Invariants live in the database** (an exclusion constraint against overlapping bookings).
- **Dedupe by meaning, not by transport.**
- **Checkpoint LLM output before acting on it.**
- **Validate a human decision against the kind of pause it answers.**

Each one has a regression test that crashes at exactly that point.
