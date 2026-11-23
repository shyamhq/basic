# Taking this to an Adalat AI interview

The patterns in this repo (long-running threads, mailbox, durable timers, human sign-off,
layered memory) carry straight over to court transcription and order drafting. This doc maps
them over and prepares you for Adalat-specific questions.

---

## 1. What we know about Adalat AI (from public sources found earlier)

| Fact | Source type |
|---|---|
| Indian legal-tech **nonprofit** | press, MIT Solve |
| Core product: **AI speech-to-text for courts**, handling legal jargon in **multiple Indian languages** and dialects | press, MIT Solve |
| Also: **case management** and **document digitization** tools | press |
| Lets judges dictate proceedings instead of writing by hand where stenographers are scarce | press |
| **Kerala High Court** directed all trial courts in the state to adopt Adalat.AI for **recording depositions** | Moneylife |
| Deployment counts vary by source: **~2,500 / 3,000+ / 4,000 courtrooms across ~8 states** | press (self-reported, inconsistent) |
| Partnered with **Karya** to create legal training data in Indian languages and accents | Analytics India Mag |
| Engineering job posts list **React/Next.js, TypeScript, Go, PostgreSQL**; SRE posts mention **Kubernetes + ArgoCD** and sensitive real-time data | careers page / job boards |
| ML roles: **legal reasoning** (incl. a tax-litigation co-pilot), **NLP**, **speech**, staff ML for low-resource settings | careers page |
| Team is built by linguists, researchers, engineers and lawyers; listings say AI-native, small teams, remote-first | YourStory, listings |

Anything below marked **(assumption)** is my inference, not a published fact. Say it as a
hypothesis in the interview ("I'd guess...; is that right?").

---

## 2. Concept mapping: Vali mock → Adalat

| This repo | Adalat equivalent | Notes |
|---|---|---|
| ShiftFill thread (`src/graphs/shiftFill.ts`), one per open shift | **Hearing/deposition thread**, one per deposition or hearing session | Lives for hours (the hearing) to days (sign-off, corrections) |
| Mailbox events (`thread_inbox`: `nurse_reply`, `timer`, `human_decision`, `cancel`) | **Audio chunk ready**, **ASR segment done**, **judge edit**, **sign-off**, **court-system callback** (e.g. case-management sync) | Same per-thread serialization (named queue `thread:<id>`) + dedupe |
| Natural thread ids (`fill:<shiftId>`) | One thread per **deposition id / case + hearing date** | A retry or a second upload can't start a duplicate thread |
| `wait` interrupt | Waiting for the next audio segment, the judge's review, or an upload after reconnecting | No compute held while waiting |
| `interpret` node (LLM output checkpointed before decisions) | ASR / draft output stored per chunk before anything acts on it | A replay never re-transcribes into a different result |
| Durable timers (`timers`) | "Remind the judge to sign the deposition by end of day", "retry upload when the courtroom is back online" | Rows, not `setTimeout` |
| HITL approval (`approval`, `review_drafts`) | **Judge or stenographer review and sign-off** before a deposition or order is final | The legal record must be human-approved; the AI drafts |
| Approve / edit / reject | Accept the transcript, edit inline, or reject and re-dictate | Edits become training data and procedural memory |
| Stale-decision guard (`interruptId`) | A sign-off applies only to the version it was given for | Version every draft; sign the hash |
| Preference memory | **Judge-specific** formatting, phrasing, vocabulary, preferred citation style | Provenance matters: "judge said" > "inferred" |
| Episodic memory | **Case history across hearings**: earlier depositions, adjournments, prior orders | Retrieval by case number + semantic search |
| Semantic memory | **Statutes, legal glossary, local-language legal terms** (RAG) | Glossary also biases ASR |
| Procedural memory (learned from rejections) | **Court-specific templates and rules** learned from judge corrections | e.g. "this court numbers paragraphs in Malayalam numerals" **(assumption)** |
| Tool gateway risk tiers (`authorize` checks a decided approval row) | Read case data = read; save draft = write; **finalize / publish order = high (needs a recorded signature)** | Enforced in code; the signer is verified, not trusted from the caller |
| DB invariant (`no_overlapping_bookings` EXCLUDE constraint) | One final signed version per deposition; one judge per courtroom slot | Invariants live in the database, not in the agent |
| graphile-worker (jobs enqueued in the same transaction) | Per-deposition jobs: assemble, generate PDF, case-management sync | Fine while consumers are TypeScript + Postgres |
| RabbitMQ + outbox reference (`src/reference/rabbitmq.ts`) | When **Go services** consume the jobs (Adalat's stack has Go) | The outbox keeps it consistent with Postgres; Kafka fits raw audio streams |
| Reaper + checkpoint-as-truth projections | Resume transcription after a crash or a courtroom power/network loss; sign-off inbox rebuilt from the checkpoint | Idempotent per chunk |
| Per-kind decision validation (`DECISIONS`) | A "sign" action is only valid on a sign-off request, for the current version | 400 instead of a wrong action reaching the workflow |
| `review_outbound` mode | "Nothing leaves the courtroom unsigned" mode | Default on for legal records |

Cross-cutting **(assumptions)** to raise:
- **Poor connectivity in district courts:** record and transcribe locally (edge) or buffer
  audio, sync with an idempotent upload protocol (chunk ids, resumable uploads).
- **Data residency and privacy:** court data stays in India; witness PII is redacted in logs;
  access is controlled per court and case; audit trails.
- **Multilingual:** language and dialect detection per segment, code-switching
  (English + Malayalam/Hindi...), a transliteration layer, and glossaries per language.

---

## 3. Sketch: long-running deposition transcription + order drafting, judge in the loop

```
 Courtroom (edge device / browser)                      Cloud (India region)
 ┌──────────────────────────────┐   resumable chunk upload   ┌──────────────────────────────┐
 │ mic → VAD → 10–30s chunks    │ ─────────────────────────▶ │ Gateway: auth (court, judge),│
 │ local buffer (offline-first) │   (chunk_id = idempotency) │ dedupe, 1 tx: inbox + job    │
 │ optional on-device ASR draft │ ◀── live draft text ────── └──────────────┬───────────────┘
 └──────────────────────────────┘                                           │
                       pg job queue (or outbox relay → RabbitMQ/Kafka if Go workers consume)
                                                                           │
            ┌──────────────────────────────────────────────────────────────┴───────────┐
            ▼                                                                          ▼
   ASR workers (GPU): language ID → ASR                              Deposition thread (graph, Postgres checkpoints)
   with legal glossary biasing → segment         ── segment_done ─▶   collect segments → assemble draft
   (Kafka/stream if very high volume)                                  → interrupt: wait(judge_review | segment | timer)
                                                                       → judge edits (HITL) → apply, keep diff
                                                                       → sign-off gate (HIGH risk tool: finalize)
                                                                       → generate PDF + hash → case-management sync
                                                                       → finish: episodes (case history), corrections
                                                                         → procedural memory / training data
   Memory (pgvector):  judge prefs │ case episodes │ statutes/glossary RAG │ court templates
   Timers: sign-off reminder, SLA to court admin, retry sync when offline
```

Order drafting is a second thread type (e.g. `order_draft`). It is spawned from the
deposition thread or by the judge:
1. Retrieve the case history (episodic) and the relevant statutes (semantic).
2. Draft using the court template (procedural).
3. The judge reviews and edits (approve/edit/reject).
4. Finalize only with a signature.

---

## 4. Fifteen Adalat-specific questions with answers

**1. How would you make transcription of a 3-hour deposition robust to crashes and network drops?**
- Split audio into chunks with stable ids on the device.
- Upload resumably and idempotently (the gateway dedupes on `chunk_id`).
- Each chunk is a mailbox event for the deposition thread; one job queue per thread keeps them in order.
- ASR results are keyed by chunk, so re-processing is harmless.
- The thread checkpoints after each segment; a reaper resumes stalled threads.
- Treat the checkpoint as the source of truth: the sign-off inbox and status are projections
  rebuilt on every pass, so a crash between two writes can't leave a judge without a sign-off request.

**2. Real-time draft vs final transcript?**
Two paths:
- A fast streaming draft for the courtroom screen (latency first, maybe on-device).
- A batch "final" pass with a bigger model, glossary biasing and diarization, which feeds the
  sign-off draft.

The graph only ever puts the final pass in front of the judge for signature.

**3. Where must a human be in the loop?**
Anything that becomes part of the legal record or an order: deposition sign-off, order
finalization, corrections to names, numbers and sections. Make "finalize" a high-risk tool
that requires a recorded signer, like `scheduler.book_shift` requires a decided approval row
(`authorize` in `src/tools.ts` checks the database, not a caller-supplied string).

**4. How do you stop a sign-off from applying to the wrong version?**
Version every draft and hash its content. The approval stores the version and interrupt id.
Reject a sign-off whose version isn't the current one, like the `interruptId` check in
`src/worker.ts`, and return a conflict to the UI. Validate the action per request kind at the
gateway (`DECISIONS` in `src/gateway.ts`), so a wrong button returns 400 instead of acting.

**5. How do judges' corrections improve the system?**
- Store edit diffs per segment.
- Recurring corrections become judge preferences (vocabulary, style) and court procedures
  (templates). This is reflection, as in the `memory_reflect` job.
- Corrections also feed ASR fine-tuning and evaluation data, curated and consented
  **(assumption about the process)**.

**6. How would you handle code-switching between English and a regional language?**
- Segment-level language ID.
- An ASR model trained on code-switched legal speech (the Karya partnership is for exactly
  this kind of data).
- A domain glossary for biasing; post-processing for legal terms, section numbers and citations.
- Optional transliteration so the output follows the court's language rules.

**7. What memory would you keep per judge, and how do you avoid bias or leakage?**
- Formatting, phrasing, citation style and vocabulary only. No inferences about how a judge
  decides cases.
- Scope memory by judge and court namespace, and never share it across tenants.
- Record provenance, let judges view and edit their preferences, and forget on request.

**8. How do you evaluate transcription quality in legal settings?**
- WER/CER per language and dialect.
- Weighted errors on critical tokens: names, dates, amounts, section numbers.
- The judge edit rate per page; time-to-sign-off.
- A held-out set per court and language, evaluated in CI.

**9. Order drafting: how do you prevent hallucinated law?**
- Retrieve before generating: statutes and precedents from a curated corpus.
- Use structured outputs with citation fields that must resolve to retrieved ids.
- Validate in code that every cited section exists.
- Require judge sign-off; never auto-publish.

**10. What if the courtroom is offline for hours?**
- Record locally with integrity hashes and queue uploads.
- Durable timers retry the sync.
- The deposition thread waits at an interrupt, with no compute held.
- The admin dashboard shows "pending sync" from `agent_runs`-style lifecycle rows.

**11. Go or TypeScript for these services?**
TypeScript for product APIs, the UI and the orchestration glue (shared types with Next.js).
Go for high-throughput ingest (audio upload, chunk fan-out) and services needing tight
latency and concurrency. They meet at Postgres and the queue. In the mock the queue is a
Postgres one (graphile-worker), which suits TypeScript workers. Once Go services consume jobs
too, I'd switch to the pattern in `src/reference/rabbitmq.ts`: a transactional outbox table
plus a relay to RabbitMQ (or Kafka), with a JSON contract both languages consume.

**12. RabbitMQ, Kafka or a Postgres queue for audio?**
- Raw audio chunks and partial ASR at high volume, with replay: a log (Kafka/Redpanda) or
  object storage plus events.
- Per-deposition jobs (assemble, generate PDF, sync): a job queue. A Postgres queue
  (graphile-worker) gets transactional enqueue for free; RabbitMQ needs an outbox for
  consistency with Postgres, and is the better fit when Go workers consume the jobs.

**13. How do you deploy safely with Kubernetes + ArgoCD when threads are paused for days?**
- Keep graph changes backward compatible, or version graphs and run old and new workers side by side
  (the mock records `graph_version` per run and looks graphs up with `getGraph(name, version)`).
- Drain old versions.
- Migrations for checkpoint state when needed.
- GitOps rollouts with canaries on non-critical courts first **(assumption)**.

**14. Privacy and security of court data?**
- Encryption in transit and at rest; an India region.
- RBAC per court and case; immutable audit logs of who viewed, edited and signed.
- PII redaction in logs and LLM traces.
- Model providers with no training on customer data, or self-hosted models.
- Retention policies set with the courts **(assumption on the specifics)**.

**15. How would you measure impact?**
- Time from end of hearing to signed deposition.
- Judge edit rate; pages signed per day.
- Adjournments caused by missing records **(assumption: a metric they may care about)**.
- Adoption per court; uptime during court hours.

---

## 5. How to pitch your experience

| Your experience | Why it matters to Adalat |
|---|---|
| **Vali Health (founding engineer):** long-running agents that wait on humans, a from-scratch harness with state that survives restarts, strict tool schemas, human approval before anything reached a real person, Twilio voice/SMS, RAG, fine-tuning | Depositions and orders are long-running, human-signed workflows. You've built exactly that shape, from zero, with real users. |
| **Carrier (IoT cloud):** event-driven TypeScript/Node microservices with gRPC; telemetry from AWS IoT Core into **Kafka**; analytics through Kinesis Firehose, S3 and Athena; a self-hosted WebSocket service; OTA firmware rollouts; GitOps with Helm + **Argo CD** | Courtrooms are edge devices with flaky networks streaming audio. Ingest pipelines, real-time sync and fleet rollouts are the same problem. Argo CD matches their SRE stack. |
| **Cruise (via Toptal):** Go/gRPC ingress/egress services on Cloud Spanner; fleet dashboards over large datasets; load-testing framework; resiliency with backoff | Their Go services; dashboards for thousands of courtrooms; reliability engineering for court hours. |
| **Kumo (project):** a TypeScript control plane, agent runtime over MCP, repo set up for AI-assisted development (AGENTS.md, ADRs, skills) | They describe themselves as AI-native; you work that way and can set it up for a team. |

**A 30-second pitch to adapt:** "I build systems where AI does the heavy lifting but humans
own the decision. At Vali that meant agents that ran for hours, waited for nurses and
coordinators, and never acted without an approval gate. At Carrier it meant streaming device
data through Kafka into reliable pipelines and rolling out to fleets with Argo CD. Adalat's
problem looks like both: audio from thousands of courtrooms, long-running transcription and
drafting, and a judge who must sign off. That's the intersection I've worked in."

**Smart questions to ask them:**
1. Where does ASR run today: on-device in the courtroom, in the cloud, or hybrid? How do you handle offline courtrooms?
2. What is the sign-off workflow for a deposition, and what does a judge edit most often?
3. How do judge corrections flow back into model training and evaluation?
4. How are you thinking about order drafting and legal reasoning (e.g. the tax-litigation co-pilot)? Where is the human gate?
5. What does an incident during court hours look like, and what's your on-call model?
6. How do Go and TypeScript split across services today?
7. Data residency and privacy: what constraints do the courts set, and who owns retention?
8. How do you measure success per court: time to signed record, edit rate, adoption?
9. What's the biggest reliability problem this role would own in the first 90 days?
