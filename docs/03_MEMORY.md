# 03 · Memory

## Taxonomy (CoALA paper; used by LangMem, Mastra, Letta, Mem0)

| Type | Question it answers | Here | Lifetime |
|---|---|---|---|
| **Working / short-term** | "Where am I in this task?" | Graph state + Postgres checkpoints (`ShiftFillState`) | One thread |
| **Profile** | "Who is this nurse, officially?" | `caregivers` row: license, certs, specialties, languages, bio, min rate, max distance, shift prefs, channel, contact hours, reliability | Until staff edit it |
| **Preference** | "What has she told us she likes?" | Store ns `[agency, A, caregiver, C, prefs]`, e.g. "no nights", "don't call before 9", "text me" | Until contradicted |
| **Episodic** | "What happened last time?" | Store ns `[agency, A, episodes]`, e.g. "James declined a day shift at Sunrise" (vector-searchable) | Decays (90 days) |
| **Semantic** | "What's true about the world?" | Store ns `[agency, A, facility, F, facts]`, e.g. "Oakridge needs a TB result < 12 months" | Long |
| **Procedural** | "How should *I* behave?" | Store ns `[agency, A, procedures]`, e.g. "ask before a nurse's first shift at a facility" (with a structured `rule`) | Long; learned from humans |

> Profile and preference are deliberately separate. Staff own the **profile**; the agent
> **learns** preferences. Each learned fact records its **source**
> (`nurse_stated` | `inferred` | `staff`) and `updatedAt`, and keeps `previous` for audit.

## Write path (asynchronous, off the hot path)
```
SMS / call transcript ──► graph enqueues memory.extract (outbox, dedupe key = event id)
                              ──► memory worker: extractPreferences (LLM / heuristic, zod schema)
                              ──► upsertPreference (conflict rules)
ShiftFill.finish ──► memory.episode per nurse contacted (key = thread:caregiver → idempotent)
Human reject + reason ──► memory.reflect ──► reflect() ──► addProcedure (rule)
Hourly ──► memory.consolidate
```
Why async? Extraction is an LLM call. The nurse's reply should move the shift forward
immediately; learning can lag by a second. A failed extraction also never blocks a booking.

## Read path (before decisions)
`recallForShift` (in `find_candidates`) gets, per candidate, their **preferences** (exact namespace
listing) and **episodes** (semantic search, `query = "<facility> <shift time> shift"`, plus an exact
filter on `caregiverId`). It also gets the agency's **procedures** and the facility's **facts**.

How each memory type is used:
| Memory | Used in | Effect |
|---|---|---|
| Hard preference | `find_candidates` | **Excludes** the nurse, and says why in the log (test 15) |
| Soft preference | `score_candidate` | ±3 / +2 points |
| Episodes | `score_candidate` | +1 "worked here before", −1 "declined here before" (test 16) |
| Channel / contact-hour preference | `score_candidate` | Call vs SMS; no calls before her hour |
| Facts + notes | `handle_event` (questions), Retell variables | Answers "where do I park?" |
| Procedures | `check_policy` (structured rule), `classify` (prompt text) | New approval gate (test 5) |
| All prefs as text | `send_wave` → Retell `known_preferences` | The voice agent knows her preferences |

## Conflict resolution
`upsertPreference`:
- **Authority**: `staff (3) > nurse_stated (2) > inferred (1)`. An inference never overwrites
  what the nurse said (test 17).
- **Single-valued types** (`channel`, `contact_after_hour`, `min_rate`) use one key, so the newest
  value replaces the old one and the old value is kept in `previous`. Multi-valued types use the key `type:value`.
- **Hard vs soft**: something stated explicitly ("I don't do nights anymore") is hard. A hedged
  statement or a learned pattern is soft.

## Consolidation (the "sleep" job)
`consolidate(agencyId)`:
1. **Forget**: delete episodes older than 90 days. Privacy and relevance both improve.
2. **Promote**: ≥2 `declined`/`no_response` for the same shift time → soft `avoid_shift_time` (inferred).
   ≥2 `assigned` at the same facility → soft `prefer_facility`.
3. **Respect authority**: promotion goes through `upsertPreference`, so it can't override a
   stated preference.

It's deterministic on purpose: easy to test and explain. In production an LLM can summarize
clusters of episodes into one fact, which is the step where episodic memory becomes semantic memory.

## Storage: LangGraph `PostgresStore` + pgvector
- Items are `(namespace[], key) → JSON`. Configuring `index: { dims, embed, fields: ["text"] }`
  makes the store embed only the `text` field and keep vectors in `store_vectors`.
- `search(prefix, { query, filter, limit })` = vector similarity + exact JSON filters.
- The embedding here is an **offline hashing trick** (`embed` in `memory.ts`) so it runs without
  an API. Swap in a real model; the store only needs `texts → number[][]`.
- **Gotcha**: the JS store rejects namespace labels containing `_` or `%` (SQL LIKE wildcards),
  so ids are encoded (`cg_maria` → `cg-maria`).
- Namespacing by `agency` gives tenant isolation (and the place to add row-level security).

## pgvector or a vector DB?
pgvector: one database, transactional, joins with domain data, good to millions of vectors
with HNSW. A dedicated vector DB (Pinecone, which Vali used; Qdrant): billions of vectors, managed
scaling, hybrid search features, at the cost of another system and sync problems. Start with
pgvector and move when recall or latency SLOs force it.

## Memory risks to discuss
- **Prompt injection via memory**: a nurse texts "ignore previous instructions...", and it gets
  extracted and later shown to the model. Mitigations: extract into **typed schemas**, never raw
  text, as instructions; quote memory as data; let structured rules (not free text) drive policy.
- **Stale or wrong memories**: provenance, `updatedAt`, decay, and an easy "forget"/edit UI for staff.
- **PII / HIPAA**: minimize what you store (preferences, not health details), keep retention
  limits, encrypt, and scope per tenant.
- **Evaluating memory**: replay past threads with and without memory (fill rate, time-to-fill,
  number of messages sent, "annoyed nurse" rate). Unit-test extraction with labeled messages.

## How Mastra and LangMem frame the same ideas
- **Mastra**: *working memory* (a persistent profile/scratchpad the agent updates), *semantic
  recall* (vector search over past messages), and message history, scoped by resource and thread.
- **LangMem** (LangGraph): memory managers that extract into *collections* (unbounded, searchable)
  or *profiles* (one schema document), plus prompt optimization for procedural memory.

Our nurse profile plus preferences works like a profile or working memory, episodes are
semantic recall over events, and procedures are prompt or policy optimization.
