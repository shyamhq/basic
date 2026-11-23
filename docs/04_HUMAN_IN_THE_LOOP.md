# 04 · Human in the loop (HITL)

Principle: **autonomy proportional to risk**. Reversible, low-impact steps run automatically.
Risky, irreversible or ambiguous steps pause for a human. The pause is cheap because the
thread just sits at `interrupt()`.

## The mechanism (same for every pattern)
```
node: ev = interrupt({ type, accepts: ["human_decision", "cancel"], title, options, ... })
worker.project(): approvals row built FROM THE CHECKPOINT on every drain (interrupt_id UNIQUE)
                  -> a crash right after the pause can't lose the inbox entry (test 17)
UI: GET /api/approvals?status=pending
coordinator: POST /api/approvals/:id/decision {action, edits?, reason?, decidedBy}
gateway (1 tx): approval FOR UPDATE, must be pending; body validated against the approval's KIND;
                → decided + mailbox event human_decision {…, approvalId, interruptId}
worker: event.interruptId must equal the CURRENT interrupt id → else 'stale'
graph: node re-runs, interrupt() returns the event → exhaustive switch on action (default: throw)
```

## Decisions are validated per kind
The decision body is checked against a zod schema for the approval's kind (`DECISIONS` in `gateway.ts`):

| Kind | Allowed |
|---|---|
| `approval` | `approve` · `edit {rate > 0}` · `reject {reason}` |
| `escalation` | `assign {caregiverId exists in this agency, rate?}` · `cancel {reason?}` |
| `triage_review` | `edit {intent ∈ callout, preference_update, question, other}` |
| `review_outbound` | `approve {drafts?}` · `reject` |
| `emergency_ack` | `approve {reason?}` |

Without this, "approve" clicked on an escalation fell through to the cancel branch and cancelled
the shift, and an assign with a typo'd id crashed the thread until it was dead-lettered. Both were
reproduced in review. Now each is a 400, and the approval stays pending and actionable (test 11).
Approvals for pauses the thread has left become `stale`; a finished or failed thread's become `cancelled`.

## Six patterns in this codebase

| # | Pattern | Where | Trigger | Human can |
|---|---|---|---|---|
| 1 | **Approval gate on risky actions** | `shiftFill.ts` `check_policy` → `approval` | overtime, rate above posted, learned rules | approve / **edit** (rate) / reject (+reason) |
| 2 | **Escalation handoff** | `escalate` | waves exhausted, hard deadline, no candidates | assign someone manually / cancel the shift |
| 3 | **Low-confidence routing** | `intake.ts` `human_triage` | triage confidence < 0.6 | pick the intent → graph continues |
| 4 | **Safety-critical always-human** | `intake.ts` `emergency` | emergency keywords (rail in code) | acknowledge + notes |
| 5 | **Draft review mode** | `review_drafts` | `agencies.review_outbound = true` | approve / **edit text** / reject |
| 6 | **Failure handoff** | `worker.ts` `onThreadDead` | job's last retry failed | paged; approvals cancelled; parent told `child_done{failed}` |

Plus:
- **Approval SLA** (`escalateStaleApprovals`): an unanswered approval pages the on-call manager once
  after 10 minutes. The `escalated` flag and the page job commit in one transaction (test 13).
- **Cancel**: every pause also accepts a `cancel` event (`POST /api/threads/:id/cancel`), so a
  human can stop an agent at any point; open offers are withdrawn (test 5).

## Defense in depth: three layers keep an unapproved booking from happening
1. **Graph**: `book` is only reachable through `check_policy` (auto-approval *by policy* is explicit:
   `{ by: "policy:auto" }`) or a human decision.
2. **Tool gateway**: `scheduler.book_shift` is risk tier **high**. It doesn't trust the caller's
   "approved by" string: it checks that a **decided approval row for this thread** exists, else
   `NeedsApproval`. A bug in the graph can't bypass it.
3. **Audit**: `tool_calls.approved_by` records who approved every booking (test 6).

## Correctness details interviewers like
- **Double click / two coordinators**: `SELECT … FOR UPDATE` plus a status check gives first-wins,
  and the second gets HTTP 409 (test 12).
- **Stale decision**: a decision carries the `interruptId` it was made for. If the thread has
  moved on, the worker marks it `stale` instead of resuming the wrong interrupt (test 12).
- **Events during a pause**: a nurse's text arriving while a human decides is **buffered** in
  the mailbox, not lost and not misapplied (test 14).
- **The node re-runs on resume**: code before `interrupt()` runs twice, so keep it idempotent
  (test 8).
- **Timeouts**: humans are slow. The SLA escalation and the hard-deadline timer keep the shift
  from silently expiring.

## Learning from humans (the flywheel)
A **reject with a reason** → `memory.reflect` → `reflect()` turns it into a playbook rule
(text + optional structured `rule`) → `addProcedure`. `check_policy` reads procedures **fresh**,
so the next decision, even in the same thread, applies the rule. Test 7: "never book someone's
first shift at a facility without checking with me" becomes `{require_approval, when:
first_shift_at_facility}`, and Anita's acceptance then needs approval.

Other signals worth capturing: **edits** (approved rate vs proposed rate → calibrate the
negotiation policy), approval latency, and override rate per risk type. When humans approve
a risk type 99% of the time unedited, consider auto-approving it. That's how autonomy
grows safely over time.

## UX for the approvals inbox
- One screen with context: who, what, why it's risky, ranked alternatives, the thread timeline.
- Choices, not a blank box: `options` in the interrupt payload drive the buttons.
- Show the deadline (SLA / shift start) and sort by urgency.
- Every decision is attributed (`decided_by`) and reversible where possible.

## Mapping to framework vocabulary
- LangGraph: `interrupt()` + `Command({ resume })`; LangChain's HITL middleware offers
  approve / edit / reject decisions for tool calls.
- Mastra: workflow `suspend()` / `resume()`; tool-call approval.
- OpenAI / Claude Agent SDKs: permission callbacks before tool execution.
- Temporal: signals + `await condition(...)`.
