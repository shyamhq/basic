# 04 · Human in the loop (HITL)

Principle: **autonomy proportional to risk**. Reversible, low-impact steps run automatically.
Risky, irreversible or ambiguous steps pause for a human. The pause is cheap because the
thread just sits at `interrupt()`.

## The mechanism (same for every pattern)
```
node: d = interrupt({ type, accepts: ["human_decision"], title, options, ... })
worker.recordWaitState: INSERT approvals (interrupt_id UNIQUE) → status waiting_human
UI: GET /api/approvals?status=pending
coordinator: POST /api/approvals/:id/decision {action, edits?, reason?, decidedBy}
gateway (1 tx): approval FOR UPDATE, must be pending → decided; mailbox event human_decision {…, interruptId}
worker: event.interruptId must equal the CURRENT interrupt id → else 'stale'
graph: node re-runs, interrupt() returns the decision → approve | edit | reject | assign | cancel
```

## Six patterns in this codebase

| # | Pattern | Where | Trigger | Human can |
|---|---|---|---|---|
| 1 | **Approval gate on risky actions** | `shiftFill.ts` `check_policy` → `approval` | overtime, rate above posted, learned rules | approve / **edit** (rate) / reject (+reason) |
| 2 | **Escalation handoff** | `escalate` | waves exhausted, hard deadline, no candidates | assign someone manually / cancel the shift |
| 3 | **Low-confidence routing** | `intake.ts` `human_triage` | triage confidence < 0.6 | pick the intent → graph continues |
| 4 | **Safety-critical always-human** | `intake.ts` `emergency` | emergency keywords (rail in code) | acknowledge + notes |
| 5 | **Draft review mode** | `review_drafts` | `agencies.review_outbound = true` | approve / **edit text** / reject |
| 6 | **Failure handoff** | `worker.ts` `onDeadJob` | job in the DLQ | gets paged, fixes, re-kicks |

Plus: **approval SLA** (`escalateStaleApprovals`): an unanswered approval pages the on-call
manager once after 10 minutes.

## Defense in depth: three layers keep an unapproved booking from happening
1. Graph: `book` is only reachable through `check_policy` (auto-approval *by policy* is explicit:
   `approvedBy = "policy:auto"`) or a human decision.
2. Tool gateway: `scheduler.book_shift` is risk tier **high** and throws `NeedsApproval` without
   `approvedBy`, so a bug in the graph can't bypass it.
3. Audit: `tool_calls.approved_by` records who approved every booking (test 4).

## Correctness details interviewers like
- **Double click / two coordinators**: `SELECT … FOR UPDATE` plus a status check gives first-wins,
  and the second gets HTTP 409 (test 9).
- **Stale decision**: a decision carries the `interruptId` it was made for. If the thread has
  moved on (e.g. a newer approval round), the worker marks it `stale` instead of resuming the
  wrong interrupt (test 9).
- **Events during a pause**: a nurse's text arriving while a human decides is **buffered** in
  the mailbox, not lost and not misapplied (test 11).
- **The node re-runs on resume**: code before `interrupt()` runs twice, so keep it idempotent
  (test 6).
- **Timeouts**: humans are slow. The SLA escalation and the hard-deadline timer keep the shift
  from silently expiring.

## Learning from humans (the flywheel)
A **reject with a reason** → `memory.reflect` → `reflect()` turns it into a playbook rule
(text + optional structured `rule`) → `addProcedure`. `check_policy` reads procedures **fresh**,
so the next decision, even in the same thread, applies the rule. Test 5: "never book someone's
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
