// TOOL GATEWAY: the only way an agent can touch the outside world.
//
// Every tool call goes through callTool(), which does five things:
//   1. VALIDATE  args against a strict zod schema (the LLM never sends free-form args)
//   2. AUTHORIZE by risk tier: read | write | high. "high" needs `approvedBy`
//                -> even if a graph has a bug, booking a shift without a human approval
//                   (or an explicit auto-approval policy) is impossible.
//   3. DEDUPE    with an idempotency key. LangGraph re-runs a node after a crash or an
//                interrupt resume, so a node may call the same tool twice - the second
//                call returns the cached result instead of texting the nurse twice.
//   4. EXECUTE
//   5. AUDIT     (args, result, who approved) - the same row as the idempotency cache.
//
// The same registry could be exposed as an MCP server so other agents (or Claude Code)
// get the same governed tools.

import { z } from "zod";
import { comms } from "./comms.js";
import { MANAGER_PHONE } from "./config.js";
import { one, q, tx } from "./db.js";

type Risk = "read" | "write" | "high";
type Ctx = { threadId: string; idempotencyKey: string; approvedBy?: string };
type ToolDef<S extends z.ZodType> = { risk: Risk; schema: S; run: (args: z.infer<S>, ctx: Ctx) => Promise<unknown> };

export class NeedsApproval extends Error {}
export class ToolConflict extends Error {}   // business conflict, e.g. shift already taken -> not retryable

const tool = <S extends z.ZodType>(t: ToolDef<S>) => t;

export const TOOLS = {
  "scheduler.get_shift": tool({
    risk: "read",
    schema: z.object({ shiftId: z.string() }).strict(),
    run: async ({ shiftId }) =>
      one(`SELECT s.*, f.name AS facility_name, f.requirements, f.notes AS facility_notes
           FROM shifts s JOIN facilities f ON f.id = s.facility_id WHERE s.id = $1`, [shiftId]),
  }),

  "scheduler.open_shift": tool({
    risk: "write",
    schema: z.object({ shiftId: z.string(), reason: z.string() }).strict(),
    run: async ({ shiftId }) => {
      await q(`UPDATE shifts SET status = 'open', caregiver_id = NULL WHERE id = $1 AND status IN ('scheduled','open')`, [shiftId]);
      return { ok: true };
    },
  }),

  "scheduler.book_shift": tool({
    risk: "high",
    schema: z.object({ shiftId: z.string(), caregiverId: z.string(), rate: z.number() }).strict(),
    run: async ({ shiftId, caregiverId, rate }) =>
      tx(async (c) => {
        const s = await one(`SELECT * FROM shifts WHERE id = $1 FOR UPDATE`, [shiftId], c);
        if (!s) throw new ToolConflict("shift not found");
        if (s.status === "filled" && s.caregiver_id !== caregiverId) throw new ToolConflict("shift already filled");
        if (s.status === "cancelled") throw new ToolConflict("shift cancelled");
        const hours = (new Date(s.ends_at).getTime() - new Date(s.starts_at).getTime()) / 3_600_000;
        await q(`UPDATE shifts SET status = 'filled', caregiver_id = $2, rate = $3 WHERE id = $1`, [shiftId, caregiverId, rate], c);
        await q(`UPDATE caregivers SET hours_this_week = hours_this_week + $2 WHERE id = $1`, [caregiverId, hours], c);
        return { ok: true, hours };
      }),
  }),

  "scheduler.cancel_shift": tool({
    risk: "high",
    schema: z.object({ shiftId: z.string(), reason: z.string() }).strict(),
    run: async ({ shiftId }) => {
      await q(`UPDATE shifts SET status = 'cancelled' WHERE id = $1`, [shiftId]);
      return { ok: true };
    },
  }),

  "comms.send_sms": tool({
    risk: "write",
    schema: z.object({ to: z.string(), body: z.string().max(480) }).strict(),
    run: async ({ to, body }, ctx) => comms.sendSms(to, body, { threadId: ctx.threadId }),
  }),

  "comms.start_call": tool({
    risk: "write",
    schema: z.object({ to: z.string(), vars: z.record(z.string(), z.string()) }).strict(),
    run: async ({ to, vars }, ctx) => comms.startCall(to, vars, { threadId: ctx.threadId, offerCode: vars.offer_code }),
  }),

  "ops.page_manager": tool({
    risk: "write",
    schema: z.object({ message: z.string() }).strict(),
    run: async ({ message }, ctx) => comms.sendSms(MANAGER_PHONE, `[PAGE] ${message}`, { threadId: ctx.threadId }),
  }),
} as const;

export type ToolName = keyof typeof TOOLS;

export async function callTool<N extends ToolName>(name: N, args: z.infer<(typeof TOOLS)[N]["schema"]>, ctx: Ctx): Promise<any> {
  const def = TOOLS[name] as ToolDef<z.ZodType>;
  const parsed = def.schema.parse(args);                                     // 1. validate
  if (def.risk === "high" && !ctx.approvedBy) throw new NeedsApproval(name); // 2. authorize

  // 3. dedupe: claim the key first. If someone already ran it, return their result.
  const claimed = await one(
    `INSERT INTO tool_calls (idempotency_key, thread_id, tool, args, approved_by) VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT (idempotency_key) DO NOTHING RETURNING idempotency_key`,
    [ctx.idempotencyKey, ctx.threadId, name, parsed, ctx.approvedBy ?? null],
  );
  if (!claimed) {
    const prev = await one(`SELECT result FROM tool_calls WHERE idempotency_key = $1`, [ctx.idempotencyKey]);
    if (prev?.result?.error) throw new ToolConflict(prev.result.error);
    if (prev?.result != null) return prev.result.value;
    // fall through: claimed earlier but never finished (crash mid-call) -> run again
  }
  try {
    const result = await def.run(parsed, ctx);                                // 4. execute
    await q(`UPDATE tool_calls SET result = $2 WHERE idempotency_key = $1`, [ctx.idempotencyKey, { value: result ?? null }]); // 5. audit
    return result;
  } catch (e) {
    await q(`UPDATE tool_calls SET result = $2 WHERE idempotency_key = $1`, [ctx.idempotencyKey, { error: String(e) }]);
    if (!(e instanceof ToolConflict)) await q(`DELETE FROM tool_calls WHERE idempotency_key = $1`, [ctx.idempotencyKey]); // allow retry
    throw e;
  }
}
