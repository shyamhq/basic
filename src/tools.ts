// TOOL GATEWAY: the only way an agent touches the outside world.
//
// callTool() does five things:
//   1. VALIDATE   args with a strict zod schema (the model never sends free-form args)
//   2. AUTHORIZE  by risk tier. "high" tools need an approval: either "policy:auto" or a DECIDED
//                 approval row for this thread (verified here, not trusted from the caller).
//   3. DEDUPE     with a deterministic idempotency key (LangGraph re-runs nodes on resume/crash).
//   4. EXECUTE
//   5. AUDIT      args, result and who approved - the same row is the idempotency cache.
//
// Two kinds of tool, two guarantees:
//   internal (our DB, e.g. book_shift): claim + business change + result in ONE transaction
//            -> exactly-once effect. A crash anywhere rolls the whole thing back.
//   external (SMS, phone call): we can't put Twilio/Retell in our transaction
//            -> at-least-once. A crash after the provider accepted the message but before we
//               recorded it can send it twice. Mitigate with provider idempotency keys if offered.

import { and, eq, sql } from "drizzle-orm";
import { z } from "zod";
import { comms } from "./comms.js";
import { MANAGER_PHONE } from "./config.js";
import { db, type Tx } from "./db/index.js";
import { approvals, caregivers, facilities, shifts, toolCalls } from "./db/schema.js";
import { maybeCrash } from "./faults.js";

type Risk = "read" | "write" | "high";
export type ToolCtx = {
  agencyId: string; threadId: string; idempotencyKey: string;
  approval?: { by: string; approvalId?: number };          // required for "high" tools
};
type Def<S extends z.ZodType> = {
  risk: Risk; kind: "internal" | "external"; schema: S;
  run: (args: z.infer<S>, ctx: ToolCtx, tx: Tx) => Promise<unknown>;
};
const tool = <S extends z.ZodType>(d: Def<S>) => d;

export class NeedsApproval extends Error {}
export class ToolConflict extends Error {}   // business conflict (shift taken, overlap) -> NOT retryable

export const TOOLS = {
  "scheduler.get_shift": tool({
    risk: "read", kind: "internal",
    schema: z.object({ shiftId: z.string() }).strict(),
    run: async ({ shiftId }, _c, tx) => {
      const [row] = await tx.select({ shift: shifts, facility: facilities }).from(shifts)
        .innerJoin(facilities, eq(facilities.id, shifts.facilityId)).where(eq(shifts.id, shiftId));
      return row ?? null;
    },
  }),

  "scheduler.open_shift": tool({
    risk: "write", kind: "internal",
    schema: z.object({ shiftId: z.string(), reason: z.string() }).strict(),
    run: async ({ shiftId }, _c, tx) => {
      await tx.update(shifts).set({ status: "open", caregiverId: null })
        .where(and(eq(shifts.id, shiftId), sql`${shifts.status} in ('scheduled','open')`));
      return { ok: true };
    },
  }),

  "scheduler.book_shift": tool({
    risk: "high", kind: "internal",
    schema: z.object({ shiftId: z.string(), caregiverId: z.string(), rate: z.number() }).strict(),
    run: async ({ shiftId, caregiverId, rate }, _c, tx) => {
      const [s] = await tx.select().from(shifts).where(eq(shifts.id, shiftId)).for("update");
      if (!s) throw new ToolConflict("shift not found");
      if (s.status === "filled" && s.caregiverId === caregiverId) return { ok: true, alreadyBooked: true };   // idempotent
      if (s.status === "filled") throw new ToolConflict("shift already filled");
      if (s.status === "cancelled") throw new ToolConflict("shift cancelled");
      const hours = (s.endsAt.getTime() - s.startsAt.getTime()) / 3_600_000;
      try {
        await tx.update(shifts).set({ status: "filled", caregiverId, rate }).where(eq(shifts.id, shiftId));
      } catch (e: any) {
        // 23P01 = exclusion_violation: the DB invariant "no overlapping bookings" said no.
        if (e?.cause?.code === "23P01" || e?.code === "23P01") throw new ToolConflict("caregiver already booked on an overlapping shift");
        throw e;
      }
      await tx.update(caregivers).set({ hoursThisWeek: sql`${caregivers.hoursThisWeek} + ${hours}` }).where(eq(caregivers.id, caregiverId));
      return { ok: true, hours };
    },
  }),

  "scheduler.cancel_shift": tool({
    risk: "high", kind: "internal",
    schema: z.object({ shiftId: z.string(), reason: z.string() }).strict(),
    run: async ({ shiftId }, _c, tx) => {
      await tx.update(shifts).set({ status: "cancelled" }).where(eq(shifts.id, shiftId));
      return { ok: true };
    },
  }),

  "comms.send_sms": tool({
    risk: "write", kind: "external",
    schema: z.object({ to: z.string(), body: z.string().max(480) }).strict(),
    run: async ({ to, body }, c) => comms.sendSms(to, body, { agencyId: c.agencyId, threadId: c.threadId }),
  }),

  "comms.start_call": tool({
    risk: "write", kind: "external",
    schema: z.object({ to: z.string(), vars: z.record(z.string(), z.string()) }).strict(),
    run: async ({ to, vars }, c) => comms.startCall(to, vars, { agencyId: c.agencyId, threadId: c.threadId, offerCode: vars.offer_code }),
  }),

  "ops.page_manager": tool({
    risk: "write", kind: "external",
    schema: z.object({ message: z.string() }).strict(),
    run: async ({ message }, c) => comms.sendSms(MANAGER_PHONE, `[PAGE] ${message}`, { agencyId: c.agencyId, threadId: c.threadId }),
  }),
} as const;

export type ToolName = keyof typeof TOOLS;

export async function callTool<N extends ToolName>(name: N, args: z.infer<(typeof TOOLS)[N]["schema"]>, ctx: ToolCtx): Promise<any> {
  const def = TOOLS[name] as Def<z.ZodType>;
  const parsed = def.schema.parse(args);                                      // 1. validate
  if (def.risk === "high") await authorize(name, ctx);                        // 2. authorize

  const claim = (tx: Tx) => tx.insert(toolCalls)                             // 3. dedupe (claim the key)
    .values({ idempotencyKey: ctx.idempotencyKey, agencyId: ctx.agencyId, threadId: ctx.threadId, tool: name, args: parsed, approvedBy: ctx.approval?.by ?? null })
    .onConflictDoNothing().returning({ k: toolCalls.idempotencyKey });
  const previous = async (tx: Tx) => (await tx.select().from(toolCalls).where(eq(toolCalls.idempotencyKey, ctx.idempotencyKey)))[0];

  if (def.kind === "internal") {
    // claim + effect + result: one transaction -> exactly-once
    return db.transaction(async (tx) => {
      if (!(await claim(tx)).length) return (await previous(tx))!.result?.value;
      const result = await def.run(parsed, ctx, tx);                         // 4. execute
      maybeCrash("tool.inside_tx");                                           // tests: crash before COMMIT -> all rolled back
      await tx.update(toolCalls).set({ result: { value: result ?? null } }).where(eq(toolCalls.idempotencyKey, ctx.idempotencyKey)); // 5. audit
      return result;
    });
  }

  // external: at-least-once
  const claimed = await db.transaction(async (tx) => (await claim(tx)).length > 0);
  if (!claimed) {
    const prev = await db.transaction(previous);
    if (prev?.result) return prev.result.value;                              // already done -> replay-safe
    // claimed earlier but never finished (crashed mid-call) -> send again (the at-least-once window)
  }
  const result = await def.run(parsed, ctx, null as unknown as Tx);
  await db.update(toolCalls).set({ result: { value: result ?? null } }).where(eq(toolCalls.idempotencyKey, ctx.idempotencyKey));
  return result;
}

/** "approvedBy" is not trusted: policy auto-approval is explicit, human approval must exist in the DB. */
async function authorize(name: string, ctx: ToolCtx) {
  if (!ctx.approval) throw new NeedsApproval(name);
  if (ctx.approval.by === "policy:auto") return;
  const [a] = await db.select().from(approvals).where(eq(approvals.id, ctx.approval.approvalId ?? -1));
  if (!a || a.status !== "decided" || a.threadId !== ctx.threadId) throw new NeedsApproval(`${name}: no decided approval for this thread`);
}
