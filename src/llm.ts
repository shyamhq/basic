// LLM GATEWAY: the only file that talks to a model.
//
// Interview talking points this file demonstrates:
//  * Every LLM call is a small, typed function with a zod schema (strict structured
//    output), never "parse the prose".
//  * Safety rails live in CODE, before the model (emergency keywords).
//  * Graceful degradation: no key / timeout / refusal -> deterministic heuristic.
//  * One place for model routing, effort, fallbacks, retries and token accounting.
//  * The model does judgment (classify, extract); code does constraints (filters, policy).

import Anthropic from "@anthropic-ai/sdk";
import { betaZodOutputFormat } from "@anthropic-ai/sdk/helpers/beta/zod";
import { z } from "zod";
import { LLM_MODEL, USE_REAL_LLM } from "./config.js";

// ---------------------------------------------------------------- schemas
export const Intent = z.enum(["callout", "facility_request", "preference_update", "question", "emergency", "other"]);

export const TriageSchema = z.object({
  intent: Intent,
  confidence: z.number().describe("0..1"),
  urgency: z.enum(["low", "normal", "high", "critical"]),
  summary: z.string(),
});
export type Triage = z.infer<typeof TriageSchema>;

export const PrefSchema = z.object({
  type: z.enum(["avoid_shift_time", "contact_after_hour", "channel", "avoid_facility", "prefer_facility", "min_rate"]),
  value: z.union([z.string(), z.number()]),
  strength: z.enum(["hard", "soft"]).describe("hard = the nurse stated it explicitly; soft = inferred"),
  text: z.string().describe("the preference in one short sentence"),
});
export type Pref = z.infer<typeof PrefSchema> & { source?: string };

export const ReplySchema = z.object({
  intent: z.enum(["accept", "decline", "counter", "question", "unclear"]),
  counterRate: z.number().nullable(),
  preferences: z.array(PrefSchema),
});
export type Reply = z.infer<typeof ReplySchema>;

export const ReflectionSchema = z.object({
  text: z.string().describe("a reusable playbook rule written as an instruction"),
  rule: z
    .object({ type: z.literal("require_approval"), when: z.enum(["first_shift_at_facility", "overtime", "rate_above_posted"]) })
    .nullable(),
});
export type Reflection = z.infer<typeof ReflectionSchema>;

// ---------------------------------------------------------------- client + accounting
export const usage = { calls: 0, inputTokens: 0, outputTokens: 0, fallbacks: 0 };
let client: Anthropic | null = null;

async function structured<T>(schema: z.ZodType<T>, system: string, user: string): Promise<T | null> {
  if (!USE_REAL_LLM) return null;
  client ??= new Anthropic({ timeout: 30_000, maxRetries: 2 });
  try {
    const res = await client.beta.messages.parse({
      model: LLM_MODEL,
      max_tokens: 2000,
      betas: ["server-side-fallback-2026-07-01"],
      fallbacks: "default",                       // if the model declines, the API retries on a fallback model
      output_config: { effort: "low", format: betaZodOutputFormat(schema) },   // classification = low effort
      system,
      messages: [{ role: "user", content: user }],
    });
    usage.calls++;
    usage.inputTokens += res.usage.input_tokens;
    usage.outputTokens += res.usage.output_tokens;
    if (res.stop_reason === "refusal" || !res.parsed_output) return null;
    return res.parsed_output as T;
  } catch (e) {
    console.warn("[llm] falling back to heuristics:", (e as Error).message);
    usage.fallbacks++;
    return null;
  }
}

// ---------------------------------------------------------------- 1) triage an inbound message
const EMERGENCY = /\b(911|fell|fall(en)?|chest pain|not breathing|unresponsive|bleeding|seizure|abuse|injur(ed|y)|ambulance)\b/i;

export async function classifyInbound(text: string, playbook: string[] = []): Promise<Triage> {
  // Safety rail in code: never let a model "decide" an emergency isn't one.
  if (EMERGENCY.test(text)) {
    return { intent: "emergency", confidence: 1, urgency: "critical", summary: "Possible emergency - human required" };
  }
  const llm = await structured(
    TriageSchema,
    `You triage inbound texts/calls from nurses and facilities for a healthcare staffing agency.
Agency playbook:\n${playbook.map((p) => `- ${p}`).join("\n") || "- (none)"}`,
    `Message: """${text}"""`,
  );
  return llm ?? heuristicTriage(text);
}

function heuristicTriage(text: string): Triage {
  const t = text.toLowerCase();
  if (/(sick|can'?t make|cannot make|call(ing)? out|won'?t be able|not able to (come|make)|family emergency|cancel my shift)/.test(t))
    return { intent: "callout", confidence: 0.9, urgency: "high", summary: "Nurse is calling out of shifts" };
  if (/(need (a|an|\d+) (cna|rn|lvn|nurse)|short.?staffed|open shift)/.test(t))
    return { intent: "facility_request", confidence: 0.85, urgency: "high", summary: "Facility needs staff" };
  if (/(nights?|weekends?|don'?t (call|text)|before \d|prefer|stop (calling|texting)|only (days|text))/.test(t))
    return { intent: "preference_update", confidence: 0.8, urgency: "low", summary: "Nurse is updating preferences" };
  if (t.trim().endsWith("?")) return { intent: "question", confidence: 0.7, urgency: "normal", summary: "Question" };
  return { intent: "other", confidence: 0.4, urgency: "normal", summary: "Unclear message" };
}

// ---------------------------------------------------------------- 2) parse a nurse's reply to an offer
export async function parseReply(text: string, ctx: { facilityId?: string } = {}): Promise<Reply> {
  const llm = await structured(
    ReplySchema,
    `A nurse is replying to a shift offer. Classify the reply. If they name a pay rate they would accept, it is a
"counter" with counterRate. Also extract any durable preferences they state (shift times, contact hours, channel, facilities).
Use facility id "${ctx.facilityId ?? ""}" for facility preferences.`,
    `Reply: """${text}"""`,
  );
  return llm ?? heuristicReply(text, ctx);
}

function heuristicReply(text: string, ctx: { facilityId?: string }): Reply {
  const t = text.toLowerCase();
  const preferences = heuristicPreferences(text, ctx);
  const rate = t.match(/\$\s?(\d{2}(?:\.\d+)?)/) ?? t.match(/\b(\d{2})\s*(?:\/\s*hr|an hour|per hour)/);
  if (rate && /(for|if|need|at least|make it|do it)/.test(t)) {
    return { intent: "counter", counterRate: Number(rate[1]), preferences };
  }
  if (/^\s*(yes|yep|yeah|sure|ok(ay)?|i can|count me in|y)\b/.test(t)) return { intent: "accept", counterRate: null, preferences };
  if (/^\s*(no|nope|can'?t|cannot|not available|pass|n)\b/.test(t)) return { intent: "decline", counterRate: null, preferences };
  if (t.includes("?")) return { intent: "question", counterRate: null, preferences };
  return { intent: "unclear", counterRate: null, preferences };
}

// ---------------------------------------------------------------- 3) extract memories from free text / transcripts
export async function extractPreferences(text: string, ctx: { facilityId?: string } = {}): Promise<Pref[]> {
  const llm = await structured(
    z.object({ preferences: z.array(PrefSchema) }),
    `Extract durable work preferences a nurse states in this message or call transcript. Only things that will
still be true next week. Explicit statements are "hard"; hedged ones are "soft". Facility id in context: "${ctx.facilityId ?? ""}".`,
    text,
  );
  return llm?.preferences ?? heuristicPreferences(text, ctx);
}

function heuristicPreferences(text: string, ctx: { facilityId?: string }): Pref[] {
  const t = text.toLowerCase();
  const out: Pref[] = [];
  if (/(no|don'?t do|not doing|stopped doing|can'?t do) (any )?(more )?nights?|no more nights|nights? anymore/.test(t))
    out.push({ type: "avoid_shift_time", value: "night", strength: "hard", text: "Does not work night shifts" });
  if (/only (do )?(days|day shifts|mornings)/.test(t)) {
    out.push({ type: "avoid_shift_time", value: "night", strength: "hard", text: "Only works day shifts" });
    out.push({ type: "avoid_shift_time", value: "evening", strength: "hard", text: "Only works day shifts" });
  }
  const before = t.match(/don'?t (call|text|contact) (me )?before (\d{1,2})/);
  if (before) out.push({ type: "contact_after_hour", value: Number(before[3]), strength: "hard", text: `Do not contact before ${before[3]}:00` });
  if (/(text me (instead|only)|please text|don'?t call me$|no (phone )?calls)/.test(t))
    out.push({ type: "channel", value: "sms", strength: "hard", text: "Prefers texts over calls" });
  if (/(call me instead|prefer (a )?call)/.test(t)) out.push({ type: "channel", value: "call", strength: "hard", text: "Prefers phone calls" });
  if (ctx.facilityId && /(too far|long drive|commute)/.test(t))
    out.push({ type: "avoid_facility", value: ctx.facilityId, strength: "soft", text: `Finds ${ctx.facilityId} too far` });
  if (ctx.facilityId && /(love|loved|enjoy|great team|like working)/.test(t))
    out.push({ type: "prefer_facility", value: ctx.facilityId, strength: "soft", text: `Likes working at ${ctx.facilityId}` });
  return out;
}

// ---------------------------------------------------------------- 4) reflect on a human correction -> procedural memory
export async function reflect(feedback: { decision: string; reason: string; context: string }): Promise<Reflection> {
  const llm = await structured(
    ReflectionSchema,
    `A human staffing coordinator overrode an AI agent. Turn their reason into ONE reusable playbook rule the agent
should follow next time. If it maps to a structured policy, fill "rule"; otherwise rule = null.`,
    JSON.stringify(feedback),
  );
  if (llm) return llm;
  const r = feedback.reason.toLowerCase();
  if (/first (shift|time)|never worked|new to (this|the) facility|orientation/.test(r))
    return { text: "Ask a coordinator before booking a nurse's first-ever shift at a facility.", rule: { type: "require_approval", when: "first_shift_at_facility" } };
  if (/rate|pay|budget/.test(r))
    return { text: "Ask a coordinator before agreeing to any rate above the posted rate.", rule: { type: "require_approval", when: "rate_above_posted" } };
  return { text: `Coordinator guidance: ${feedback.reason}`, rule: null };
}
