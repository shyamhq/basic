// GATEWAY (Hono): the single front door for every channel.
//
//   POST /webhooks/sms                 inbound SMS (simulator, or a Twilio/Retell webhook)
//   POST /webhooks/retell              Retell call events (call_analyzed -> nurse_reply + memory)
//   POST /retell/functions/:name       Retell mid-call custom functions (tools for the voice agent)
//   POST /api/shifts                   a facility posts an open shift -> ShiftFill thread "fill:<id>"
//   POST /api/threads/:id/cancel       stop a thread (every pause accepts "cancel")
//   GET  /api/approvals                HITL inbox
//   POST /api/approvals/:id/decision   decide; the body is validated against the approval's KIND
//   GET  /api/threads/:id(/history)    state, timeline, mailbox; checkpoint history
//   GET  /                             tiny console: approvals inbox + "be the nurse" simulator
//
// The gateway never runs agents. It authenticates, resolves the tenant, dedupes, turns the input
// into a mailbox event, and commits {mailbox row + kick job} in ONE transaction. Then 200, fast.
// Bad input -> 400 (zod-validator), never 500: webhook providers retry 5xx forever.

import { zValidator } from "@hono/zod-validator";
import { and, desc, eq, inArray, notInArray } from "drizzle-orm";
import { Hono } from "hono";
import { z } from "zod";
import { clock } from "./clock.js";
import { db, type Tx } from "./db/index.js";
import { agencies, agentRuns, approvals, caregivers, commsLog, inboundRequests, offers, shifts, threadInbox } from "./db/schema.js";
import { getGraph } from "./graphs/index.js";
import { deliver, startThread } from "./mailbox.js";
import { addMemoryJob } from "./jobs.js";

export const app = new Hono();

/** Webhook de-duplication: providers deliver at-least-once. */
async function firstTime(tx: Tx, key: string) {
  return (await tx.insert(inboundRequests).values({ idempotencyKey: key }).onConflictDoNothing().returning()).length > 0;
}
/** Mock auth: the tenant comes from an API key in production; here, from a header. */
const agencyOf = (c: { req: { header: (k: string) => string | undefined } }) => c.req.header("x-agency-id") ?? "ag_bright";

const looksLikeOfferReply = (text: string) =>
  /\b\d{4}\b/.test(text) || /^\s*(yes|yep|yeah|sure|ok|no|nope|can'?t|i can)\b/i.test(text) || /\$\s?\d{2}/.test(text);

// ------------------------------------------------------------------ inbound SMS
app.post("/webhooks/sms",
  zValidator("json", z.object({ from: z.string(), to: z.string(), body: z.string().min(1), messageId: z.string() })),
  async (c) => {
    const msg = c.req.valid("json");
    const result = await db.transaction(async (tx) => {
      const [agency] = await tx.select().from(agencies).where(eq(agencies.phone, msg.to));   // the dialed number -> tenant
      if (!agency) return { error: "unknown number" };
      if (!(await firstTime(tx, `sms:${msg.messageId}`))) return { duplicate: true };
      await tx.insert(commsLog).values({ agencyId: agency.id, channel: "sms", direction: "in", phone: msg.from, body: msg.body });
      return routeNurseText(tx, agency.id, msg.from, msg.body, msg.messageId);
    });
    return c.json(result, "error" in result ? 404 : 200);
  });

/** Correlation: a reply to an open offer -> that ShiftFill thread; a follow-up -> the open
 *  conversation ("double texting"); otherwise a new Intake conversation. */
async function routeNurseText(tx: Tx, agencyId: string, phone: string, body: string, messageId: string) {
  const [cg] = await tx.select().from(caregivers).where(and(eq(caregivers.phone, phone), eq(caregivers.agencyId, agencyId)));
  if (cg && looksLikeOfferReply(body)) {
    const code = body.match(/\b(\d{4})\b/)?.[1];
    const [offer] = await tx.select().from(offers)
      .where(and(eq(offers.caregiverId, cg.id), inArray(offers.status, ["sent", "accepted", "countered"]), code ? eq(offers.code, code) : undefined))
      .orderBy(desc(offers.createdAt)).limit(1);
    if (offer) {
      await deliver(tx, { agencyId, threadId: offer.threadId, kind: "nurse_reply", dedupeKey: `sms:${messageId}`,
        payload: { caregiverId: cg.id, offerCode: offer.code, shiftId: offer.shiftId, text: body, phone, firstName: cg.name.split(" ")[0] } });
      return { routed: "shiftfill", threadId: offer.threadId };
    }
  }
  const [open] = await tx.select().from(agentRuns)
    .where(and(eq(agentRuns.graph, "intake"), eq(agentRuns.subject, phone), notInArray(agentRuns.status, ["done", "failed"])));
  if (open) {
    await deliver(tx, { agencyId, threadId: open.threadId, kind: "nurse_message", payload: { text: body }, dedupeKey: `sms:${messageId}` });
    return { routed: "intake-followup", threadId: open.threadId };
  }
  const threadId = `intake:${phone}:${messageId}`;
  await startThread({ threadId, graph: "intake", agencyId, subject: phone, input: { threadId, agencyId, channel: "sms", fromPhone: phone, text: body } }, tx);
  return { routed: "intake", threadId };
}

// ------------------------------------------------------------------ Retell (voice)
// Production: verify the `x-retell-signature` header with the official retell-sdk before trusting the body.
// DEDUPE BY MEANING, not by transport: the mid-call accept_shift function AND the post-call
// call_analyzed webhook describe the SAME acceptance -> both use key "offer:<code>:accept".
async function deliverOfferIntent(tx: Tx, offer: typeof offers.$inferSelect, intent: string, text: string, via: string) {
  const [cg] = await tx.select().from(caregivers).where(eq(caregivers.id, offer.caregiverId));
  return deliver(tx, { agencyId: offer.agencyId, threadId: offer.threadId, kind: "nurse_reply", dedupeKey: `offer:${offer.code}:${intent}`,
    payload: { caregiverId: offer.caregiverId, offerCode: offer.code, shiftId: offer.shiftId, text, via, phone: cg.phone, firstName: cg.name.split(" ")[0] } });
}

app.post("/webhooks/retell", async (c) => {
  const ev = await c.req.json();
  if (ev.event !== "call_analyzed") return c.json({ ignored: ev.event });
  const call = ev.call ?? {};
  const result = await db.transaction(async (tx) => {
    if (!(await firstTime(tx, `retell:${call.call_id}:analyzed`))) return { duplicate: true };
    const [offer] = await tx.select().from(offers).where(eq(offers.code, String(call.metadata?.offerCode ?? "")));
    if (!offer) return { error: "unknown offer" };
    const a = call.call_analysis?.custom_analysis_data ?? {};       // configured on the Retell agent
    if (a.counter_rate) await deliverOfferIntent(tx, offer, `counter:${a.counter_rate}`, `I can do it for $${a.counter_rate}`, "call");
    else if (a.accepted) await deliverOfferIntent(tx, offer, "accept", `YES ${offer.code}`, "call");
    else await deliverOfferIntent(tx, offer, "decline", `NO ${offer.code}`, "call");
    if (call.transcript) {                                          // transcripts are gold for memory
      await addMemoryJob(tx, "memory_extract", { agencyId: offer.agencyId, caregiverId: offer.caregiverId, text: call.transcript, source: "nurse_stated" }, `extract:retell:${call.call_id}`);
    }
    return { routed: offer.threadId };
  });
  return c.json(result, "error" in result ? 404 : 200);
});

// Mid-call tools for the voice agent. Reads answer directly; actions become mailbox events so the
// ShiftFill graph stays the single source of truth (the voice agent can't double-book).
app.post("/retell/functions/:name", async (c) => {
  const { args = {}, call = {} } = await c.req.json();
  const [offer] = await db.select().from(offers).where(eq(offers.code, String(call.metadata?.offerCode ?? args.offer_code ?? "")));
  if (!offer) return c.json({ result: "I couldn't find that shift." });
  if (c.req.param("name") === "get_shift_details") {
    const [s] = await db.select().from(shifts).where(eq(shifts.id, offer.shiftId));
    return c.json({ result: `${new Date(s.startsAt).toUTCString()}, $${s.rate}/h.` });
  }
  if (c.req.param("name") === "accept_shift") {
    await db.transaction((tx) => deliverOfferIntent(tx, offer, "accept", `YES ${offer.code}`, "call"));
    return c.json({ result: "Great - I'm confirming that now. You'll get a text in a moment." });
  }
  return c.json({ result: "Unknown function" }, 404);
});

// ------------------------------------------------------------------ facility posts an open shift
app.post("/api/shifts",
  zValidator("json", z.object({
    id: z.string(), facilityId: z.string(), startsAt: z.iso.datetime(), endsAt: z.iso.datetime(),
    rate: z.number().positive(), maxRate: z.number().positive(), specialty: z.string().nullable().default(null),
  })),
  async (c) => {
    const s = c.req.valid("json");
    const agencyId = agencyOf(c);
    const threadId = `fill:${s.id}`;                               // natural key: one fill thread per shift, ever
    await db.transaction(async (tx) => {
      await tx.insert(shifts).values({ id: s.id, agencyId, facilityId: s.facilityId, role: "CNA", specialty: s.specialty,
        startsAt: new Date(s.startsAt), endsAt: new Date(s.endsAt), rate: s.rate, maxRate: s.maxRate, status: "open" }).onConflictDoNothing();
      await startThread({ threadId, graph: "shiftfill", agencyId, subject: s.id, input: { threadId, agencyId, shiftId: s.id, parentThreadId: null, excludeCaregiverId: null } }, tx);
    });
    return c.json({ threadId });
  });

app.post("/api/threads/:id/cancel", zValidator("json", z.object({ reason: z.string() })), async (c) => {
  const [run] = await db.select().from(agentRuns).where(eq(agentRuns.threadId, c.req.param("id")));
  if (!run) return c.json({ error: "not found" }, 404);
  await db.transaction((tx) => deliver(tx, { agencyId: run.agencyId, threadId: run.threadId, kind: "cancel", payload: c.req.valid("json"), dedupeKey: `cancel:${run.threadId}` }));
  return c.json({ ok: true });
});

// ------------------------------------------------------------------ HITL inbox
app.get("/api/approvals", async (c) =>
  c.json(await db.select().from(approvals).where(and(eq(approvals.agencyId, agencyOf(c)), c.req.query("status") ? eq(approvals.status, c.req.query("status") as any) : undefined)).orderBy(approvals.id)));

// A decision must make sense for the KIND of pause it answers. "approve" on an escalation, or an
// "assign" to a caregiver who doesn't exist, is rejected here with a 400 - never sent to the graph.
const by = { decidedBy: z.string().min(1) };
const DECISIONS = {
  approval: z.discriminatedUnion("action", [
    z.object({ action: z.literal("approve"), ...by }),
    z.object({ action: z.literal("edit"), edits: z.object({ rate: z.number().positive() }), ...by }),
    z.object({ action: z.literal("reject"), reason: z.string().min(3), ...by }),
  ]),
  escalation: z.discriminatedUnion("action", [
    z.object({ action: z.literal("assign"), edits: z.object({ caregiverId: z.string(), rate: z.number().positive().optional() }), ...by }),
    z.object({ action: z.literal("cancel"), reason: z.string().optional(), ...by }),
  ]),
  triage_review: z.object({ action: z.literal("edit"), edits: z.object({ intent: z.enum(["callout", "preference_update", "question", "other"]) }), ...by }),
  review_outbound: z.discriminatedUnion("action", [
    z.object({ action: z.literal("approve"), edits: z.object({ drafts: z.array(z.any()) }).optional(), ...by }),
    z.object({ action: z.literal("reject"), reason: z.string().optional(), ...by }),
  ]),
  emergency_ack: z.object({ action: z.literal("approve"), reason: z.string().optional(), ...by }),
} as const;

app.post("/api/approvals/:id/decision", async (c) => {
  const body = await c.req.json();
  const res = await db.transaction(async (tx) => {
    // FOR UPDATE + status check: two coordinators clicking at once -> the first wins, the second gets 409.
    const [a] = await tx.select().from(approvals).where(eq(approvals.id, Number(c.req.param("id")))).for("update");
    if (!a) return { status: 404, body: { error: "not found" } };
    if (a.status !== "pending") return { status: 409, body: { error: `already ${a.status}` } };
    const parsed = DECISIONS[a.kind].safeParse(body);
    if (!parsed.success) return { status: 400, body: { error: `invalid decision for this ${a.kind}`, issues: parsed.error.issues } };
    const d = parsed.data as any;
    if (d.action === "assign") {
      const [cg] = await tx.select().from(caregivers).where(and(eq(caregivers.id, d.edits.caregiverId), eq(caregivers.agencyId, a.agencyId)));
      if (!cg) return { status: 400, body: { error: `unknown caregiver ${d.edits.caregiverId}` } };
    }
    await tx.update(approvals).set({ status: "decided", decision: d, decidedBy: d.decidedBy, decidedAt: clock.now() }).where(eq(approvals.id, a.id));
    await deliver(tx, { agencyId: a.agencyId, threadId: a.threadId, kind: "human_decision", payload: { ...d, approvalId: a.id, interruptId: a.interruptId }, dedupeKey: `decision:${a.id}` });
    return { status: 200, body: { ok: true, threadId: a.threadId } };
  });
  return c.json(res.body, res.status as 200);
});

// ------------------------------------------------------------------ observability
app.get("/api/threads/:id", async (c) => {
  const [run] = await db.select().from(agentRuns).where(eq(agentRuns.threadId, c.req.param("id")));
  if (!run) return c.json({ error: "not found" }, 404);
  const snap = await getGraph(run.graph, run.graphVersion).getState({ configurable: { thread_id: run.threadId } });
  const inbox = await db.select({ id: threadInbox.id, kind: threadInbox.kind, status: threadInbox.status, note: threadInbox.note })
    .from(threadInbox).where(eq(threadInbox.threadId, run.threadId)).orderBy(threadInbox.id);
  return c.json({ run, next: snap.next, log: (snap.values as any).log ?? [], interrupts: snap.tasks.flatMap((t: any) => t.interrupts ?? []), inbox });
});

app.get("/api/threads/:id/history", async (c) => {
  const [run] = await db.select().from(agentRuns).where(eq(agentRuns.threadId, c.req.param("id")));
  if (!run) return c.json({ error: "not found" }, 404);
  const out = [];
  for await (const s of getGraph(run.graph, run.graphVersion).getStateHistory({ configurable: { thread_id: run.threadId } })) {
    out.push({ checkpointId: s.config.configurable?.checkpoint_id, step: s.metadata?.step, next: s.next, createdAt: s.createdAt });
  }
  return c.json(out);
});

app.get("/api/comms", async (c) => {
  const phone = c.req.query("phone");
  return c.json(await db.select().from(commsLog).where(phone ? eq(commsLog.phone, phone) : undefined).orderBy(desc(commsLog.id)).limit(100));
});
app.get("/api/runs", async (c) => c.json(await db.select().from(agentRuns).orderBy(desc(agentRuns.createdAt)).limit(50)));

// ------------------------------------------------------------------ tiny console
app.get("/", (c) => c.html(UI));

const UI = /* html */ `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vali mock console</title>
<style>body{font:14px system-ui;margin:16px;max-width:1100px}section{border:1px solid #ccc;border-radius:8px;padding:12px;margin:12px 0}
pre{white-space:pre-wrap;background:#f6f6f6;padding:8px;border-radius:6px}button{margin-right:6px}input{margin:2px}</style></head><body>
<h2>Vali mock console</h2>
<section><h3>Be the nurse (SMS simulator)</h3>
From <input id="from" value="+15550001004" size="14"> <input id="body" value="YES" size="40"> <button onclick="send()">Send SMS</button>
<pre id="sms"></pre></section>
<section><h3>Approvals inbox (human in the loop)</h3><div id="appr"></div></section>
<section><h3>Agent runs</h3><pre id="runs"></pre></section>
<script>
async function j(u,o){const r=await fetch(u,o);return r.json()}
async function send(){await j('/webhooks/sms',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({from:from.value,to:'+15550009999',body:body.value,messageId:'ui-'+Date.now()})});load()}
async function decide(id,action,intent){const reason=['reject','cancel'].includes(action)?prompt('Reason?'):undefined;let edits=intent?{intent}:undefined;
 if(action==='edit'&&!intent)edits={rate:Number(prompt('New rate?'))};if(action==='assign')edits={caregiverId:prompt('Caregiver id?')};
 const r=await j('/api/approvals/'+id+'/decision',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({action,edits,reason,decidedBy:'coordinator@ui'})});
 if(r.error)alert(r.error);load()}
async function load(){
 const a=await j('/api/approvals?status=pending');
 appr.innerHTML=a.map(x=>'<div><b>#'+x.id+' '+x.kind+'</b> '+(x.payload.title||'')+'<br><small>'+JSON.stringify(x.payload.risks||'')+'</small><br>'+
  (x.kind==='triage_review'?x.payload.options.map(o=>'<button onclick="decide('+x.id+',\\'edit\\',\\''+o+'\\')">'+o+'</button>').join(''):(x.payload.options||['approve','reject']).map(o=>'<button onclick="decide('+x.id+',\\''+o+'\\')">'+o+'</button>').join(''))+'</div><hr>').join('')||'empty';
 sms.textContent=(await j('/api/comms')).map(m=>m.createdAt.slice(11,19)+' '+m.direction+' '+m.channel+' '+m.phone+': '+m.body).join('\\n');
 runs.textContent=(await j('/api/runs')).map(r=>r.threadId+'  '+r.status+'  '+(r.waitingFor||'')).join('\\n');}
load();setInterval(load,2000);
</script></body></html>`;
