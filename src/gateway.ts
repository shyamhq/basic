// GATEWAY (Hono): the single front door for every channel.
//
//   POST /webhooks/sms                 inbound SMS (simulator, or a Twilio/Retell webhook)
//   POST /webhooks/retell              Retell call events (call_analyzed -> nurse_reply + memory)
//   POST /retell/functions/:name       Retell mid-call custom functions (tools for the voice agent)
//   POST /api/shifts                   a facility posts an open shift -> new ShiftFill thread
//   GET  /api/approvals                HITL inbox
//   POST /api/approvals/:id/decision   approve / edit / reject / assign / cancel
//   GET  /api/threads/:id              state + timeline + what it is waiting on
//   GET  /api/threads/:id/history      checkpoint history ("time travel")
//   GET  /                             tiny UI: approvals inbox + "be the nurse" phone simulator
//
// The gateway does NOT run agents. It authenticates, dedupes, normalizes the input into a
// mailbox event and commits {mailbox row + outbox row} in ONE transaction, then returns
// 200 in milliseconds. Webhook providers retry on slow/failed responses, so being fast
// and idempotent here matters.

import { Hono } from "hono";
import { z } from "zod";
import { clock } from "./clock.js";
import { one, q, tx } from "./db.js";
import { GRAPHS, type GraphName } from "./graphs/index.js";
import { deliverToThread, enqueue, startThread } from "./outbox.js";
import { AGENCY } from "./seed.js";

export const app = new Hono();

/** Webhook de-duplication: providers deliver at-least-once. */
async function firstTime(key: string, c?: any) {
  return Boolean(await one(`INSERT INTO inbound_requests (idempotency_key) VALUES ($1) ON CONFLICT DO NOTHING RETURNING 1`, [key], c));
}

const looksLikeOfferReply = (text: string) =>
  /\b\d{4}\b/.test(text) || /^\s*(yes|yep|yeah|sure|ok|no|nope|can'?t|i can)\b/i.test(text) || /\$\s?\d{2}/.test(text);

// ------------------------------------------------------------------ inbound SMS
const SmsIn = z.object({ from: z.string(), body: z.string().min(1), messageId: z.string() });

app.post("/webhooks/sms", async (c) => {
  const msg = SmsIn.parse(await c.req.json());
  const result = await tx(async (cx) => {
    if (!(await firstTime(`sms:${msg.messageId}`, cx))) return { duplicate: true };
    await q(`INSERT INTO comms_log (channel, direction, phone, body) VALUES ('sms','in',$1,$2)`, [msg.from, msg.body], cx);
    return routeNurseText(msg.from, msg.body, msg.messageId, cx);
  });
  return c.json(result);
});

/** Correlation: is this a reply to an open offer (-> that ShiftFill thread) or a new conversation (-> Intake)? */
async function routeNurseText(phone: string, body: string, messageId: string, cx: any) {
  const cg = await one(`SELECT id FROM caregivers WHERE phone = $1`, [phone], cx);
  if (cg && looksLikeOfferReply(body)) {
    const code = body.match(/\b(\d{4})\b/)?.[1];
    const offer = await one(
      `SELECT * FROM offers WHERE caregiver_id = $1 AND status IN ('sent','accepted','countered')
       AND ($2::text IS NULL OR code = $2) ORDER BY created_at DESC LIMIT 1`, [cg.id, code ?? null], cx,
    );
    if (offer) {
      await deliverToThread(offer.thread_id, "nurse_reply", { caregiverId: cg.id, offerCode: offer.code, text: body }, `sms:${messageId}`, cx);
      return { routed: "shiftfill", threadId: offer.thread_id };
    }
  }
  const threadId = `intake:${messageId}`;
  await startThread({ threadId, graph: "intake", agencyId: AGENCY, input: { threadId, agencyId: AGENCY, channel: "sms", fromPhone: phone, text: body } }, cx);
  return { routed: "intake", threadId };
}

// ------------------------------------------------------------------ Retell (voice)
// Production: verify the `x-retell-signature` header with the official retell-sdk before trusting the body.
app.post("/webhooks/retell", async (c) => {
  const ev = await c.req.json();
  if (ev.event !== "call_analyzed") return c.json({ ignored: ev.event });
  const call = ev.call;
  const meta = call.metadata ?? {};
  const result = await tx(async (cx) => {
    if (!(await firstTime(`retell:${call.call_id}:analyzed`, cx))) return { duplicate: true };
    const offer = await one(`SELECT * FROM offers WHERE code = $1`, [meta.offerCode], cx);
    if (!offer) return { error: "unknown offer" };
    // Post-call analysis fields are configured on the Retell agent (custom_analysis_data).
    const a = call.call_analysis?.custom_analysis_data ?? {};
    const text = a.counter_rate ? `I can do it for $${a.counter_rate}` : a.accepted ? `YES ${offer.code}` : `NO ${offer.code}`;
    await deliverToThread(offer.thread_id, "nurse_reply", { caregiverId: offer.caregiver_id, offerCode: offer.code, text, via: "call" }, `retell:${call.call_id}`, cx);
    // The transcript is gold for memory ("I don't do nights anymore, and don't call before 9").
    if (call.transcript) {
      await enqueue("memory.extract", { agencyId: AGENCY, caregiverId: offer.caregiver_id, text: call.transcript, source: "nurse_stated" }, `extract:retell:${call.call_id}`, cx);
    }
    return { routed: offer.thread_id };
  });
  return c.json(result);
});

// Mid-call tools for the voice agent. Read tools answer directly; actions become mailbox events,
// so the ShiftFill graph stays the single source of truth (the voice agent can't double-book).
app.post("/retell/functions/:name", async (c) => {
  const { args = {}, call = {} } = await c.req.json();
  const offer = await one(`SELECT * FROM offers WHERE code = $1`, [call.metadata?.offerCode ?? args.offer_code]);
  if (!offer) return c.json({ result: "I couldn't find that shift." });
  if (c.req.param("name") === "get_shift_details") {
    const s = await one(`SELECT s.*, f.name, f.notes FROM shifts s JOIN facilities f ON f.id = s.facility_id WHERE s.id = $1`, [offer.shift_id]);
    return c.json({ result: `${s.name}, ${new Date(s.starts_at).toUTCString()}, $${s.rate}/h. ${s.notes ?? ""}` });
  }
  if (c.req.param("name") === "accept_shift") {
    await deliverToThread(offer.thread_id, "nurse_reply", { caregiverId: offer.caregiver_id, offerCode: offer.code, text: `YES ${offer.code}`, via: "call" },
      `retell-fn:${call.call_id}:accept`);
    return c.json({ result: "Great - I'm confirming that now. You'll get a text in a moment." });
  }
  return c.json({ result: "Unknown function" }, 404);
});

// ------------------------------------------------------------------ facility posts an open shift
const ShiftIn = z.object({
  id: z.string(), facilityId: z.string(), startsAt: z.string(), endsAt: z.string(),
  rate: z.number(), maxRate: z.number(), specialty: z.string().nullable().default(null),
});
app.post("/api/shifts", async (c) => {
  const s = ShiftIn.parse(await c.req.json());
  const threadId = `fill:${s.id}`;
  await tx(async (cx) => {
    await q(`INSERT INTO shifts (id, agency_id, facility_id, role, specialty, starts_at, ends_at, rate, max_rate, status)
             VALUES ($1,$2,$3,'CNA',$4,$5,$6,$7,$8,'open') ON CONFLICT (id) DO NOTHING`,
      [s.id, AGENCY, s.facilityId, s.specialty, s.startsAt, s.endsAt, s.rate, s.maxRate], cx);
    await startThread({ threadId, graph: "shiftfill", agencyId: AGENCY, input: { threadId, agencyId: AGENCY, shiftId: s.id, parentThreadId: null, excludeCaregiverId: null } }, cx);
  });
  return c.json({ threadId });
});

// ------------------------------------------------------------------ HITL inbox
app.get("/api/approvals", async (c) =>
  c.json(await q(`SELECT * FROM approvals WHERE status = coalesce($1, status) ORDER BY id`, [c.req.query("status") ?? null])));

const Decision = z.object({
  action: z.enum(["approve", "edit", "reject", "assign", "cancel"]),
  edits: z.record(z.string(), z.any()).optional(),
  reason: z.string().optional(),
  decidedBy: z.string(),
});

app.post("/api/approvals/:id/decision", async (c) => {
  const d = Decision.parse(await c.req.json());
  const res = await tx(async (cx) => {
    // FOR UPDATE + status check: two coordinators clicking at once -> only the first wins.
    const a = await one(`SELECT * FROM approvals WHERE id = $1 FOR UPDATE`, [c.req.param("id")], cx);
    if (!a) return { status: 404, body: { error: "not found" } };
    if (a.status !== "pending") return { status: 409, body: { error: `already ${a.status}` } };
    await q(`UPDATE approvals SET status = 'decided', decision = $2, decided_by = $3, decided_at = $4 WHERE id = $1`,
      [a.id, d, d.decidedBy, clock.now()], cx);
    await deliverToThread(a.thread_id, "human_decision", { ...d, approvalId: a.id, interruptId: a.interrupt_id }, `decision:${a.id}`, cx);
    return { status: 200, body: { ok: true, threadId: a.thread_id } };
  });
  return c.json(res.body, res.status as any);
});

// ------------------------------------------------------------------ observability
app.get("/api/threads/:id", async (c) => {
  const run = await one(`SELECT * FROM agent_runs WHERE thread_id = $1`, [c.req.param("id")]);
  if (!run) return c.json({ error: "not found" }, 404);
  const snap = await GRAPHS[run.graph as GraphName].getState({ configurable: { thread_id: run.thread_id } });
  const inbox = await q(`SELECT id, kind, status, note FROM thread_inbox WHERE thread_id = $1 ORDER BY id`, [run.thread_id]);
  return c.json({
    run, next: snap.next, log: (snap.values as any).log ?? [],
    interrupts: snap.tasks.flatMap((t: any) => t.interrupts ?? []), inbox,
  });
});

app.get("/api/threads/:id/history", async (c) => {
  const run = await one(`SELECT * FROM agent_runs WHERE thread_id = $1`, [c.req.param("id")]);
  if (!run) return c.json({ error: "not found" }, 404);
  const out = [];
  for await (const s of GRAPHS[run.graph as GraphName].getStateHistory({ configurable: { thread_id: run.thread_id } })) {
    out.push({ checkpointId: s.config.configurable?.checkpoint_id, step: s.metadata?.step, next: s.next, createdAt: s.createdAt });
  }
  return c.json(out);
});

app.get("/api/comms", async (c) =>
  c.json(await q(`SELECT * FROM comms_log WHERE phone = coalesce($1, phone) ORDER BY id DESC LIMIT 100`, [c.req.query("phone") ?? null])));

app.get("/api/runs", async (c) => c.json(await q(`SELECT * FROM agent_runs ORDER BY created_at DESC LIMIT 50`)));

// ------------------------------------------------------------------ tiny UI
app.get("/", (c) => c.html(UI));

const UI = /* html */ `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vali mock - console</title>
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
async function send(){await j('/webhooks/sms',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({from:from.value,body:body.value,messageId:'ui-'+Date.now()})});load()}
async function decide(id,action,intent){const reason=action==='reject'?prompt('Reason?'):undefined;let edits=intent?{intent}:undefined;
 if(action==='edit')edits={rate:Number(prompt('New rate?'))};if(action==='assign')edits={caregiverId:prompt('Caregiver id?')};
 await j('/api/approvals/'+id+'/decision',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({action,edits,reason,decidedBy:'coordinator@ui'})});load()}
async function load(){
 const a=await j('/api/approvals?status=pending');
 appr.innerHTML=a.map(x=>'<div><b>#'+x.id+' '+x.kind+'</b> '+(x.payload.title||'')+'<br><small>'+JSON.stringify(x.payload.risks||x.payload.options||'')+'</small><br>'+
  (x.kind==='triage_review'?x.payload.options.map(o=>'<button onclick="decide('+x.id+',\\'edit\\',\\''+o+'\\')">'+o+'</button>').join(''):(x.payload.options||['approve','reject']).map(o=>'<button onclick="decide('+x.id+',\\''+o+'\\')">'+o+'</button>').join(''))+'</div><hr>').join('')||'empty';
 sms.textContent=(await j('/api/comms')).map(m=>m.created_at.slice(11,19)+' '+m.direction+' '+m.channel+' '+m.phone+': '+m.body).join('\\n');
 runs.textContent=(await j('/api/runs')).map(r=>r.thread_id+'  '+r.status+'  '+(r.waiting_for||'')).join('\\n');}
load();setInterval(load,2000);
</script></body></html>`;
