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

// A small operations console: "be the nurse" phone simulator, approvals inbox, threads + timelines.
// Plain HTML + fetch polling - no build step. (No backticks or ${} below: this lives inside a TS template.)
const UI = /* html */ `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Vali Ops Console</title>
<style>
:root{--bg:#f5f6f8;--card:#fff;--fg:#18212c;--mut:#5d6878;--line:#dde2e9;--acc:#0f6e6a;--warn:#9a5b00;--warnbg:#fdf3e1;--in:#e8f1fb;--out:#e9f5f1}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif}
header{background:#13202b;color:#fff;padding:12px 20px;display:flex;align-items:center;gap:14px}
header b{font-size:16px}header span{color:#9fb3c4;font-size:13px}
#caption{display:none;background:#0f6e6a;color:#fff;font-size:20px;font-weight:600;padding:14px 20px}
main{display:grid;grid-template-columns:1fr 1.25fr;gap:16px;padding:16px 20px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px;display:flex;flex-direction:column;gap:8px;min-width:0}
h3{margin:0;font-size:13px;letter-spacing:.05em;text-transform:uppercase;color:var(--mut)}
.row{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
input,select{font:inherit;padding:6px 8px;border:1px solid var(--line);border-radius:6px}
button{font:inherit;padding:6px 12px;border:1px solid var(--acc);background:var(--acc);color:#fff;border-radius:6px;cursor:pointer}
button.ghost{background:#fff;color:var(--acc)}
.msgs{max-height:300px;overflow:auto;display:flex;flex-direction:column;gap:4px}
.m{padding:5px 8px;border-radius:6px;font-size:13px}.m.in{background:var(--in)}.m.out{background:var(--out)}
.m small{color:var(--mut)}
.appr{border:1px solid #f0d9a8;background:var(--warnbg);border-radius:8px;padding:8px 10px;display:flex;flex-direction:column;gap:6px}
.chip{display:inline-block;font-size:12px;padding:1px 8px;border-radius:99px;border:1px solid var(--warn);color:var(--warn);margin-right:4px}
.runs{display:flex;flex-direction:column;gap:3px}
.run{display:flex;justify-content:space-between;gap:8px;padding:5px 8px;border-radius:6px;cursor:pointer;font-family:ui-monospace,Menlo,monospace;font-size:12.5px}
.run:hover,.run.sel{background:#eef3f7}
.pill{font-size:11px;padding:1px 8px;border-radius:99px;font-family:system-ui}
.p-done{background:#e2f1ef;color:#0f6e6a}.p-waiting_event{background:#e8f1fb;color:#24588f}.p-waiting_human{background:var(--warnbg);color:var(--warn)}.p-running,.p-queued{background:#eee;color:#555}.p-failed{background:#fbe4e1;color:#a33a2b}
pre{margin:0;white-space:pre-wrap;font:12.5px/1.5 ui-monospace,Menlo,monospace;background:#f7f8fa;border-radius:6px;padding:8px;max-height:360px;overflow:auto}
.empty{color:var(--mut);font-size:13px}
</style></head><body>
<header><b>Vali Ops Console</b><span>long-running nurse-staffing agents · LangGraph.js · Postgres · graphile-worker</span></header>
<div id="caption"></div>
<main>
 <div style="display:flex;flex-direction:column;gap:16px">
  <div class="card"><h3>Nurse phone (SMS simulator)</h3>
   <div class="row"><select id="from">
    <option value="+15550001001">Sofia Rossi</option><option value="+15550001002">Maria Lopez</option>
    <option value="+15550001003">Anita Reddy</option><option value="+15550001004" selected>James Okafor</option>
    <option value="+15550001005">Grace Lee</option></select>
    <input id="body" placeholder="type a text..." style="flex:1;min-width:180px"><button id="send" onclick="send()">Send</button></div>
  </div>
  <div class="card"><h3>Messages (newest first)</h3><div id="msgs" class="msgs"></div></div>
 </div>
 <div style="display:flex;flex-direction:column;gap:16px">
  <div class="card"><h3>Approvals inbox (human in the loop)</h3><div id="appr"></div></div>
  <div class="card"><h3>Agent threads</h3><div id="runs" class="runs"></div></div>
  <div class="card"><h3>Timeline: <span id="selname">click a thread</span></h3><pre id="timeline"></pre></div>
 </div>
</main>
<script>
var sel=null, names={'+15550001001':'Sofia','+15550001002':'Maria','+15550001003':'Anita','+15550001004':'James','+15550001005':'Grace','+15550000001':'Manager'};
function esc(s){return String(s==null?'':s).replace(/[&<>]/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;'}[c]})}
async function j(u,o){var r=await fetch(u,o);return r.json()}
async function send(){var b=document.getElementById('body');if(!b.value)return;
 await j('/webhooks/sms',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({from:document.getElementById('from').value,to:'+15550009999',body:b.value,messageId:'ui-'+Date.now()})});b.value='';load()}
function val(id){var e=document.getElementById(id);return e?e.value:''}
async function decide(id,action,intent){var body={action:action,decidedBy:'coordinator@console'};
 if(intent)body.edits={intent:intent};
 if(action==='edit'&&!intent)body.edits={rate:Number(val('rate-'+id))};
 if(action==='assign')body.edits={caregiverId:val('cg-'+id)};
 if(action==='reject'||action==='cancel')body.reason=val('reason-'+id)||'no reason given';
 var r=await j('/api/approvals/'+id+'/decision',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
 if(r.error)document.getElementById('err-'+id).textContent=r.error;load()}
function btn(id,a,label,ghost,intent){return '<button class="'+(ghost?'ghost':'')+'" onclick="decide('+id+',\\''+a+'\\''+(intent?',\\''+intent+'\\'':'')+')">'+label+'</button>'}
function card(x){var p=x.payload,h='<div class="appr"><div><b>#'+x.id+' '+esc(x.kind)+'</b> '+esc(p.title)+'</div>';
 if(p.risks&&p.risks.length)h+='<div>'+p.risks.map(function(r){return '<span class="chip">'+esc(r)+'</span>'}).join('')+'</div>';
 var row='<div class="row">';
 if(x.kind==='approval')row+=btn(x.id,'approve','Approve')+'<input id="rate-'+x.id+'" size="4" placeholder="$/h">'+btn(x.id,'edit','Edit rate',1)+'<input id="reason-'+x.id+'" placeholder="reason">'+btn(x.id,'reject','Reject',1);
 else if(x.kind==='escalation')row+='<input id="cg-'+x.id+'" size="10" placeholder="cg_grace">'+btn(x.id,'assign','Assign')+'<input id="reason-'+x.id+'" placeholder="reason">'+btn(x.id,'cancel','Cancel shift',1);
 else if(x.kind==='triage_review')row+=p.options.map(function(o){return btn(x.id,'edit',o,1,o)}).join('');
 else if(x.kind==='review_outbound')row+=btn(x.id,'approve','Approve drafts')+btn(x.id,'reject','Reject',1);
 else row+=btn(x.id,'approve','Acknowledge');
 return h+row+'</div><div id="err-'+x.id+'" style="color:#a33a2b;font-size:12px"></div></div>'}
function pick(t){sel=t;load()}
async function load(){
 var a=await j('/api/approvals?status=pending');
 var ae=document.getElementById('appr'), focused=ae.contains(document.activeElement);
 if(!focused)ae.innerHTML=a.length?a.map(card).join(''):'<div class="empty">Nothing needs a human right now.</div>';
 var m=await j('/api/comms');
 document.getElementById('msgs').innerHTML=m.map(function(x){return '<div class="m '+x.direction+'"><small>'+x.createdAt.slice(11,19)+' · '+(x.direction==='in'?'from ':'to ')+(names[x.phone]||x.phone)+' · '+x.channel+'</small><br>'+esc(x.body)+'</div>'}).join('')||'<div class="empty">No messages yet.</div>';
 var r=await j('/api/runs');
 if(!sel&&r.length)sel=r[r.length-1].threadId;
 document.getElementById('runs').innerHTML=r.map(function(x){return '<div class="run'+(x.threadId===sel?' sel':'')+'" onclick="pick(\\''+x.threadId+'\\')"><span>'+esc(x.threadId)+'</span><span class="pill p-'+x.status+'">'+x.status+(x.waitingFor?' · '+x.waitingFor:'')+'</span></div>'}).join('')||'<div class="empty">No agent threads yet.</div>';
 if(sel){var t=await j('/api/threads/'+encodeURIComponent(sel));document.getElementById('selname').textContent=sel;
  document.getElementById('timeline').textContent=(t.log||[]).join('\\n')}}
load();setInterval(load,1500);
</script></body></html>`;
