// COMMS ADAPTER: how the agent reaches a nurse.
//
//   SimulatorComms (default): writes to comms_log. Free, offline, used by tests and the demo.
//                             The gateway's /sim/* endpoints let you "be the nurse".
//   RetellComms:              real outbound phone calls via Retell's voice agents.
//
// The agent never talks to Retell directly - it calls the tool gateway, which calls
// this adapter. Swapping providers (Retell <-> Vapi <-> Twilio) touches only this file.
//
// How a Retell call plugs into the long-running agent:
//   1. ShiftFill decides "call Anita" -> startCall({ dynamicVars }) with the shift details AND
//      memory (name, preferred times, notes) -> Retell agent prompt uses {{variables}}.
//   2. During the call Retell may call our custom functions (POST /retell/functions/:name),
//      e.g. get_shift_details or accept_shift. accept_shift does NOT book anything: it drops a
//      mailbox event, so the graph stays the single source of truth.
//   3. After the call Retell POSTs `call_analyzed` (transcript + post-call analysis) to
//      /webhooks/retell -> we turn it into the same "nurse_reply" event an SMS would create,
//      plus a memory.extract job on the transcript.
//
// SMS on Retell/Vapi in the US requires A2P 10DLC registration (~2-3 weeks), so SMS stays on
// the simulator here. For a quick real test, start Retell outbound calls or browser "web calls".

import { COMMS_PROVIDER } from "./config.js";
import { q } from "./db.js";

export interface Comms {
  sendSms(to: string, body: string, meta?: Record<string, unknown>): Promise<{ id: string }>;
  startCall(to: string, dynamicVars: Record<string, string>, meta?: Record<string, unknown>): Promise<{ id: string }>;
}

export class SimulatorComms implements Comms {
  async sendSms(to: string, body: string, meta: Record<string, unknown> = {}) {
    const [r] = await q(`INSERT INTO comms_log (channel, direction, phone, body, meta) VALUES ('sms','out',$1,$2,$3) RETURNING id`, [to, body, meta]);
    return { id: `sim-sms-${r.id}` };
  }
  async startCall(to: string, vars: Record<string, string>, meta: Record<string, unknown> = {}) {
    const body = `[voice call] ${vars.opening ?? ""}`;
    const [r] = await q(`INSERT INTO comms_log (channel, direction, phone, body, meta) VALUES ('call','out',$1,$2,$3) RETURNING id`, [to, body, { ...meta, vars }]);
    return { id: `sim-call-${r.id}` };
  }
}

export class RetellComms implements Comms {
  private sim = new SimulatorComms();
  async sendSms(to: string, body: string, meta?: Record<string, unknown>) {
    // Needs an A2P-registered number on Retell; we log via the simulator instead.
    return this.sim.sendSms(to, body, { ...meta, note: "retell sms not configured - simulated" });
  }
  async startCall(to: string, vars: Record<string, string>, meta: Record<string, unknown> = {}) {
    const res = await fetch("https://api.retellai.com/v2/create-phone-call", {
      method: "POST",
      headers: { Authorization: `Bearer ${process.env.RETELL_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        from_number: process.env.RETELL_FROM_NUMBER,
        to_number: to,
        override_agent_id: process.env.RETELL_AGENT_ID,
        retell_llm_dynamic_variables: vars,          // injected into the voice agent's prompt
        metadata: meta,                              // comes back on every webhook -> correlation (offer code, thread id)
      }),
    });
    if (!res.ok) throw new Error(`retell create-phone-call ${res.status}: ${await res.text()}`);
    const data = (await res.json()) as { call_id: string };
    await q(`INSERT INTO comms_log (channel, direction, phone, body, meta) VALUES ('call','out',$1,$2,$3)`, [to, "[retell call]", { ...meta, vars, call_id: data.call_id }]);
    return { id: data.call_id };
  }
}

export const comms: Comms = COMMS_PROVIDER === "retell" ? new RetellComms() : new SimulatorComms();
