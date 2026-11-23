// COMMS ADAPTER: how the agent reaches a nurse.
//
//   SimulatorComms (default): writes to app.comms_log. Free, offline; tests, demo and the console's
//                             "be the nurse" panel use it.
//   RetellComms:              real outbound phone calls through Retell voice agents.
//
// The agent never calls Retell directly: graph -> tool gateway -> this adapter. Swapping providers
// (Retell <-> Vapi <-> Twilio) only touches this file.
//
// How a Retell call plugs into the long-running agent:
//   1. ShiftFill decides "call Anita" -> startCall(vars): the shift details AND her memory
//      (preferences, facility notes) become Retell dynamic variables in the voice agent's prompt.
//   2. Mid-call, Retell may call our custom functions (POST /retell/functions/:name). accept_shift
//      doesn't book anything: it drops a mailbox event, so the graph stays the source of truth.
//   3. After the call Retell POSTs `call_analyzed` (transcript + analysis) -> the same nurse_reply
//      event an SMS would create, plus a memory.extract job on the transcript.
//
// SMS through Retell/Vapi in the US requires A2P 10DLC registration (~2-3 weeks), so SMS stays on
// the simulator here.

import { COMMS_PROVIDER } from "./config.js";
import { db } from "./db/index.js";
import { commsLog } from "./db/schema.js";

type Meta = Record<string, unknown> & { agencyId: string };

export interface Comms {
  sendSms(to: string, body: string, meta: Meta): Promise<{ id: string }>;
  startCall(to: string, vars: Record<string, string>, meta: Meta): Promise<{ id: string }>;
}

export class SimulatorComms implements Comms {
  async sendSms(to: string, body: string, meta: Meta) {
    const [r] = await db.insert(commsLog).values({ agencyId: meta.agencyId, channel: "sms", direction: "out", phone: to, body, meta }).returning();
    return { id: `sim-sms-${r.id}` };
  }
  async startCall(to: string, vars: Record<string, string>, meta: Meta) {
    const [r] = await db.insert(commsLog).values({ agencyId: meta.agencyId, channel: "call", direction: "out", phone: to, body: `[voice call] ${vars.opening ?? ""}`, meta: { ...meta, vars } }).returning();
    return { id: `sim-call-${r.id}` };
  }
}

export class RetellComms implements Comms {
  private sim = new SimulatorComms();
  async sendSms(to: string, body: string, meta: Meta) {
    return this.sim.sendSms(to, body, { ...meta, note: "Retell SMS needs an A2P-registered number - simulated" });
  }
  async startCall(to: string, vars: Record<string, string>, meta: Meta) {
    const res = await fetch("https://api.retellai.com/v2/create-phone-call", {
      method: "POST",
      signal: AbortSignal.timeout(10_000),                     // never let a slow provider hang a worker
      headers: { Authorization: `Bearer ${process.env.RETELL_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        from_number: process.env.RETELL_FROM_NUMBER,
        to_number: to,
        override_agent_id: process.env.RETELL_AGENT_ID,
        retell_llm_dynamic_variables: vars,                    // injected into the voice agent's prompt
        metadata: meta,                                        // echoed on every webhook -> correlation
      }),
    });
    if (!res.ok) throw new Error(`retell create-phone-call ${res.status}: ${await res.text()}`);
    const data = (await res.json()) as { call_id: string };
    await db.insert(commsLog).values({ agencyId: meta.agencyId, channel: "call", direction: "out", phone: to, body: "[retell call]", meta: { ...meta, vars, callId: data.call_id } });
    return { id: data.call_id };
  }
}

export const comms: Comms = COMMS_PROVIDER === "retell" ? new RetellComms() : new SimulatorComms();
