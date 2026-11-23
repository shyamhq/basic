// Creates the Retell voice agent the ShiftFill graph calls nurses with.
//   RETELL_API_KEY=... npx tsx scripts/retell-setup.ts            (or: npm run retell:setup)
// Prints the agent_id -> put it in .env as RETELL_AGENT_ID.
//
// How it plugs into the system (see src/comms.ts and src/gateway.ts):
//   send_wave  -> POST /v2/create-phone-call with retell_llm_dynamic_variables = shift + memory
//   the voice agent's prompt uses those {{variables}}
//   after the call Retell POSTs call_analyzed -> /webhooks/retell reads
//   call_analysis.custom_analysis_data.{accepted, counter_rate} + the transcript (memory)

const API = "https://api.retellai.com";
const KEY = process.env.RETELL_API_KEY ?? "proxy-injected";   // a network proxy may inject the real key
const WEBHOOK = process.env.RETELL_WEBHOOK_URL ?? null;         // public URL of /webhooks/retell (e.g. an ngrok tunnel)

async function retell(method: string, path: string, body?: unknown) {
  const res = await fetch(API + path, {
    method,
    headers: { Authorization: `Bearer ${KEY}`, "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}: ${text}`);
  return text ? JSON.parse(text) : null;
}

// The variables ShiftFill's send_wave passes for every call (keep in sync with src/graphs/shiftFill.ts).
const PROMPT = `You are Riley, a friendly scheduling assistant at BrightCare Staffing, calling a nurse about one open shift.

Shift details:
- Nurse: {{nurse_name}}
- Facility: {{facility}}
- When: {{shift_time}} ({{hours}} hours)
- Pay: \${{rate}} per hour. You may agree to at most \${{max_rate}} per hour if the nurse asks for more; never offer more than she asks for, and never go above \${{max_rate}}.
- Offer code: {{offer_code}}
- Facility notes: {{facility_notes}}
- What we already know about this nurse's preferences: {{known_preferences}}

How to run the call:
1. Confirm you're speaking with {{nurse_name}}. If not, apologise and end the call.
2. Describe the shift in one or two sentences and ask if she can take it.
3. If she asks about parking, check-in or requirements, answer from the facility notes. If you don't know, say a coordinator will text her.
4. If she names a rate, repeat it back. If it's at or below \${{max_rate}}, say you'll confirm with the team. If it's above, say the most the facility can pay is \${{max_rate}} and ask whether that works.
5. If she declines, thank her and ask if there's anything about her availability we should remember (for example no nights, or times not to call).
6. Never promise the shift is booked: say she'll get a confirmation text shortly.
7. Keep it under two minutes, warm and professional. Respect the preferences we already know.`;

const ANALYSIS = [
  { type: "boolean", name: "accepted", description: "True only if the nurse clearly agreed to work the shift at the offered rate." },
  { type: "number", name: "counter_rate", description: "The hourly rate in dollars the nurse asked for if she wanted more than offered; leave empty if she did not name a rate.", required: false },
  { type: "string", name: "stated_preferences", description: "Any lasting work preferences the nurse stated (shift times, days, facilities, when not to call). Empty if none.", required: false },
];

const voices: any[] = await retell("GET", "/list-voices");
const voice = voices.find((v) => v.provider === "retell" && v.gender === "female") ?? voices.find((v) => v.gender === "female") ?? voices[0];
console.log(`voice: ${voice.voice_name} (${voice.voice_id}, ${voice.provider})`);

const llm = await retell("POST", "/create-retell-llm", {
  general_prompt: PROMPT,
  begin_message: "Hi, is this {{nurse_name}}? This is Riley from BrightCare Staffing, calling about an open shift.",
  start_speaker: "agent",
  default_dynamic_variables: {                       // used when testing from the Retell dashboard
    nurse_name: "Anita", facility: "Oakridge Memory Care", shift_time: "tomorrow 3pm to 11pm", hours: "8",
    rate: "26", max_rate: "32", offer_code: "1234", facility_notes: "Memory care unit on floor 2.",
    known_preferences: "Prefers calls; does not work night shifts.",
  },
});
console.log(`llm_id: ${llm.llm_id}`);

const agent = await retell("POST", "/create-agent", {
  agent_name: "Vali ShiftFill Caller",
  response_engine: { type: "retell-llm", llm_id: llm.llm_id },
  voice_id: voice.voice_id,
  language: "en-US",
  webhook_url: WEBHOOK,
  post_call_analysis_data: ANALYSIS,
});
console.log(`agent_id: ${agent.agent_id}`);
console.log(`\nAdd to .env:\n  COMMS_PROVIDER=retell\n  RETELL_AGENT_ID=${agent.agent_id}\n  RETELL_FROM_NUMBER=<a number bought in Retell, for real phone calls>`);
console.log(`Test it now in the Retell dashboard: Agents -> "${agent.agent_name ?? "Vali ShiftFill Caller"}" -> Test (web call in your browser, no phone number needed).`);
