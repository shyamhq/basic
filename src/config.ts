// All knobs in one place. Everything can be overridden with env vars.

const env = process.env;

export const DATABASE_URL = env.DATABASE_URL ?? "postgresql://postgres:postgres@localhost:5432/vali";
export const RABBITMQ_URL = env.RABBITMQ_URL ?? "amqp://guest:guest@localhost";

// LLM: without a key we use deterministic heuristics, so the whole system and
// every test runs offline. With ANTHROPIC_API_KEY set, Claude is used.
export const USE_REAL_LLM = Boolean(env.ANTHROPIC_API_KEY);
export const LLM_MODEL = env.VALI_LLM_MODEL ?? "claude-opus-5-5";

// Voice/SMS provider: "simulator" (default, free, offline) or "retell".
export const COMMS_PROVIDER = env.COMMS_PROVIDER ?? "simulator";

// Shift-fill agent business rules
export const WAVE_SIZE = Number(env.VALI_WAVE_SIZE ?? 2);            // nurses contacted per wave
export const MAX_WAVES = Number(env.VALI_MAX_WAVES ?? 2);            // then hand off to a human
export const WAVE_TIMEOUT_MIN = Number(env.VALI_WAVE_TIMEOUT_MIN ?? 15);
export const URGENT_HOURS = 4;                                       // shift starts sooner -> call instead of SMS
export const OVERTIME_HOURS = 40;
export const TRIAGE_MIN_CONFIDENCE = 0.6;                            // below -> human review
export const APPROVAL_SLA_MIN = 10;                                  // unanswered approval -> page a manager

// Worker reliability
export const MAX_JOB_ATTEMPTS = 5;      // then -> dead-letter queue
export const RETRY_DELAY_MS = 2000;     // RabbitMQ delay-queue TTL
export const LEASE_SECONDS = 60;        // "running" thread with an older heartbeat is presumed dead

export const MANAGER_PHONE = "+15550000001";   // on-call staffing manager
