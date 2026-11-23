// LONG-TERM MEMORY on LangGraph's PostgresStore (+ pgvector).
//
// Memory taxonomy (CoALA paper; used by LangMem, Mastra, Letta and others):
//
//   WORKING / SHORT-TERM  graph state of ONE thread, saved by the checkpointer   (see graphs/*)
//   PROFILE               structured nurse bio/licenses/rates - the `caregivers` table (source of truth, staff-editable)
//   PREFERENCE            learned per-nurse rules: "no nights", "don't call before 9"         ns: [agency, A, caregiver, C, prefs]
//   EPISODIC              what happened: "declined Sunrise night shift, too far" (vector-searchable) ns: [agency, A, episodes]
//   SEMANTIC              facts about the world: "Oakridge requires a TB test"                 ns: [agency, A, facility, F, facts]
//   PROCEDURAL            how the agent should behave, learned from human corrections          ns: [agency, A, procedures]
//
// WRITE PATH: in the background (memory.jobs queue) so the hot path never waits on extraction.
// READ PATH:  recallForShift() before ranking + before every outreach (personalisation).
// CONSOLIDATION: a periodic job turns repeated episodes into (soft) preferences and forgets old episodes.

import { createHash, randomUUID } from "node:crypto";
import { PostgresStore } from "@langchain/langgraph-checkpoint-postgres/store";
import { clock } from "./clock.js";
import { DATABASE_URL } from "./config.js";
import { extractPreferences, reflect, type Pref } from "./llm.js";

// ---------------------------------------------------------------- embeddings
// Offline "hashing trick" embedding so everything runs without an API.
// Swap for a real model (Voyage, OpenAI, a local bge model) in production -
// the store only needs `embed(texts) -> number[][]`.
const DIMS = 256;
export async function embed(texts: string[]): Promise<number[][]> {
  return texts.map((t) => {
    const v = new Array(DIMS).fill(0);
    for (const word of t.toLowerCase().match(/[a-z0-9]+/g) ?? []) {
      const h = createHash("md5").update(word).digest();
      v[h.readUInt16LE(0) % DIMS] += h[2] % 2 ? 1 : -1;
    }
    const norm = Math.hypot(...v) || 1;
    return v.map((x) => x / norm);
  });
}

// ---------------------------------------------------------------- the store
let store: PostgresStore | null = null;
export function getMemoryStore(): PostgresStore {
  store ??= new PostgresStore({
    connectionOptions: DATABASE_URL,
    index: { dims: DIMS, embed, fields: ["text"] },   // only the "text" field is embedded
  });
  return store;
}

// Gotcha: the JS PostgresStore rejects namespace labels containing SQL LIKE wildcards
// ('%' or '_'), so ids like "cg_maria" are encoded as "cg-maria" inside namespaces.
const L = (id: string) => id.replace(/_/g, "-");
const ns = {
  prefs: (a: string, c: string) => ["agency", L(a), "caregiver", L(c), "prefs"],
  episodes: (a: string) => ["agency", L(a), "episodes"],
  facts: (a: string, f: string) => ["agency", L(a), "facility", L(f), "facts"],
  procedures: (a: string) => ["agency", L(a), "procedures"],
};

// ---------------------------------------------------------------- PREFERENCES
export type StoredPref = Pref & { source: string; updatedAt: string; previous?: unknown };

// Who wins in a conflict? What the nurse SAID or staff entered beats what we INFERRED.
const AUTHORITY: Record<string, number> = { inferred: 1, nurse_stated: 2, staff: 3 };
const SINGLE_VALUED = new Set(["channel", "contact_after_hour", "min_rate"]);
const prefKey = (p: Pref) => (SINGLE_VALUED.has(p.type) ? p.type : `${p.type}:${p.value}`);

export async function upsertPreference(agencyId: string, caregiverId: string, pref: Pref, source: string) {
  const s = getMemoryStore();
  const key = prefKey(pref);
  const existing = (await s.get(ns.prefs(agencyId, caregiverId), key))?.value as StoredPref | undefined;
  if (existing && AUTHORITY[existing.source] > AUTHORITY[source]) return "kept_existing"; // inference can't override the nurse
  const value: StoredPref = { ...pref, source, updatedAt: clock.now().toISOString(), previous: existing ? { value: existing.value, source: existing.source } : undefined };
  await s.put(ns.prefs(agencyId, caregiverId), key, value as any);
  return existing ? "updated" : "created";
}

export async function getPreferences(agencyId: string, caregiverId: string): Promise<StoredPref[]> {
  const items = await getMemoryStore().search(ns.prefs(agencyId, caregiverId), { limit: 50 });
  return items.map((i) => i.value as StoredPref);
}

// ---------------------------------------------------------------- EPISODES
export type Episode = {
  text: string; caregiverId: string; facilityId: string; shiftId: string;
  signal: "accepted" | "declined" | "countered" | "no_response" | "assigned" | "no_show";
  shiftTime: string; at: string;
};

export async function addEpisode(agencyId: string, ep: Omit<Episode, "at"> & { at?: string }, key = randomUUID()) {
  await getMemoryStore().put(ns.episodes(agencyId), key, { ...ep, at: ep.at ?? clock.now().toISOString() });
}

/** Semantic search over episodes, optionally filtered on exact fields (e.g. caregiverId). */
export async function searchEpisodes(agencyId: string, query: string, filter: Record<string, string> = {}, limit = 5) {
  const items = await getMemoryStore().search(ns.episodes(agencyId), { query, filter, limit });
  return items.map((i) => ({ ...(i.value as Episode), score: i.score }));
}

// ---------------------------------------------------------------- SEMANTIC FACTS
export async function addFacilityFact(agencyId: string, facilityId: string, key: string, text: string) {
  await getMemoryStore().put(ns.facts(agencyId, facilityId), key, { text });
}
export async function getFacilityFacts(agencyId: string, facilityId: string): Promise<string[]> {
  return (await getMemoryStore().search(ns.facts(agencyId, facilityId), { limit: 20 })).map((i) => String(i.value.text));
}

// ---------------------------------------------------------------- PROCEDURES (learned playbook)
export type Procedure = { text: string; rule: { type: "require_approval"; when: string } | null; source: string };

export async function addProcedure(agencyId: string, p: Procedure) {
  const key = p.rule ? `${p.rule.type}:${p.rule.when}` : createHash("md5").update(p.text).digest("hex").slice(0, 12);
  await getMemoryStore().put(ns.procedures(agencyId), key, p as any);
}
export async function getProcedures(agencyId: string): Promise<Procedure[]> {
  return (await getMemoryStore().search(ns.procedures(agencyId), { limit: 50 })).map((i) => i.value as Procedure);
}

// ---------------------------------------------------------------- READ PATH used by the agent
export type CandidateMemory = { prefs: StoredPref[]; episodes: (Episode & { score?: number })[] };

export async function recallForShift(agencyId: string, shift: { facility_id: string; shiftTime: string }, caregiverIds: string[]) {
  const perCaregiver: Record<string, CandidateMemory> = {};
  for (const id of caregiverIds) {
    perCaregiver[id] = {
      prefs: await getPreferences(agencyId, id),
      episodes: await searchEpisodes(agencyId, `${shift.facility_id} ${shift.shiftTime} shift`, { caregiverId: id }, 5),
    };
  }
  return {
    perCaregiver,
    procedures: await getProcedures(agencyId),
    facilityFacts: await getFacilityFacts(agencyId, shift.facility_id),
  };
}

// ---------------------------------------------------------------- CONSOLIDATION (periodic, "sleep")
// Episodic -> preference promotion + forgetting. Deterministic on purpose: easy to test and explain.
// Promotions are NOT written here: each becomes a memory_upsert_pref job in that caregiver's queue,
// so it can't race with an extraction for the same nurse (read-modify-write in upsertPreference).
export async function consolidate(agencyId: string, retentionDays = 90) {
  const s = getMemoryStore();
  const cutoff = clock.now().getTime() - retentionDays * 86_400_000;
  const counts = new Map<string, number>();
  const forget: string[] = [];
  let scanned = 0;
  for (let offset = 0; ; offset += 500) {                       // page through ALL episodes
    const page = await s.search(ns.episodes(agencyId), { limit: 500, offset });
    if (!page.length) break;
    for (const item of page) {
      scanned++;
      const ep = item.value as Episode;
      if (new Date(ep.at).getTime() < cutoff) { forget.push(item.key); continue; }
      if (ep.signal === "declined" || ep.signal === "no_response") bump(counts, `${ep.caregiverId}|avoid_shift_time|${ep.shiftTime}`);
      if (ep.signal === "assigned") bump(counts, `${ep.caregiverId}|prefer_facility|${ep.facilityId}`);
    }
  }
  for (const key of forget) await s.delete(ns.episodes(agencyId), key);   // delete after paging, not during
  const promotions: { caregiverId: string; pref: Pref }[] = [];
  for (const [k, n] of counts) {
    if (n < 2) continue;                                        // a pattern needs at least 2 observations
    const [caregiverId, type, value] = k.split("|");
    const text = type === "avoid_shift_time" ? `Usually turns down ${value} shifts (${n} times)` : `Often works at ${value} (${n} shifts)`;
    promotions.push({ caregiverId, pref: { type: type as Pref["type"], value, strength: "soft", text } });
  }
  return { scanned, forgotten: forget.length, promotions };
}
const bump = (m: Map<string, number>, k: string) => m.set(k, (m.get(k) ?? 0) + 1);

// ---------------------------------------------------------------- memory jobs (background WRITE PATH)
// Each runs as a graphile-worker task in queue "memory:<caregiverId>" -> one writer per nurse.
export const memoryTasks = {
  /** free text or a call transcript from a nurse -> preferences */
  memory_extract: async (p: any) => {
    const prefs = await extractPreferences(p.text, { facilityId: p.facilityId });
    for (const pref of prefs) await upsertPreference(p.agencyId, p.caregiverId, pref, p.source ?? "nurse_stated");
  },
  /** idempotent: the key comes from the producer */
  memory_episode: async (p: any) => addEpisode(p.agencyId, p.episode, p.key),
  /** a human overrode the agent -> learn a playbook rule */
  memory_reflect: async (p: any) => {
    const r = await reflect({ decision: p.decision, reason: p.reason, context: p.context });
    await addProcedure(p.agencyId, { ...r, source: `approval:${p.approvalId}` });
  },
  memory_upsert_pref: async (p: any) => void (await upsertPreference(p.agencyId, p.caregiverId, p.pref, p.source)),
};
