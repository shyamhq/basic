// Seed a tiny staffing agency: 2 facilities, 8 nurses with profiles, a few shifts.
// Run: npm run seed      (drops and recreates everything)

import { clock } from "./clock.js";
import { initSchema, pool, q } from "./db.js";
import { setupLangGraphTables } from "./graphs/index.js";
import { addFacilityFact, getMemoryStore } from "./memory.js";

export const AGENCY = "ag_bright";

/** Tomorrow at HH:00 UTC (relative to the controllable clock). */
export function tomorrowAt(hour: number) {
  const d = clock.now();
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1, hour));
}
const daysAgo = (n: number, hour: number) => {
  const d = clock.now();
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - n, hour));
};

const NURSES = [
  // id, name, phone, license, certs, specialties, languages, x, y, min_rate, max_dist, shift_prefs, channel, contact_start, hours, reliability, bio
  ["cg_sofia", "Sofia Rossi", "+15550001001", "CNA", ["TB_TEST", "DEMENTIA_CERT"], ["geriatrics"], ["en", "it"], 1, 1, 22, 20, ["day", "evening"], "sms", 8, 16, 0.85, "CNA, 4 yrs in skilled nursing."],
  ["cg_maria", "Maria Lopez", "+15550001002", "CNA", ["TB_TEST", "DEMENTIA_CERT"], ["geriatrics", "dementia"], ["en", "es"], 2, 1, 24, 15, ["day"], "sms", 7, 36, 0.97, "10 yrs geriatric CNA, bilingual EN/ES, loves memory care."],
  ["cg_anita", "Anita Reddy", "+15550001003", "CNA", ["TB_TEST", "DEMENTIA_CERT"], ["dementia"], ["en", "te"], 1, 2, 25, 15, ["day", "evening"], "call", 9, 16, 0.95, "Dementia-certified CNA, prefers calls, speaks Telugu."],
  ["cg_james", "James Okafor", "+15550001004", "CNA", ["TB_TEST"], ["geriatrics"], ["en"], 3, 3, 22, 20, ["day", "night"], "sms", 7, 24, 0.9, "Flexible CNA, picks up last-minute shifts."],
  ["cg_grace", "Grace Lee", "+15550001005", "CNA", ["TB_TEST"], [], ["en", "ko"], 4, 4, 23, 25, ["night"], "sms", 10, 8, 0.88, "Night-owl CNA, new to the agency."],
  ["cg_brian", "Brian Kim", "+15550001006", "CNA", ["TB_TEST"], ["geriatrics"], ["en"], 30, 30, 22, 10, ["day"], "sms", 8, 10, 0.8, "Lives far from both facilities."],
  ["cg_priya", "Priya Shah", "+15550001007", "RN", ["TB_TEST", "DEMENTIA_CERT"], ["wound care"], ["en", "hi"], 5, 0, 45, 20, ["day"], "sms", 8, 20, 0.96, "RN, wound care specialist."],
  ["cg_tom", "Tom Becker", "+15550001008", "CNA", [], ["geriatrics"], ["en"], 2, 2, 21, 20, ["day"], "sms", 8, 12, 0.7, "Missing TB test on file."],
] as const;

export async function seed() {
  await initSchema({ reset: true });
  await setupLangGraphTables();

  await q(`INSERT INTO agencies (id, name) VALUES ($1, 'BrightCare Staffing')`, [AGENCY]);
  await q(`INSERT INTO facilities (id, agency_id, name, x, y, requirements, notes) VALUES
    ('fac_sunrise', $1, 'Sunrise Manor', 0, 0, '{TB_TEST}', 'Park in the back lot; badge pickup at the front desk.'),
    ('fac_oak', $1, 'Oakridge Memory Care', 12, 5, '{TB_TEST,DEMENTIA_CERT}', 'Memory care unit on floor 2.')`, [AGENCY]);

  for (const n of NURSES) {
    await q(
      `INSERT INTO caregivers (id, agency_id, name, phone, license, certs, specialties, languages, x, y, min_rate, max_distance_mi,
         shift_prefs, preferred_channel, contact_start, hours_this_week, reliability, bio)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
      [n[0], AGENCY, n[1], n[2], n[3], n[4], n[5], n[6], n[7], n[8], n[9], n[10], n[11], n[12], n[13], n[14], n[15], n[16]],
    );
  }

  const shifts = [
    // Sofia is scheduled on two shifts tomorrow (she will call out sick in the demo)
    ["sh_sunrise_day", "fac_sunrise", "CNA", "geriatrics", tomorrowAt(7), tomorrowAt(15), 24, 30, "scheduled", "cg_sofia"],
    ["sh_oak_eve", "fac_oak", "CNA", "dementia", tomorrowAt(15), tomorrowAt(23), 26, 32, "scheduled", "cg_sofia"],
    // history: who has worked where (used by the "first shift at facility" learned policy)
    ["sh_hist_1", "fac_sunrise", "CNA", "geriatrics", daysAgo(3, 7), daysAgo(3, 15), 24, 30, "completed", "cg_maria"],
    ["sh_hist_2", "fac_sunrise", "CNA", "geriatrics", daysAgo(5, 7), daysAgo(5, 15), 24, 30, "completed", "cg_james"],
  ];
  for (const s of shifts) {
    await q(`INSERT INTO shifts (id, agency_id, facility_id, role, specialty, starts_at, ends_at, rate, max_rate, status, caregiver_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, [s[0], AGENCY, ...s.slice(1)]);
  }

  // SEMANTIC memory: a fact about a facility, not in the scheduling system.
  await addFacilityFact(AGENCY, "fac_oak", "tb_policy", "Oakridge needs a TB result less than 12 months old at check-in.");
}

/** Post a brand-new open shift (what a facility does through the gateway API). */
export async function createOpenShift(id: string, facilityId: string, start: Date, end: Date, rate: number, maxRate: number, specialty: string | null = null) {
  await q(`INSERT INTO shifts (id, agency_id, facility_id, role, specialty, starts_at, ends_at, rate, max_rate, status)
           VALUES ($1,$2,$3,'CNA',$4,$5,$6,$7,$8,'open')`, [id, AGENCY, facilityId, specialty, start, end, rate, maxRate]);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await seed();
  console.log("Seeded agency", AGENCY);
  await getMemoryStore().stop();
  await pool.end();
}
