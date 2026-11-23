// Seed a tiny staffing agency: 2 facilities, 8 nurse profiles, a few shifts.
//   npm run seed      (migrates everything, wipes data, seeds)

import { runMigrations } from "graphile-worker";
import { clock } from "./clock.js";
import { db, migrateApp, pool, truncateAll } from "./db/index.js";
import { agencies, caregivers, facilities, shifts } from "./db/schema.js";
import { setupLangGraphTables } from "./graphs/index.js";
import { addFacilityFact, getMemoryStore } from "./memory.js";

export const AGENCY = "ag_bright";
export const AGENCY_PHONE = "+15550009999";   // the number nurses text; identifies the tenant

/** Tomorrow at HH:00 UTC (relative to the controllable clock). */
export function tomorrowAt(hour: number) {
  const d = clock.now();
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1, hour));
}
const daysAgo = (n: number, hour: number) => {
  const d = clock.now();
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - n, hour));
};

/** Three owners migrate their own tables: Drizzle (app.*), graphile-worker, LangGraph. */
let migrated = false;
export async function setupAll() {
  if (migrated) return;
  await migrateApp();
  await runMigrations({ pgPool: pool });
  await setupLangGraphTables();
  migrated = true;
}

type Nurse = typeof caregivers.$inferInsert;
const nurse = (id: string, name: string, phone: string, license: string, certs: string[], specialties: string[], languages: string[],
  x: number, y: number, minRate: number, maxDistanceMi: number, shiftPrefs: string[], preferredChannel: string, contactStart: number,
  hoursThisWeek: number, reliability: number, bio: string): Nurse =>
  ({ id, agencyId: AGENCY, name, phone, license, certs, specialties, languages, x, y, minRate, maxDistanceMi, shiftPrefs, preferredChannel, contactStart, hoursThisWeek, reliability, bio });

export async function seed() {
  await setupAll();
  await truncateAll();

  await db.insert(agencies).values({ id: AGENCY, name: "BrightCare Staffing", phone: AGENCY_PHONE });
  await db.insert(facilities).values([
    { id: "fac_sunrise", agencyId: AGENCY, name: "Sunrise Manor", x: 0, y: 0, requirements: ["TB_TEST"], notes: "Park in the back lot; badge pickup at the front desk." },
    { id: "fac_oak", agencyId: AGENCY, name: "Oakridge Memory Care", x: 12, y: 5, requirements: ["TB_TEST", "DEMENTIA_CERT"], notes: "Memory care unit on floor 2." },
  ]);
  await db.insert(caregivers).values([
    nurse("cg_sofia", "Sofia Rossi", "+15550001001", "CNA", ["TB_TEST", "DEMENTIA_CERT"], ["geriatrics"], ["en", "it"], 1, 1, 22, 20, ["day", "evening"], "sms", 8, 16, 0.85, "CNA, 4 yrs in skilled nursing."),
    nurse("cg_maria", "Maria Lopez", "+15550001002", "CNA", ["TB_TEST", "DEMENTIA_CERT"], ["geriatrics", "dementia"], ["en", "es"], 2, 1, 24, 15, ["day"], "sms", 7, 36, 0.97, "10 yrs geriatric CNA, bilingual EN/ES, loves memory care."),
    nurse("cg_anita", "Anita Reddy", "+15550001003", "CNA", ["TB_TEST", "DEMENTIA_CERT"], ["dementia"], ["en", "te"], 1, 2, 25, 15, ["day", "evening"], "call", 9, 16, 0.95, "Dementia-certified CNA, prefers calls, speaks Telugu."),
    nurse("cg_james", "James Okafor", "+15550001004", "CNA", ["TB_TEST"], ["geriatrics"], ["en"], 3, 3, 22, 20, ["day", "night"], "sms", 7, 24, 0.9, "Flexible CNA, picks up last-minute shifts."),
    nurse("cg_grace", "Grace Lee", "+15550001005", "CNA", ["TB_TEST"], [], ["en", "ko"], 4, 4, 23, 25, ["night"], "sms", 10, 8, 0.88, "Night-owl CNA, new to the agency."),
    nurse("cg_brian", "Brian Kim", "+15550001006", "CNA", ["TB_TEST"], ["geriatrics"], ["en"], 30, 30, 22, 10, ["day"], "sms", 8, 10, 0.8, "Lives far from both facilities."),
    nurse("cg_priya", "Priya Shah", "+15550001007", "RN", ["TB_TEST", "DEMENTIA_CERT"], ["wound care"], ["en", "hi"], 5, 0, 45, 20, ["day"], "sms", 8, 20, 0.96, "RN, wound care specialist."),
    nurse("cg_tom", "Tom Becker", "+15550001008", "CNA", [], ["geriatrics"], ["en"], 2, 2, 21, 20, ["day"], "sms", 8, 12, 0.7, "Missing TB test on file."),
  ]);
  const shift = (id: string, facilityId: string, specialty: string, startsAt: Date, endsAt: Date, rate: number, maxRate: number,
    status: "scheduled" | "completed", caregiverId: string) =>
    ({ id, agencyId: AGENCY, facilityId, role: "CNA", specialty, startsAt, endsAt, rate, maxRate, status, caregiverId });
  await db.insert(shifts).values([
    // Sofia is scheduled on two shifts tomorrow (she calls out sick in the demo)
    shift("sh_sunrise_day", "fac_sunrise", "geriatrics", tomorrowAt(7), tomorrowAt(15), 24, 30, "scheduled", "cg_sofia"),
    shift("sh_oak_eve", "fac_oak", "dementia", tomorrowAt(15), tomorrowAt(23), 26, 32, "scheduled", "cg_sofia"),
    // history: who has worked where (used by the learned "first shift at facility" policy)
    shift("sh_hist_1", "fac_sunrise", "geriatrics", daysAgo(3, 7), daysAgo(3, 15), 24, 30, "completed", "cg_maria"),
    shift("sh_hist_2", "fac_sunrise", "geriatrics", daysAgo(5, 7), daysAgo(5, 15), 24, 30, "completed", "cg_james"),
  ]);
  // SEMANTIC memory: a fact about a facility that isn't in the scheduling system.
  await addFacilityFact(AGENCY, "fac_oak", "tb_policy", "Oakridge needs a TB result less than 12 months old at check-in.");
}

if (import.meta.url === `file://${process.argv[1]}`) {
  await seed();
  console.log("Seeded agency", AGENCY);
  await getMemoryStore().stop();
  await pool.end();
}
