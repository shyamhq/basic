// Records a captioned screen video of the REAL system: services (gateway + graphile workers) running,
// a headless browser driving the operations console like a person would.
//   npx tsx scripts/record-demo.ts [outDir]
// Produces <outDir>/vali-demo.mp4 (needs ffmpeg) and the raw .webm.

import { execFileSync, spawn } from "node:child_process";
import { mkdirSync, readdirSync, renameSync } from "node:fs";
import { join } from "node:path";
import { and, eq } from "drizzle-orm";
import { createRequire } from "node:module";
import type { Page } from "playwright";
// Playwright is often installed globally (not in package.json) -> resolve it the CommonJS way (honors NODE_PATH).
const { chromium } = createRequire(import.meta.url)("playwright") as typeof import("playwright");
import { db, pool } from "../src/db/index.js";
import { offers } from "../src/db/schema.js";
import { getMemoryStore } from "../src/memory.js";
import { seed, tomorrowAt } from "../src/seed.js";

const OUT = process.argv[2] ?? "media";
const URL = "http://localhost:3000";
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

mkdirSync(OUT, { recursive: true });
await seed();
const services = spawn("npx", ["tsx", "src/services.ts"], { stdio: "ignore", detached: true, env: { ...process.env, LOG_LEVEL: "warn" } });
for (let i = 0; i < 60; i++) { try { await fetch(URL); break; } catch { await sleep(500); } }

const code = async (threadId: string, caregiverId: string) => {
  for (let i = 0; i < 40; i++) {
    const [o] = await db.select().from(offers).where(and(eq(offers.threadId, threadId), eq(offers.caregiverId, caregiverId)));
    if (o) return o.code;
    await sleep(250);
  }
  throw new Error(`no offer for ${caregiverId} on ${threadId}`);
};

const browser = await chromium.launch();
const context = await browser.newContext({ viewport: { width: 1280, height: 860 }, recordVideo: { dir: OUT, size: { width: 1280, height: 860 } } });
const page = await context.newPage();
await page.goto(URL);

async function caption(p: Page, text: string, holdMs = 2500) {
  await p.evaluate((t) => { const c = document.getElementById("caption")!; c.style.display = "block"; c.textContent = t; }, text);
  await sleep(holdMs);
}
async function text(p: Page, phone: string, body: string) {
  await p.selectOption("#from", phone);
  await p.click("#body");
  await p.keyboard.type(body, { delay: 35 });
  await sleep(400);
  await p.click("#send");
}
const openThread = (p: Page, id: string) => p.click(`.run:has-text("${id}")`);

await caption(page, "Vali mock: AI agents that fill open nursing shifts (real services, real Postgres)", 3500);

await caption(page, "1 · Sofia texts that she's sick", 1200);
await text(page, "+15550001001", "Hi, I'm sick with a fever and can't make my shifts tomorrow");
await sleep(3500);
await openThread(page, "intake:");
await caption(page, "2 · Intake agent: triage -> call-out -> spawns one ShiftFill agent per shift (fan-out)", 4500);

await openThread(page, "fill:sh_sunrise_day");
await caption(page, "3 · ShiftFill: SQL hard filters -> memory -> parallel scoring -> texts the top 2 nurses -> PAUSES (interrupt)", 6000);

await caption(page, "4 · James replies YES: no risk -> auto-approved by policy -> booked, other offers withdrawn", 1200);
await text(page, "+15550001004", `YES ${await code("fill:sh_sunrise_day", "cg_james")}`);
await sleep(3500);
await openThread(page, "fill:sh_sunrise_day");
await sleep(3500);

await caption(page, "5 · Evening shift: Anita declines and says 'no nights anymore' (memory); Maria counters at $30", 1200);
await openThread(page, "fill:sh_oak_eve");
await text(page, "+15550001003", `No ${await code("fill:sh_oak_eve", "cg_anita")}, and I don't do nights anymore`);
await sleep(1500);
await text(page, "+15550001002", `I can do it for $30 ${await code("fill:sh_oak_eve", "cg_maria")}`);
await sleep(3500);
await caption(page, "6 · Overtime + above the posted rate -> the agent pauses for a HUMAN (approvals inbox)", 4000);

await caption(page, "7 · The coordinator edits the rate to $29 and approves -> booked -> supervisor's fan-in completes", 1200);
await page.fill("input[id^=rate-]", "29");
await sleep(600);
await page.click("button:has-text('Edit rate')");
await sleep(3500);
await openThread(page, "fill:sh_oak_eve");
await sleep(2500);
await openThread(page, "intake:");
await sleep(3500);

await caption(page, "8 · A facility posts a NIGHT shift -> long-term memory excludes Anita before anyone is texted", 1200);
const start = tomorrowAt(23);
await page.evaluate(async (b) => {
  await fetch("/api/shifts", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(b) });
}, { id: "sh_oak_night", facilityId: "fac_oak", startsAt: start.toISOString(), endsAt: new Date(start.getTime() + 8 * 3_600_000).toISOString(), rate: 28, maxRate: 34, specialty: "dementia" });
await sleep(3000);
await openThread(page, "fill:sh_oak_night");
await sleep(5000);

await caption(page, "Durable: checkpoints, mailbox, jobs and approvals all live in Postgres. Any process can crash and resume.", 4500);

await context.close();
await browser.close();
process.kill(-services.pid!, "SIGINT");   // the whole process group (npx -> tsx -> node)

const webm = readdirSync(OUT).filter((f) => f.endsWith(".webm")).map((f) => join(OUT, f)).sort().pop()!;
renameSync(webm, join(OUT, "vali-demo.webm"));
execFileSync("ffmpeg", ["-y", "-loglevel", "error", "-i", join(OUT, "vali-demo.webm"), "-c:v", "libx264", "-pix_fmt", "yuv420p", "-movflags", "+faststart", join(OUT, "vali-demo.mp4")]);
console.log(`recorded ${join(OUT, "vali-demo.mp4")}`);
await getMemoryStore().stop();
await pool.end();
process.exit(0);
