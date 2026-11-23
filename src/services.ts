// Run the whole system "for real": RabbitMQ consumers + outbox relay + scheduler + HTTP gateway.
//
//   npm run services            # everything in one process (simplest)
//   npm run services worker     # only agent + memory workers (scale these horizontally)
//   npm run services relay      # only the outbox relay
//   npm run services scheduler  # only timers / SLA / reaper / consolidation
//   npm run services gateway    # only the HTTP gateway + UI on :3000
//
// In production each role is its own deployment; they share nothing but Postgres and RabbitMQ.

import { serve } from "@hono/node-server";
import { RabbitBus } from "./bus.js";
import { pool } from "./db.js";
import { app } from "./gateway.js";
import { setupLangGraphTables } from "./graphs/index.js";
import { getMemoryStore } from "./memory.js";
import { enqueue, relayOnce } from "./outbox.js";
import { dispatch } from "./runtime.js";
import { AGENCY } from "./seed.js";
import { escalateStaleApprovals, fireDueTimers, reapDeadRuns } from "./timers.js";
import { onDeadJob } from "./worker.js";

const role = process.argv[2] ?? "all";
const want = (r: string) => role === "all" || role === r;
const loops: NodeJS.Timeout[] = [];

/** setInterval that never overlaps itself and never crashes the process. */
function every(ms: number, name: string, fn: () => Promise<unknown>) {
  let busy = false;
  loops.push(setInterval(async () => {
    if (busy) return;
    busy = true;
    try { await fn(); } catch (e) { console.error(`[${name}]`, e); } finally { busy = false; }
  }, ms));
}

await setupLangGraphTables();
const bus = new RabbitBus();
await bus.setup(5);

if (want("worker")) {
  await bus.consume("agent.jobs", dispatch, onDeadJob);
  await bus.consume("memory.jobs", dispatch, onDeadJob);
  console.log("[worker] consuming agent.jobs + memory.jobs");
}
if (want("relay")) {
  every(200, "relay", () => relayOnce(bus));                       // could also use LISTEN/NOTIFY to cut latency
  console.log("[relay] outbox -> RabbitMQ every 200ms");
}
if (want("scheduler")) {
  every(1000, "timers", async () => { await fireDueTimers(); await escalateStaleApprovals(); await reapDeadRuns(); });
  every(60 * 60 * 1000, "consolidate", () => enqueue("memory.consolidate", { agencyId: AGENCY }));
  console.log("[scheduler] timers / approval SLA / reaper every 1s, memory consolidation hourly");
}
if (want("gateway")) {
  serve({ fetch: app.fetch, port: Number(process.env.PORT ?? 3000) });
  console.log(`[gateway] http://localhost:${process.env.PORT ?? 3000}`);
}

process.on("SIGINT", async () => {
  loops.forEach(clearInterval);
  await bus.close();
  await getMemoryStore().stop();
  await pool.end();
  process.exit(0);
});
