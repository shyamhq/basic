// Run the system "for real":
//   npm run services            # gateway + workers (+ cron) in one process
//   npm run services worker     # only graphile-worker: thread/memory jobs + cron (scale horizontally)
//   npm run services gateway    # only the HTTP gateway + console on :3000
//
// Everything coordinates through Postgres: the gateway writes mailbox rows + jobs in one transaction,
// any number of worker processes pick jobs up (one at a time per thread, thanks to named queues).

import { serve } from "@hono/node-server";
import { pool } from "./db/index.js";
import { app } from "./gateway.js";
import { log } from "./log.js";
import { getMemoryStore } from "./memory.js";
import { setupAll } from "./seed.js";
import { startWorker } from "./tasks.js";

const role = process.argv[2] ?? "all";
await setupAll();

const runner = role === "all" || role === "worker" ? await startWorker(10) : null;
if (runner) log.info("worker: thread_kick + memory jobs, cron: timers_tick (1m), consolidate_all (1h), checkpoint_retention (daily)");

if (role === "all" || role === "gateway") {
  const port = Number(process.env.PORT ?? 3000);
  serve({ fetch: app.fetch, port });
  log.info(`gateway: http://localhost:${port}`);
}

process.on("SIGINT", async () => {
  await runner?.stop();             // graceful: finish in-flight jobs, release locks
  await getMemoryStore().stop();
  await pool.end();
  process.exit(0);
});
