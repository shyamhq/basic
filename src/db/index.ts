// One pg.Pool shared by Drizzle (our tables), LangGraph's PostgresSaver (checkpoints) and
// graphile-worker (jobs). Same database, so "change state + enqueue the next job" is ONE transaction.

import { drizzle } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";
import { sql } from "drizzle-orm";
import pg from "pg";
import { DATABASE_URL } from "../config.js";
import * as schema from "./schema.js";

export const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 20 });
export const db = drizzle({ client: pool, schema, casing: "snake_case" });

/** Either the root db or a transaction - every helper accepts both so callers can compose transactions. */
export type Db = typeof db;
export type Tx = Parameters<Parameters<Db["transaction"]>[0]>[0];
export type DbOrTx = Db | Tx;

export { schema, sql };

/** Apply Drizzle migrations from ./drizzle (generated SQL + the hand-written invariants file). */
export async function migrateApp() {
  await migrate(db, { migrationsFolder: new URL("../../drizzle", import.meta.url).pathname });
}

/** Tests/demo: wipe data but keep tables (LangGraph and graphile cache "already migrated"). */
export async function truncateAll() {
  await db.execute(sql`
    DO $$ DECLARE t text; BEGIN
      FOR t IN SELECT format('%I.%I', schemaname, tablename) FROM pg_tables
               WHERE schemaname = 'app'
                  OR (schemaname = 'public' AND tablename IN ('checkpoints','checkpoint_blobs','checkpoint_writes','store','store_vectors'))
                  OR (schemaname = 'graphile_worker' AND tablename IN ('_private_jobs','_private_job_queues'))
      LOOP EXECUTE 'TRUNCATE ' || t || ' RESTART IDENTITY CASCADE'; END LOOP;
    END $$;`);
}

pool.on("error", (e) => console.error("[pg pool]", e.message));   // idle-client errors must not crash the process
pool.on("connect", (client) => client.on("error", (e) => console.error("[pg client]", e.message)));
