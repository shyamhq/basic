-- Things Drizzle can't express in schema.ts live in this hand-written migration.

CREATE EXTENSION IF NOT EXISTS vector;       -- pgvector, used by LangGraph's PostgresStore (long-term memory)
--> statement-breakpoint
CREATE EXTENSION IF NOT EXISTS btree_gist;   -- lets an EXCLUDE constraint mix "=" (text) and "&&" (ranges)
--> statement-breakpoint
-- INVARIANT: a caregiver can never hold two scheduled/filled shifts that overlap in time.
-- Enforced by the database, not by the agent: two ShiftFill threads racing to book the same
-- nurse get a 23P01 exclusion_violation on the loser, which the tool gateway maps to ToolConflict.
ALTER TABLE "app"."shifts" ADD CONSTRAINT "no_overlapping_bookings"
  EXCLUDE USING gist (caregiver_id WITH =, tstzrange(starts_at, ends_at) WITH &&)
  WHERE (status IN ('scheduled', 'filled') AND caregiver_id IS NOT NULL);
