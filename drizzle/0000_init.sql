CREATE SCHEMA "app";
--> statement-breakpoint
CREATE TABLE "app"."agencies" (
	"id" text PRIMARY KEY NOT NULL,
	"name" text NOT NULL,
	"phone" text NOT NULL,
	"review_outbound" boolean DEFAULT false NOT NULL,
	CONSTRAINT "agencies_phone_unique" UNIQUE("phone")
);
--> statement-breakpoint
CREATE TABLE "app"."agent_runs" (
	"thread_id" text PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"graph" text NOT NULL,
	"graph_version" integer NOT NULL,
	"subject" text NOT NULL,
	"parent_thread_id" text,
	"status" text DEFAULT 'queued' NOT NULL,
	"waiting_for" text,
	"lease_owner" text,
	"lease_version" integer DEFAULT 0 NOT NULL,
	"heartbeat_at" timestamp with time zone,
	"error" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "app"."approvals" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"thread_id" text NOT NULL,
	"interrupt_id" text NOT NULL,
	"kind" text NOT NULL,
	"payload" jsonb NOT NULL,
	"status" text DEFAULT 'pending' NOT NULL,
	"decision" jsonb,
	"decided_by" text,
	"escalated" boolean DEFAULT false NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
	"decided_at" timestamp with time zone,
	CONSTRAINT "approvals_interrupt_id_unique" UNIQUE("interrupt_id")
);
--> statement-breakpoint
CREATE TABLE "app"."caregivers" (
	"id" text PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"name" text NOT NULL,
	"phone" text NOT NULL,
	"license" text NOT NULL,
	"certs" text[] DEFAULT '{}' NOT NULL,
	"specialties" text[] DEFAULT '{}' NOT NULL,
	"languages" text[] DEFAULT '{}' NOT NULL,
	"bio" text,
	"x" real NOT NULL,
	"y" real NOT NULL,
	"min_rate" real NOT NULL,
	"max_distance_mi" real NOT NULL,
	"shift_prefs" text[] DEFAULT '{}' NOT NULL,
	"preferred_channel" text DEFAULT 'sms' NOT NULL,
	"contact_start" integer DEFAULT 8 NOT NULL,
	"contact_end" integer DEFAULT 21 NOT NULL,
	"hours_this_week" real DEFAULT 0 NOT NULL,
	"reliability" real DEFAULT 0.9 NOT NULL,
	"active" boolean DEFAULT true NOT NULL,
	CONSTRAINT "caregivers_phone_unique" UNIQUE("phone")
);
--> statement-breakpoint
CREATE TABLE "app"."comms_log" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"channel" text NOT NULL,
	"direction" text NOT NULL,
	"phone" text NOT NULL,
	"body" text NOT NULL,
	"meta" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "app"."facilities" (
	"id" text PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"name" text NOT NULL,
	"x" real NOT NULL,
	"y" real NOT NULL,
	"requirements" text[] DEFAULT '{}' NOT NULL,
	"notes" text
);
--> statement-breakpoint
CREATE TABLE "app"."inbound_requests" (
	"idempotency_key" text PRIMARY KEY NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "app"."offers" (
	"code" text PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"thread_id" text NOT NULL,
	"shift_id" text NOT NULL,
	"caregiver_id" text NOT NULL,
	"wave" integer NOT NULL,
	"channel" text NOT NULL,
	"status" text DEFAULT 'sent' NOT NULL,
	"last_event_id" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "offers_thread_id_caregiver_id_unique" UNIQUE("thread_id","caregiver_id")
);
--> statement-breakpoint
CREATE TABLE "app"."outbox" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"topic" text NOT NULL,
	"payload" jsonb NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"published_at" timestamp with time zone
);
--> statement-breakpoint
CREATE TABLE "app"."shifts" (
	"id" text PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"facility_id" text NOT NULL,
	"role" text NOT NULL,
	"specialty" text,
	"starts_at" timestamp with time zone NOT NULL,
	"ends_at" timestamp with time zone NOT NULL,
	"rate" real NOT NULL,
	"max_rate" real NOT NULL,
	"status" text NOT NULL,
	"caregiver_id" text
);
--> statement-breakpoint
CREATE TABLE "app"."thread_inbox" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"thread_id" text NOT NULL,
	"kind" text NOT NULL,
	"payload" jsonb NOT NULL,
	"dedupe_key" text,
	"status" text DEFAULT 'pending' NOT NULL,
	"resumed_interrupt_id" text,
	"note" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "thread_inbox_dedupe_key_unique" UNIQUE("dedupe_key")
);
--> statement-breakpoint
CREATE TABLE "app"."timers" (
	"id" text PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"thread_id" text NOT NULL,
	"kind" text NOT NULL,
	"payload" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"fire_at" timestamp with time zone NOT NULL,
	"fired_at" timestamp with time zone,
	"cancelled" boolean DEFAULT false NOT NULL
);
--> statement-breakpoint
CREATE TABLE "app"."tool_calls" (
	"idempotency_key" text PRIMARY KEY NOT NULL,
	"agency_id" text NOT NULL,
	"thread_id" text,
	"tool" text NOT NULL,
	"args" jsonb NOT NULL,
	"result" jsonb,
	"approved_by" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX "agent_runs_subject" ON "app"."agent_runs" USING btree ("graph","subject");--> statement-breakpoint
CREATE INDEX "inbox_open" ON "app"."thread_inbox" USING btree ("thread_id","id") WHERE status in ('pending','inflight');--> statement-breakpoint
CREATE INDEX "timers_due" ON "app"."timers" USING btree ("fire_at") WHERE fired_at is null and not cancelled;