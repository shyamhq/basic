// Small helpers shared by both graphs.

import { createHash } from "node:crypto";
import { Annotation } from "@langchain/langgraph";

/** A state channel that APPENDS instead of overwriting (reducer). Needed for parallel
 *  branches (Send) writing to the same key, and for an append-only timeline. */
export const appendList = <T>() =>
  Annotation<T[]>({ reducer: (a, b) => a.concat(b), default: () => [] });

/** What the worker passes into interrupt() resumes: the mailbox event. */
export type MailboxEvent = { id: string; kind: string; payload: any };

/** Every interrupt payload declares which mailbox events can resume it. */
export type WaitPayload = { type: string; accepts: string[]; [k: string]: unknown };

export const shiftTimeOf = (startsAt: string | Date): "day" | "evening" | "night" => {
  const h = new Date(startsAt).getUTCHours();   // the mock keeps everything in UTC
  return h >= 7 && h < 15 ? "day" : h >= 15 && h < 23 ? "evening" : "night";
};

/** Deterministic 4-digit code so a node re-run produces the SAME code (replay-safe). */
export const offerCodeFor = (threadId: string, caregiverId: string) =>
  String(1000 + (createHash("sha1").update(`${threadId}:${caregiverId}`).digest().readUInt32BE(0) % 9000));

export const fmtTime = (d: string | Date) =>
  new Date(d).toISOString().replace("T", " ").slice(0, 16) + " UTC";
