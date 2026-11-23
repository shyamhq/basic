// Compile every graph with the SAME durable checkpointer (short-term memory, per thread) and the
// SAME long-term memory store. Graphs are registered per VERSION so paused threads keep running
// the code they started on (see versions.ts).

import { PostgresSaver } from "@langchain/langgraph-checkpoint-postgres";
import { pool } from "../db/index.js";
import { getMemoryStore } from "../memory.js";
import { buildIntakeGraph } from "./intake.js";
import { buildShiftFillGraph } from "./shiftFill.js";
import type { GraphName } from "./versions.js";

export const checkpointer = new PostgresSaver(pool);

const compile = (g: ReturnType<typeof buildIntakeGraph> | ReturnType<typeof buildShiftFillGraph>) =>
  g.compile({ checkpointer, store: getMemoryStore() });

const REGISTRY: Record<GraphName, Record<number, ReturnType<typeof compile>>> = {
  intake: { 2: compile(buildIntakeGraph()) },
  shiftfill: { 2: compile(buildShiftFillGraph()) },
};

export function getGraph(name: string, version: number) {
  const g = REGISTRY[name as GraphName]?.[version];
  if (!g) throw new Error(`no graph ${name}@v${version} registered - a paused thread needs it`);   // alert-worthy
  return g;
}

export async function setupLangGraphTables() {
  await checkpointer.setup();
  await getMemoryStore().setup();
}
