// Compile every graph with the SAME durable checkpointer (short-term memory, per thread)
// and the SAME long-term memory store.

import { PostgresSaver } from "@langchain/langgraph-checkpoint-postgres";
import { pool } from "../db.js";
import { getMemoryStore } from "../memory.js";
import { buildIntakeGraph } from "./intake.js";
import { buildShiftFillGraph } from "./shiftFill.js";

export const checkpointer = new PostgresSaver(pool);

export const GRAPHS = {
  intake: buildIntakeGraph().compile({ checkpointer, store: getMemoryStore() }),
  shiftfill: buildShiftFillGraph().compile({ checkpointer, store: getMemoryStore() }),
};
export type GraphName = keyof typeof GRAPHS;

export async function setupLangGraphTables() {
  await checkpointer.setup();
  await getMemoryStore().setup();
}
