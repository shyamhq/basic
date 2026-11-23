// GRAPH VERSIONING. Threads can sit paused at an interrupt for hours, across deploys.
// Each run records the graph version it started on (agent_runs.graph_version). When you make a
// breaking change (rename/remove a node, change state shape), bump the version and keep the old
// compiled graph registered in graphs/index.ts until no paused thread uses it any more.
export const GRAPH_VERSIONS = { intake: 2, shiftfill: 2 } as const;
export type GraphName = keyof typeof GRAPH_VERSIONS;
