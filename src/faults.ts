// Fault injection for crash-recovery tests and the demo.
// maybeCrash("after_book") throws ONCE if that point was armed -> simulates the process dying mid-node.

export class SimulatedCrash extends Error {}
export const faults = { crashAt: new Set<string>() };

export function maybeCrash(point: string) {
  if (faults.crashAt.delete(point)) throw new SimulatedCrash(`simulated crash at ${point}`);
}
