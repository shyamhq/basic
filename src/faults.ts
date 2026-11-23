// Fault injection for crash-recovery tests and the demo.
//   crashAt: maybeCrash(point) throws ONCE when armed  -> "the process died here"
//   always:  maybeCrash(point) throws EVERY time       -> a poison job that exhausts its retries

export class SimulatedCrash extends Error {}
export const faults = { crashAt: new Set<string>(), always: new Set<string>() };

export function maybeCrash(point: string) {
  if (faults.always.has(point) || faults.crashAt.delete(point)) throw new SimulatedCrash(`simulated crash at ${point}`);
}
