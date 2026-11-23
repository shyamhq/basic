// A controllable clock.
//
// Long-running agents are all about TIME: wave timeouts, approval SLAs, "shift
// starts in 3h". Tests must fast-forward without sleeping, so every module
// calls clock.now() instead of new Date().

let frozen: Date | null = null;

export const clock = {
  now: (): Date => (frozen ? new Date(frozen) : new Date()),
  freeze: (at: Date) => { frozen = new Date(at); },
  advanceMinutes: (m: number) => { frozen = new Date(clock.now().getTime() + m * 60_000); return clock.now(); },
  unfreeze: () => { frozen = null; },
};

export const addMinutes = (d: Date, m: number) => new Date(d.getTime() + m * 60_000);
export const hoursBetween = (a: Date, b: Date) => (b.getTime() - a.getTime()) / 3_600_000;
