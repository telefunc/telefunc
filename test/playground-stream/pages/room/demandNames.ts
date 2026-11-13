export { demandNames }
export type { NamedDemand }

import type { TrackDemand } from '@telefunc/room'

/** An entry of `me.demand`, its watchers by name. */
type NamedDemand = { track: string | null; watchers: unknown[]; watcherCount: number }

/** `me.demand` as a test compares it: each watcher by name. */
function demandNames(me: { readonly demand: readonly TrackDemand[] }): NamedDemand[] {
  return me.demand.map(({ track, watchers, watcherCount }) => ({
    track,
    watchers: watchers.map((watcher) => watcher.meta.name),
    watcherCount,
  }))
}
