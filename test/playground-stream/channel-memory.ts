export { watchChannelMemory }
export type { ChannelMemoryPeak }

/** The most the server's channel memory held since `watchChannelMemory()`, and held with what waits for room, as its
 *  own ledger counts them, with the `maxMemory` it holds within. */
type ChannelMemoryPeak = { held: number; heldWithWaiting: number; maxMemory: number }

/** The ledger Telefunc keeps on `globalThis._telefunc` (`channelMemory.ts`): what it holds, what waits for room, and
 *  its `maxMemory`, once a channel resolved it. */
type Ledger = { total: number; reserved: number; maxMemory: number }

const peaks = { held: 0, heldWithWaiting: 0 }

/** From now on, records the most the ledger counts: read it with the function returned. */
function watchChannelMemory(): () => ChannelMemoryPeak {
  const ledger = channelMemoryLedger()
  if (!Reflect.has(globalThis, WATCHED)) {
    Reflect.set(globalThis, WATCHED, true)
    recordEachWrite(ledger)
  }
  peaks.held = ledger.total
  peaks.heldWithWaiting = ledger.total + ledger.reserved
  return () => ({ ...peaks, maxMemory: ledger.maxMemory })
}

const WATCHED = Symbol.for('playground-stream:channelMemoryWatched')

/** Each write of the ledger's `total` and `reserved` goes through, and raises the peaks it reaches. */
function recordEachWrite(ledger: Ledger): void {
  const counts = { total: ledger.total, reserved: ledger.reserved }
  for (const key of ['total', 'reserved'] as const) {
    Object.defineProperty(ledger, key, {
      configurable: true,
      enumerable: true,
      get: () => counts[key],
      set: (value: number) => {
        counts[key] = value
        peaks.held = Math.max(peaks.held, counts.total)
        peaks.heldWithWaiting = Math.max(peaks.heldWithWaiting, counts.total + counts.reserved)
      },
    })
  }
}

function channelMemoryLedger(): Ledger {
  const all: unknown = Reflect.get(globalThis, '_telefunc')
  const ledger: unknown = typeof all === 'object' && all !== null ? Reflect.get(all, 'channelMemory.ts') : undefined
  if (!isLedger(ledger)) throw new Error("Telefunc's channel memory ledger isn't loaded")
  return ledger
}

function isLedger(value: unknown): value is Ledger {
  return (
    typeof value === 'object' &&
    value !== null &&
    typeof Reflect.get(value, 'total') === 'number' &&
    typeof Reflect.get(value, 'reserved') === 'number' &&
    typeof Reflect.get(value, 'maxMemory') === 'number'
  )
}
