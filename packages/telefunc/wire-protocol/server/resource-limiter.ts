export { reserveLiveQuery, LiveQuotaExceededError }
export { _resetLimiterForTesting, _liveConnectionCountForTesting, _liveReservationCountForTesting }
export type { Reservation }

import { getServerConfig } from '../../node/server/serverConfig.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { assert } from '../../utils/assert.js'
import { incrementCounter } from '../../node/server/live/telemetry.js'

/** Thrown when a connection is already at its live-query cap. Named so the caller (the result hook)
 *  can turn it into a typed rejection rather than a generic 500. */
class LiveQuotaExceededError extends Error {
  readonly connectionId: string
  constructor(connectionId: string, cap: number) {
    super(`Live-query reservation refused: connection is at its cap of ${cap} live channels.`)
    this.name = 'LiveQuotaExceededError'
    this.connectionId = connectionId
  }
}

/** A held live-query slot. Reserve before acquiring the channel; `transferToChannel` binds the
 *  release to the channel's permanent close; `release` is the pre-transfer unwind path. Both are
 *  idempotent — the slot is freed exactly once. */
type Reservation = {
  transferToChannel(channel: { onClose(cb: () => void): void }): void
  release(): void
}

const globalObject = getGlobalObject<{ connections: Map<string, { count: number }> }>('resource-limiter.ts', () => ({
  connections: new Map(),
}))

/** Reserve one live-query slot on `connectionId`, or throw `LiveQuotaExceededError` when the
 *  connection is at cap (default `config.channel.liveQueryPerConnection`). */
function reserveLiveQuery(connectionId: string): Reservation {
  const cap = getServerConfig().channel.liveQueryPerConnection
  const { connections } = globalObject
  const ledger = connections.get(connectionId) ?? { count: 0 }
  if (ledger.count >= cap) {
    incrementCounter('live.limiter.rejected')
    throw new LiveQuotaExceededError(connectionId, cap)
  }
  ledger.count++
  connections.set(connectionId, ledger)

  let released = false
  const release = (): void => {
    if (released) return
    released = true
    const current = connections.get(connectionId)
    if (!current) return
    current.count--
    if (current.count <= 0) connections.delete(connectionId)
  }

  let transferred = false
  const transferToChannel = (channel: { onClose(cb: () => void): void }): void => {
    assert(!transferred)
    transferred = true
    channel.onClose(release)
  }

  return { transferToChannel, release }
}

/** @internal test-only reset. */
function _resetLimiterForTesting(): void {
  globalObject.connections = new Map()
}

/** @internal test-only — number of connections with a live reservation ledger (must return to 0). */
function _liveConnectionCountForTesting(): number {
  return globalObject.connections.size
}

/** @internal test-only — held reservations on one connection. */
function _liveReservationCountForTesting(connectionId: string): number {
  return globalObject.connections.get(connectionId)?.count ?? 0
}
