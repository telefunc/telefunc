export { parseJsonPayload, parsePreparePayload, parseReconcilePayload, parseBarrierPayload }
export type {
  ReconcileOpenEntry,
  ReattachState,
  ReconcilePayload,
  BarrierPayload,
  PreparePayload,
  ReadyPayload,
  ReconciledPayload,
}

import { MAX_CHANNELS_PER_CONNECTION, type ChannelTransports } from '../constants.js'
import { ProtocolViolationError, assertProtocol } from './protocolViolation.js'
import { textDecoder } from './envelope.js'

// ===== Reconcile payloads (JSON-encoded after the header) =====

type ReconcileOpenEntry = {
  id: string
  ix: number
  lastSeq: number
  /** First reconcile: the server awaits it up to `connectTtl`, then an ATTACH_RESULT settles it; others fail fast. */
  initial?: true
  /** A broadcast's subscriptions as of this (re)attach, applied before its `onOpen` fires. */
  broadcast?: { text: boolean; binary: boolean }
  /** A BDP probe the RECONCILED answers: on a wire other than the last, it queues behind none of the channel's data. */
  probe?: number
}

/** What a channel adds to its own RECONCILE entry. */
type ReattachState = Pick<ReconcileOpenEntry, 'broadcast' | 'probe'>

/** On a new wire it names every channel the page holds; on the wire of the session it names, with `released`, it names
 *  only the channels no RECONCILE named there yet, and `released` the ones the page let go since its last. */
type ReconcilePayload = {
  sessionId?: string
  open: ReconcileOpenEntry[]
  released?: number[]
}

/** A barrier names the session it retires and the upgrade it commits — both mandatory, where a
 *  reconcile has neither. A malformed one can no longer fall through to ordinary reconciliation. */
type BarrierPayload = {
  sessionId: string
  upgradeId: string
  open: ReconcileOpenEntry[]
}

type PreparePayload = {
  upgradeId: string
  sessionId: string
}

type ReadyPayload = {
  upgradeId: string
}

/** Per-channel acknowledgment: the server confirms each `ix` it attached and tells the
 *  client which `lastSeq` it has on file, so the client can replay frames after that. */
type ReconciledPayload = {
  sessionId: string
  open: { ix: number; lastSeq: number }[]
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  /** The largest frame the server takes on a WebSocket. */
  maxFrameBytes: number
  serverReplayBuffer: number
  serverReplayBufferBinary: number
  clientReplayBuffer: number
  clientReplayBufferBinary: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  transports: ChannelTransports
  upgradeId?: string
}

function parseJsonPayload(payload: Uint8Array): unknown {
  try {
    return JSON.parse(textDecoder.decode(payload))
  } catch {
    throw new ProtocolViolationError('payload is not JSON')
  }
}

function asObject(value: unknown): Record<string, unknown> {
  assertProtocol(value !== null && typeof value === 'object' && !Array.isArray(value), 'payload is not an object')
  return value as Record<string, unknown>
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0
}

function isUint(value: unknown, max: number): value is number {
  return Number.isInteger(value) && (value as number) >= 0 && (value as number) <= max
}

function parsePreparePayload(value: unknown): PreparePayload {
  const { upgradeId, sessionId } = asObject(value)
  assertProtocol(isNonEmptyString(upgradeId), 'PREPARE upgradeId')
  assertProtocol(isNonEmptyString(sessionId), 'PREPARE sessionId')
  return { upgradeId, sessionId }
}

function parseOpenList(open: unknown): ReconcileOpenEntry[] {
  assertProtocol(Array.isArray(open), 'open list')
  assertProtocol(open.length <= MAX_CHANNELS_PER_CONNECTION, 'open list over entry cap')
  const indexes = new Set<number>()
  return open.map((rawEntry: unknown) => {
    const { id, ix, lastSeq, initial, probe, broadcast } = asObject(rawEntry)
    assertProtocol(typeof id === 'string', 'open entry id')
    // `ix` is truncated to u16 by the header writer, so a wider one would alias onto another channel.
    assertProtocol(isUint(ix, 0xffff) && !indexes.has(ix), 'open entry ix')
    indexes.add(ix)
    assertProtocol(isUint(lastSeq, Number.MAX_SAFE_INTEGER), 'open entry lastSeq')
    assertProtocol(initial === undefined || initial === true, 'open entry initial')
    assertProtocol(probe === undefined || isUint(probe, 0xffffffff), 'open entry probe')
    if (broadcast === undefined) return { id, ix, lastSeq, initial, probe }
    const { text, binary } = asObject(broadcast)
    assertProtocol(typeof text === 'boolean' && typeof binary === 'boolean', 'open entry broadcast')
    return { id, ix, lastSeq, initial, probe, broadcast: { text, binary } }
  })
}

function parseReconcilePayload(value: unknown): ReconcilePayload {
  const { sessionId, open, released } = asObject(value)
  const entries = parseOpenList(open)
  assertProtocol(sessionId === undefined || isNonEmptyString(sessionId), 'RECONCILE sessionId')
  if (released === undefined) return { sessionId, open: entries }
  assertProtocol(isNonEmptyString(sessionId) && Array.isArray(released), 'RECONCILE released')
  assertProtocol(released.length <= MAX_CHANNELS_PER_CONNECTION, 'RECONCILE released over entry cap')
  for (const ix of released) assertProtocol(isUint(ix, 0xffff), 'RECONCILE released ix')
  return { sessionId, open: entries, released }
}

function parseBarrierPayload(value: unknown): BarrierPayload {
  const { sessionId, upgradeId, open } = asObject(value)
  const entries = parseOpenList(open)
  // Both ids are mandatory: a barrier that cannot name its session and its upgrade has no claim
  // on either, and there is no ordinary-reconcile leg left for it to fall through to.
  assertProtocol(isNonEmptyString(sessionId), 'barrier sessionId')
  assertProtocol(isNonEmptyString(upgradeId), 'barrier upgradeId')
  return { sessionId, upgradeId, open: entries }
}
