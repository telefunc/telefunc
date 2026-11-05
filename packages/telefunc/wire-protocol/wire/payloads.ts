export { parseJsonPayload, parsePreparePayload, parseReconcilePayload, parseBarrierPayload }
export type {
  ReconcileOpenEntry,
  AttachState,
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
type AttachState = Pick<ReconcileOpenEntry, 'broadcast' | 'probe'>

/** On a new wire it names every channel the page holds; on the wire of the session it names, with `released`, it names
 *  only the channels no RECONCILE named there yet, and `released` the ones the page let go since its last. */
type ReconcilePayload = {
  sessionId?: string
  open: ReconcileOpenEntry[]
  released?: number[]
}

/** A barrier names the session it retires and the upgrade it commits; a reconcile has neither. Both are required:
 *  `parseBarrierPayload` refuses a barrier without them. */
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

/** Checks the open list JSON.parse built, where it stands: no entry is copied. */
function assertOpenList(open: unknown): asserts open is ReconcileOpenEntry[] {
  assertProtocol(Array.isArray(open), 'open list')
  assertProtocol(open.length <= MAX_CHANNELS_PER_CONNECTION, 'open list over entry cap')
  const indexes = new Set<number>()
  for (const rawEntry of open) {
    const entry = asObject(rawEntry)
    assertProtocol(typeof entry.id === 'string', 'open entry id')
    // `ix` is truncated to u16 by the header writer, so a wider one would alias onto another channel.
    assertProtocol(isUint(entry.ix, 0xffff) && !indexes.has(entry.ix), 'open entry ix')
    indexes.add(entry.ix)
    assertProtocol(isUint(entry.lastSeq, Number.MAX_SAFE_INTEGER), 'open entry lastSeq')
    assertProtocol(entry.initial === undefined || entry.initial === true, 'open entry initial')
    assertProtocol(entry.probe === undefined || isUint(entry.probe, 0xffffffff), 'open entry probe')
    if (entry.broadcast === undefined) continue
    const broadcast = asObject(entry.broadcast)
    assertProtocol(typeof broadcast.text === 'boolean' && typeof broadcast.binary === 'boolean', 'open entry broadcast')
  }
}

function assertReconcilePayload(value: unknown): asserts value is ReconcilePayload {
  const payload = asObject(value)
  assertOpenList(payload.open)
  assertProtocol(payload.sessionId === undefined || isNonEmptyString(payload.sessionId), 'RECONCILE sessionId')
  if (payload.released === undefined) return
  assertProtocol(isNonEmptyString(payload.sessionId) && Array.isArray(payload.released), 'RECONCILE released')
  assertProtocol(payload.released.length <= MAX_CHANNELS_PER_CONNECTION, 'RECONCILE released over entry cap')
  for (const ix of payload.released) assertProtocol(isUint(ix, 0xffff), 'RECONCILE released ix')
}

/** The RECONCILE JSON.parse built, checked where it stands. */
function parseReconcilePayload(value: unknown): ReconcilePayload {
  assertReconcilePayload(value)
  return value
}

function assertBarrierPayload(value: unknown): asserts value is BarrierPayload {
  const payload = asObject(value)
  assertOpenList(payload.open)
  // Both ids are mandatory: a barrier that cannot name its session and its upgrade has no claim
  // on either, and there is no ordinary-reconcile leg left for it to fall through to.
  assertProtocol(isNonEmptyString(payload.sessionId), 'barrier sessionId')
  assertProtocol(isNonEmptyString(payload.upgradeId), 'barrier upgradeId')
}

/** The BARRIER JSON.parse built, checked where it stands. */
function parseBarrierPayload(value: unknown): BarrierPayload {
  assertBarrierPayload(value)
  return value
}
