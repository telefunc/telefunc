export {
  parseJsonPayload,
  parsePreparePayload,
  parseReconcilePayload,
  parseBarrierPayload,
  isWant,
  isCallWant,
  isChannelId,
  MAX_U32,
  WIDEST_ENTRY,
  WIRE_MAX_CTRL_FRAME_BYTES,
}
export type {
  ReconcileOpenEntry,
  BroadcastKind,
  BroadcastSubscriptions,
  AttachState,
  Want,
  CallWant,
  ReconcilePayload,
  BarrierPayload,
  PreparePayload,
  ReadyPayload,
  ReconciledPayload,
}

import { MAX_CHANNELS_PER_CONNECTION, UPGRADE_MAX_ID_BYTES, type ChannelTransports } from '../constants.js'
import { ProtocolViolationError, assertProtocol } from './protocolViolation.js'
import { textDecoder, textEncoder } from './envelope.js'
import { HEADER } from './header.js'
import { hasProp } from '../../utils/hasProp.js'

// ===== Reconcile payloads (JSON-encoded after the header) =====

/** What a page wants to hear on a broadcast: `version` counts the changes the page made to it, so the server applies a
 *  want only over an older one, whichever way each came. `parts` is what an add-on built on the broadcast lets the page
 *  want beyond its two kinds, such as Room's members and tracks, as JSON the add-on reads. */
type Want = { version: number; text: boolean; binary: boolean; parts?: unknown }

/** A want a telefunction call carries, for the broadcast `channel` names. */
type CallWant = Want & { channel: string }

type ReconcileOpenEntry = {
  id: string
  ix: number
  lastSeq: number
  /** First reconcile: the server awaits it up to `connectTtl`, then an ATTACH_RESULT settles it; others fail fast. */
  initial?: true
  /** A broadcast's want as of this (re)attach, applied before its `onOpen` fires. */
  want?: Want
  /** A BDP probe the RECONCILED answers: on a wire other than the last, it queues behind none of the channel's data. */
  probe?: number
}

type BroadcastKind = 'text' | 'binary'
type BroadcastSubscriptions = Record<BroadcastKind, boolean>

/** What a channel adds to its own RECONCILE entry. */
type AttachState = Pick<ReconcileOpenEntry, 'want' | 'probe'>

// ===== What the checks admit, which the control-frame cap counts =====

/** The largest `lastSeq` an entry may name. */
const MAX_LAST_SEQ = Number.MAX_SAFE_INTEGER
/** The largest probe and want version an entry may carry. */
const MAX_U32 = 0xffffffff
/** The longest id Telefunc issues, a session's or an upgrade's: `crypto.randomUUID()`'s. */
const ISSUED_ID_BYTES = 36

/** The widest entry the checks admit, its id aside: each key at its widest value. It has every key the type has, so
 *  the cap follows a key an entry gains. */
const WIDEST_ENTRY: Required<ReconcileOpenEntry> = {
  id: '',
  ix: 0xffff,
  lastSeq: MAX_LAST_SEQ,
  initial: true,
  want: { version: MAX_U32, text: false, binary: false },
  probe: MAX_U32,
}

/** An entry at its widest with its id at `UPGRADE_MAX_ID_BYTES`, and the separator after it. */
const WIDEST_ENTRY_BYTES = JSON.stringify(WIDEST_ENTRY).length + UPGRADE_MAX_ID_BYTES + 1
const WIDEST_ISSUED_ID = 'x'.repeat(ISSUED_ID_BYTES)
/** What a RECONCILE or a barrier carries besides its entries: its ids at their widest, and every index released. */
const WIDEST_HEAD_BYTES = Math.max(
  JSON.stringify({ sessionId: WIDEST_ISSUED_ID, attempt: MAX_U32, open: [], released: [] }).length +
    MAX_CHANNELS_PER_CONNECTION * (String(0xffff).length + 1),
  JSON.stringify({ sessionId: WIDEST_ISSUED_ID, upgradeId: WIDEST_ISSUED_ID, open: [] }).length,
)

/** Largest wire control frame the server will decode, checked on the raw bytes. Derived from what the checks admit,
 *  never picked: the widest RECONCILE or barrier lists every channel a connection may hold, each entry at its widest.
 *  A cap that refuses a legal frame is worse than no cap. */
const WIRE_MAX_CTRL_FRAME_BYTES = HEADER + MAX_CHANNELS_PER_CONNECTION * WIDEST_ENTRY_BYTES + WIDEST_HEAD_BYTES

/** On a new wire it names every channel the page holds; on the wire of the session it names, with `released`, it names
 *  only the channels no RECONCILE named there yet, and `released` the ones the page let go since its last. */
type ReconcilePayload = {
  /** The page's session: the id the server gave it, or before that the one the page made. */
  sessionId: string
  /** The page's number for the wire it comes on, one more for each wire the page opens: what orders its attempts, on a
   *  wire that doesn't hold the session yet, where it is required. */
  attempt?: number
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
  /** The page's number for the probe, as a RECONCILE's `attempt` numbers its wire. */
  attempt: number
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
  /** The largest credit window, each way. */
  creditWindow: number
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

/** The bytes `value` takes in a frame's JSON, its quotes aside: what the control-frame cap counts. */
function jsonBytes(value: string): number {
  return textEncoder.encode(JSON.stringify(value)).byteLength - 2
}

/** A channel's id the cap counts: at most `UPGRADE_MAX_ID_BYTES` as JSON writes it. */
function isChannelId(value: unknown): value is string {
  return typeof value === 'string' && jsonBytes(value) <= UPGRADE_MAX_ID_BYTES
}

/** An id Telefunc issues, a session's or an upgrade's, at most the length the cap counts it at. */
function isIssuedId(value: unknown): value is string {
  return isNonEmptyString(value) && jsonBytes(value) <= ISSUED_ID_BYTES
}

function parsePreparePayload(value: unknown): PreparePayload {
  const { upgradeId, sessionId, attempt } = asObject(value)
  assertProtocol(isIssuedId(upgradeId), 'PREPARE upgradeId')
  assertProtocol(isIssuedId(sessionId), 'PREPARE sessionId')
  assertProtocol(isUint(attempt, MAX_U32), 'PREPARE attempt')
  return { upgradeId, sessionId, attempt }
}

/** Checks the open list JSON.parse built, where it stands: no entry is copied. */
function assertOpenList(open: unknown): asserts open is ReconcileOpenEntry[] {
  assertProtocol(Array.isArray(open), 'open list')
  assertProtocol(open.length <= MAX_CHANNELS_PER_CONNECTION, 'open list over entry cap')
  const indexes = new Set<number>()
  const ids = new Set<string>()
  for (const rawEntry of open) {
    const entry = asObject(rawEntry)
    // A page holds one end of a channel, at one index.
    assertProtocol(typeof entry.id === 'string' && !ids.has(entry.id), 'open entry id')
    ids.add(entry.id)
    // `ix` is truncated to u16 by the header writer, so a wider one would alias onto another channel.
    assertProtocol(isUint(entry.ix, 0xffff) && !indexes.has(entry.ix), 'open entry ix')
    indexes.add(entry.ix)
    assertProtocol(isUint(entry.lastSeq, MAX_LAST_SEQ), 'open entry lastSeq')
    assertProtocol(entry.initial === undefined || entry.initial === true, 'open entry initial')
    assertProtocol(entry.probe === undefined || isUint(entry.probe, MAX_U32), 'open entry probe')
    assertProtocol(entry.want === undefined || isWant(entry.want), 'open entry want')
  }
}

function isCallWant(value: unknown): value is CallWant {
  return isWant(value) && hasProp(value, 'channel', 'string')
}

function isWant(value: unknown): value is Want {
  return (
    hasProp(value, 'version', 'number') &&
    isUint(value.version, MAX_U32) &&
    hasProp(value, 'text', 'boolean') &&
    hasProp(value, 'binary', 'boolean')
  )
}

function assertReconcilePayload(value: unknown): asserts value is ReconcilePayload {
  const payload = asObject(value)
  assertOpenList(payload.open)
  // A barrier's ids are checked as it is admitted, so that one over the cap ends its probe alone.
  for (const { id } of payload.open) assertProtocol(isChannelId(id), 'open entry id over the cap')
  assertProtocol(isIssuedId(payload.sessionId), 'RECONCILE sessionId')
  assertProtocol(payload.attempt === undefined || isUint(payload.attempt, MAX_U32), 'RECONCILE attempt')
  if (payload.released === undefined) return
  assertProtocol(Array.isArray(payload.released), 'RECONCILE released')
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
  assertProtocol(isIssuedId(payload.sessionId), 'barrier sessionId')
  assertProtocol(isIssuedId(payload.upgradeId), 'barrier upgradeId')
}

/** The BARRIER JSON.parse built, checked where it stands. */
function parseBarrierPayload(value: unknown): BarrierPayload {
  assertBarrierPayload(value)
  return value
}
