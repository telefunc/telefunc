export {
  installChannelSubstrate,
  _resetChannelSubstrateForTesting,
  getChannelMux,
  InMemoryChannelSubstrate,
  PROXY_DIRECTION,
  ENVELOPE_KIND,
  DETACH_REASON,
  encodeProxyEnvelope,
  decodeProxyEnvelope,
  dispatchEnvelope,
}
export type {
  ChannelSubstrate,
  ChannelSubstrateHandlers,
  ConnectionRecord,
  ProxyDirection,
  ProxyEnvelope,
  ProxyAttachPayload,
  ProxyAttachAckPayload,
  ProxyDetachPayload,
  ProxyFramePayload,
  ProxyConnectionFramePayload,
  ProxyUpgradeFinalizePayload,
  ProxyPayload,
  DetachReason,
  SendFn,
}

import { assert, assertUsage } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { concat, decodeU32, encodeLengthPrefixedString, encodeU32, readLengthPrefixedString } from '../frame.js'
import { ChannelMux, type SendFn } from './substrate-runtime.js'
export type { ChannelMux }

// Cross-instance routing primitive. Channels are single-homed (their runtime state —
// closures, intervals, listeners — isn't serializable). When a client lands on a
// non-home instance, that instance becomes a *proxy* and relays wire frames over
// the substrate; the home's `ServerChannel` operates as if directly attached.
//
// The substrate is transport-agnostic: it carries opaque envelopes, knows nothing
// about wire frames or channel kinds. The local channel registry lives in `ChannelMux`.

const PROXY_DIRECTION = {
  /** Peer→home (client→server frame). */
  TO_HOME: 0x01 as const,
  /** Home→peer (server→client frame). */
  TO_PEER: 0x02 as const,
}

type ProxyDirection = (typeof PROXY_DIRECTION)[keyof typeof PROXY_DIRECTION]

const ENVELOPE_KIND = {
  ATTACH: 0x01 as const,
  ATTACH_ACK: 0x02 as const,
  DETACH: 0x03 as const,
  FRAME: 0x04 as const,
  /** POST receiver → owner: a connection-level wire frame (alias 0 — `ping`, in-body
   *  `reconcile`) from a data POST that landed on a non-owner instance. The owner
   *  re-injects on its local connection. The envelope's `channelId` field carries the
   *  connId so the owner can resolve which local connection to dispatch on. */
  CONNECTION_FRAME: 0x05 as const,
  /** Upgrade requester (new transport's instance) → owner of the previous transport: drain
   *  the previous wire's send chain, queue a `fin` ctrl, drain again, and silently retire
   *  the prior session — kept channels are already re-attached on the requester via the
   *  upgrade reconcile, so the owner must NOT fire detach lifecycle on them. Carries the
   *  list of ix's the new session kept; everything else is `RECOVERY_FAILED`. The envelope's
   *  `channelId` field carries the previous connId. */
  UPGRADE_FINALIZE: 0x06 as const,
}

const DETACH_REASON = {
  TRANSIENT: 0x01 as const,
  PERMANENT: 0x02 as const,
  RECOVERY_FAILED: 0x03 as const,
}

type DetachReason = (typeof DETACH_REASON)[keyof typeof DETACH_REASON]

type ProxyEnvelope = {
  channelId: string
  fromInstance: string
  direction: ProxyDirection
  payload: ProxyPayload
}

type ProxyFramePayload = { kind: typeof ENVELOPE_KIND.FRAME; frame: Uint8Array }

/** `ix`: client-owned wire index. `lastSeq`: highest server-emitted seq the client
 *  has acknowledged — home replays frames after it from its replay buffer. */
type ProxyAttachPayload = {
  kind: typeof ENVELOPE_KIND.ATTACH
  reconnectTimeout: number
  ix: number
  lastSeq: number
}

/** Carries the home's current `lastClientSeq` so the proxy can include it in
 *  `CtrlReconciled.open[]`. Sent after any replay frames (which flow as separate
 *  FRAME envelopes); per-connection sendChain on the proxy preserves wire order. */
type ProxyAttachAckPayload = { kind: typeof ENVELOPE_KIND.ATTACH_ACK; lastClientSeq: number }

/** `reason` selects the home's lifecycle method:
 *    `TRANSIENT`        → `_onPeerDisconnect` (start reconnect timer)
 *    `PERMANENT`        → `_onPeerClose` (terminal)
 *    `RECOVERY_FAILED`  → `_onPeerRecoveryFailure` (client dropped the channel) */
type ProxyDetachPayload = { kind: typeof ENVELOPE_KIND.DETACH; reason: DetachReason }

/** A connection-level wire frame routed cluster-wide. `direction` selects what the
 *  receiving owner does with it:
 *    TO_HOME — non-owner POST receiver forwarded a *client* frame for re-injection on
 *              the owner's local connection (e.g. alias-0 ping from a data POST).
 *    TO_PEER — non-owner asks the owner to push a *server* frame onto its local SSE
 *              wire toward the client (e.g. STREAM_REQUEST_OPEN_ACK from a non-owner
 *              that received the long-lived stream-request POST).
 *  The envelope's `channelId` field carries the connId for this kind. */
type ProxyConnectionFramePayload = { kind: typeof ENVELOPE_KIND.CONNECTION_FRAME; frame: Uint8Array }

/** Cross-instance SSE→WS upgrade finalize. The envelope's `channelId` field carries the
 *  previous connId; the payload carries the previous sessionId (so the owner can locate
 *  the right `SessionState`) and the ix's the new session kept (so dropped channels still
 *  fire `RECOVERY_FAILED` on the owner). */
type ProxyUpgradeFinalizePayload = {
  kind: typeof ENVELOPE_KIND.UPGRADE_FINALIZE
  prevSessionId: string
  keptIxes: readonly number[]
}

type ProxyPayload =
  | ProxyFramePayload
  | ProxyAttachPayload
  | ProxyAttachAckPayload
  | ProxyDetachPayload
  | ProxyConnectionFramePayload
  | ProxyUpgradeFinalizePayload

type ChannelSubstrateHandlers = {
  /** TO_HOME `attach` — proxy is announcing itself for this channel. */
  onAttach?: (env: ProxyEnvelope, payload: ProxyAttachPayload) => void
  /** TO_HOME `detach` — proxy is announcing the wire is gone. */
  onDetach?: (env: ProxyEnvelope, payload: ProxyDetachPayload) => void
  /** TO_HOME `frame` — proxied client→server wire frame. */
  onHomeFrame?: (env: ProxyEnvelope, payload: ProxyFramePayload) => void
  /** TO_PEER `frame` — server→client wire frame to write on the proxied wire. */
  onPeerFrame?: (env: ProxyEnvelope, payload: ProxyFramePayload) => void
  /** TO_PEER `detach` — home is telling the proxy to drop its routing state. */
  onPeerDetach?: (env: ProxyEnvelope, payload: ProxyDetachPayload) => void
  /** TO_PEER `attach-ack` — home's response to this peer's `attach`. */
  onAttachAck?: (env: ProxyEnvelope, payload: ProxyAttachAckPayload) => void
  /** TO_HOME `connection-frame` — non-owner forwarded a *client* alias-0 frame for this
   *  owner to re-inject on its local connection (effective direction client→server).
   *  `env.channelId` is the connId. */
  onConnectionFrameToServer?: (env: ProxyEnvelope, payload: ProxyConnectionFramePayload) => void
  /** TO_PEER `connection-frame` — non-owner asks this owner to push a *server* frame onto
   *  its local SSE wire toward the client (effective direction server→client).
   *  `env.channelId` is the connId. */
  onConnectionFrameToClient?: (env: ProxyEnvelope, payload: ProxyConnectionFramePayload) => void
  /** TO_HOME `upgrade-finalize` — upgrade requester asking this previous-transport owner to
   *  drain its local wire, send `fin`, and retire the previous session silently for the
   *  ix's the new session kept. `env.channelId` is the previous connId. */
  onUpgradeFinalize?: (env: ProxyEnvelope, payload: ProxyUpgradeFinalizePayload) => void
}

function dispatchEnvelope(handlers: ChannelSubstrateHandlers, env: ProxyEnvelope): void {
  const p = env.payload
  if (p.kind === ENVELOPE_KIND.FRAME) {
    if (env.direction === PROXY_DIRECTION.TO_HOME) handlers.onHomeFrame?.(env, p)
    else handlers.onPeerFrame?.(env, p)
    return
  }
  if (p.kind === ENVELOPE_KIND.CONNECTION_FRAME) {
    if (env.direction === PROXY_DIRECTION.TO_HOME) handlers.onConnectionFrameToServer?.(env, p)
    else handlers.onConnectionFrameToClient?.(env, p)
    return
  }
  if (p.kind === ENVELOPE_KIND.DETACH) {
    if (env.direction === PROXY_DIRECTION.TO_HOME) handlers.onDetach?.(env, p)
    else handlers.onPeerDetach?.(env, p)
    return
  }
  if (p.kind === ENVELOPE_KIND.UPGRADE_FINALIZE) {
    handlers.onUpgradeFinalize?.(env, p)
    return
  }
  if (p.kind === ENVELOPE_KIND.ATTACH) handlers.onAttach?.(env, p)
  else handlers.onAttachAck?.(env, p)
}

/** The cluster-visible state of a single client connection. Consulted only at reconcile
 *  (sessionId verification when the client lands on a non-issuing instance). Per-frame
 *  routing on the data hot path uses metadata supplied by the client in each data POST,
 *  so this record stays minimal. */
type ConnectionRecord = {
  /** The instance holding this connection's SSE response wire. */
  owner: string
  /** The currently-rotated session token. Used by reconcile on a non-local instance to
   *  verify a client's claimed `prevSessionId` against what the cluster believes. */
  sessionId: string
}

interface ChannelSubstrate {
  readonly selfInstanceId: string
  /** Heartbeat cadence the runtime uses for refreshing every kind of pin (ms). */
  readonly heartbeatIntervalMs: number

  // ── ServerChannel directory ─────────────────────────────────────────────────

  /** Announce this instance as home for `channelId` — cluster-wide only. Same-process
   *  waiters are the runtime's job and fire synchronously before this is called. */
  pinChannel(channelId: string): Promise<void>

  unpinChannel(channelId: string): Promise<void>

  /** Refresh TTL on every supplied channel pin in one batch. Implementations pipeline. */
  refreshChannels(channelIds: readonly string[]): Promise<void>

  /** Locate the home for `channelId` *on a different instance*. `timeoutMs` bounds the
   *  total wait (including the lookup itself). Self pins MUST NOT be returned: the
   *  runtime races this against its own local-channel waiter. */
  locateRemoteHome(channelId: string, timeoutMs: number): Promise<string | null>

  // ── Connection directory ──────────────────────────────────────────────

  /** Atomically replace the connection record so re-reconciles invalidate the previous
   *  sessionId cluster-wide. */
  pinConnection(connId: string, record: ConnectionRecord): Promise<void>

  unpinConnection(connId: string): Promise<void>

  /** Refresh TTL on every supplied connection record in one batch. */
  refreshConnections(connIds: readonly string[]): Promise<void>

  /** Read the entire connection record in one round trip. Returns null if the record
   *  doesn't exist (connection unpinned, never reconciled, or TTL'd out). */
  locateConnection(connId: string): Promise<ConnectionRecord | null>

  // ── Instance liveness ─────────────────────────────────────────────────

  /** Announce this instance is alive cluster-wide and refresh the TTL. Called once at
   *  mux init and again on every heartbeat so peers can detect silent death (instance
   *  crashed, network-partitioned from Redis, etc.) when the pin's TTL elapses. */
  pinInstance(): Promise<void>

  unpinInstance(): Promise<void>

  /** True iff `instanceId`'s alive-pin is still present. Used by homes to detect dead
   *  owners (clears proxy attachment, fires per-channel `_onPeerDisconnect`) and by
   *  owners to detect dead homes (synthesizes per-channel `abort` to the client). */
  isInstanceAlive(instanceId: string): Promise<boolean>

  // ── Cluster messaging ─────────────────────────────────────────────────

  /** Send an opaque envelope to another cluster instance. Envelope kinds and their
   *  semantics live in `ENVELOPE_KIND` + `ChannelSubstrateHandlers`; the substrate
   *  itself just delivers bytes. The `ChannelMux` constructs envelopes inline at every
   *  call site — keeping shape inside one module. */
  forward(targetInstance: string, envelope: ProxyEnvelope): Promise<void>

  /** Subscribe to envelopes destined for this instance. Each envelope is dispatched
   *  to whichever entries in `handlers` apply. Returns an unsubscribe callable. */
  listen(handlers: ChannelSubstrateHandlers): () => void

  dispose(): Promise<void>
}

/** Single-process default. Cross-instance ops are no-ops; same-process traffic
 *  bypasses the substrate entirely (handled by the mux's local registry + waiters). */
class InMemoryChannelSubstrate implements ChannelSubstrate {
  readonly selfInstanceId: string = 'in-memory'
  readonly heartbeatIntervalMs = 60_000

  async pinChannel(_channelId: string): Promise<void> {}
  async unpinChannel(_channelId: string): Promise<void> {}
  async refreshChannels(_channelIds: readonly string[]): Promise<void> {}
  async locateRemoteHome(_channelId: string, _timeoutMs: number): Promise<string | null> {
    return null
  }
  async pinConnection(_connId: string, _record: ConnectionRecord): Promise<void> {}
  async unpinConnection(_connId: string): Promise<void> {}
  async refreshConnections(_connIds: readonly string[]): Promise<void> {}
  async locateConnection(_connId: string): Promise<ConnectionRecord | null> {
    return null
  }
  async pinInstance(): Promise<void> {}
  async unpinInstance(): Promise<void> {}
  async isInstanceAlive(_instanceId: string): Promise<boolean> {
    // Single-process — only one instance ever exists; treat any reachable instanceId as alive.
    return true
  }
  async forward(_targetInstance: string, _envelope: ProxyEnvelope): Promise<void> {}
  listen(_handlers: ChannelSubstrateHandlers): () => void {
    return () => {}
  }
  async dispose(): Promise<void> {}
}

// ───────────────────────────────────────────────────────────────────────────
// Envelope wire format
//
//   [u8 kind][u8 direction]
//   [u32 BE channelIdLen][channelId UTF-8]      (= connId for CONNECTION_FRAME)
//   [u32 BE fromInstanceLen][fromInstance UTF-8]
//   [payload]
//
// Payload by kind:
//   ATTACH:           [u32 reconnectTimeout][u32 ix][u32 lastSeq]
//   ATTACH_ACK:       [u32 lastClientSeq]
//   DETACH:           [u8 reasonTag]
//   FRAME:            [bytes…] (length implied by remaining buffer)
//   CONNECTION_FRAME: [bytes…] (raw wire frame — owner re-injects on local connection)
//   UPGRADE_FINALIZE: [u32 prevSessionIdLen][prevSessionId UTF-8][u32 keptCount][u32 ix]…
// ───────────────────────────────────────────────────────────────────────────

function encodeProxyEnvelope(env: ProxyEnvelope): Uint8Array<ArrayBuffer> {
  const tags = new Uint8Array([env.payload.kind, env.direction])
  const channelId = encodeLengthPrefixedString(env.channelId)
  const fromInstance = encodeLengthPrefixedString(env.fromInstance)
  const payload = env.payload
  let payloadBytes: Uint8Array<ArrayBuffer>
  if (payload.kind === ENVELOPE_KIND.FRAME || payload.kind === ENVELOPE_KIND.CONNECTION_FRAME) {
    payloadBytes = payload.frame as Uint8Array<ArrayBuffer>
  } else if (payload.kind === ENVELOPE_KIND.DETACH) {
    payloadBytes = new Uint8Array([payload.reason])
  } else if (payload.kind === ENVELOPE_KIND.ATTACH_ACK) {
    payloadBytes = encodeU32(payload.lastClientSeq)
  } else if (payload.kind === ENVELOPE_KIND.UPGRADE_FINALIZE) {
    const prevSessionId = encodeLengthPrefixedString(payload.prevSessionId)
    const ixBytes = new Uint8Array(4 + payload.keptIxes.length * 4) as Uint8Array<ArrayBuffer>
    const view = new DataView(ixBytes.buffer)
    view.setUint32(0, payload.keptIxes.length, false)
    for (let i = 0; i < payload.keptIxes.length; i++) view.setUint32(4 + i * 4, payload.keptIxes[i]!, false)
    payloadBytes = concat(prevSessionId, ixBytes)
  } else {
    payloadBytes = concat(encodeU32(payload.reconnectTimeout), encodeU32(payload.ix), encodeU32(payload.lastSeq))
  }
  return concat(tags, channelId, fromInstance, payloadBytes)
}

function decodeProxyEnvelope(bytes: Uint8Array): ProxyEnvelope {
  assert(bytes.byteLength >= 2, 'Malformed substrate envelope — header truncated')
  const kind = bytes[0]
  assert(
    kind === ENVELOPE_KIND.ATTACH ||
      kind === ENVELOPE_KIND.ATTACH_ACK ||
      kind === ENVELOPE_KIND.DETACH ||
      kind === ENVELOPE_KIND.FRAME ||
      kind === ENVELOPE_KIND.CONNECTION_FRAME ||
      kind === ENVELOPE_KIND.UPGRADE_FINALIZE,
    `Malformed substrate envelope — unknown kind ${kind}`,
  )
  const direction = bytes[1]
  assert(
    direction === PROXY_DIRECTION.TO_HOME || direction === PROXY_DIRECTION.TO_PEER,
    `Malformed substrate envelope — unknown direction ${direction}`,
  )
  const channelIdResult = readLengthPrefixedString(bytes, 2)
  const fromInstanceResult = readLengthPrefixedString(bytes, channelIdResult.offsetAfter)
  const offset = fromInstanceResult.offsetAfter
  const view = bytes as Uint8Array<ArrayBuffer>
  let payload: ProxyPayload
  if (kind === ENVELOPE_KIND.FRAME || kind === ENVELOPE_KIND.CONNECTION_FRAME) {
    payload = { kind, frame: bytes.subarray(offset) }
  } else if (kind === ENVELOPE_KIND.DETACH) {
    const reason = bytes[offset]
    assert(
      reason === DETACH_REASON.TRANSIENT ||
        reason === DETACH_REASON.PERMANENT ||
        reason === DETACH_REASON.RECOVERY_FAILED,
      `Malformed substrate envelope — unknown detach reason ${reason}`,
    )
    payload = { kind, reason }
  } else if (kind === ENVELOPE_KIND.ATTACH_ACK) {
    payload = { kind, lastClientSeq: decodeU32(view, offset) }
  } else if (kind === ENVELOPE_KIND.UPGRADE_FINALIZE) {
    const prevSessionResult = readLengthPrefixedString(bytes, offset)
    const after = prevSessionResult.offsetAfter
    const count = decodeU32(view, after)
    const keptIxes = new Array<number>(count)
    for (let i = 0; i < count; i++) keptIxes[i] = decodeU32(view, after + 4 + i * 4)
    payload = { kind, prevSessionId: prevSessionResult.value, keptIxes }
  } else {
    payload = {
      kind,
      reconnectTimeout: decodeU32(view, offset),
      ix: decodeU32(view, offset + 4),
      lastSeq: decodeU32(view, offset + 8),
    }
  }
  return { channelId: channelIdResult.value, fromInstance: fromInstanceResult.value, direction, payload }
}

const globalObject = getGlobalObject<{
  substrate: ChannelSubstrate
  mux: ChannelMux
  installed: boolean
}>('wire-protocol/server/substrate.ts', () => {
  const substrate = new InMemoryChannelSubstrate()
  return { substrate, mux: new ChannelMux(substrate), installed: false }
})

function getChannelMux(): ChannelMux {
  return globalObject.mux
}

function installChannelSubstrate(substrate: ChannelSubstrate): void {
  if (globalObject.installed) return
  assertUsage(
    !globalObject.mux.hasChannels(),
    '`config.channel.substrate` must be set before any channel is registered.',
  )
  swapSubstrate(substrate)
  globalObject.installed = true
}

function _resetChannelSubstrateForTesting(substrate: ChannelSubstrate): void {
  swapSubstrate(substrate)
  globalObject.installed = true
}

function swapSubstrate(substrate: ChannelSubstrate): void {
  globalObject.mux.dispose()
  globalObject.substrate = substrate
  globalObject.mux = new ChannelMux(substrate)
}
