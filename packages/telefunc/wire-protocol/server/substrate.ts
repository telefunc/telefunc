export {
  getChannelSubstrate,
  installChannelSubstrate,
  _resetChannelSubstrateForTesting,
  getChannelMux,
  InMemoryChannelSubstrate,
  PROXY_DIRECTION,
  ENVELOPE_KIND,
  encodeProxyEnvelope,
  decodeProxyEnvelope,
  dispatchEnvelope,
}
export type {
  ChannelSubstrate,
  ChannelSubstrateHandlers,
  ProxyDirection,
  ProxyEnvelope,
  ProxyAttachPayload,
  ProxyAttachAckPayload,
  ProxyDetachPayload,
  ProxyFramePayload,
  ProxyPayload,
  DetachReason,
  SendFn,
}

import { assert, assertUsage } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { concat, decodeU32, encodeLengthPrefixedString, encodeU32, readLengthPrefixedString } from '../frame.js'
import { ChannelMux, type DetachReason, type SendFn } from './substrate-runtime.js'
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
}

const DETACH_REASON_TAG = {
  transient: 0x01,
  permanent: 0x02,
  'recovery-failed': 0x03,
} as const

const DETACH_REASON_BY_TAG: Record<number, DetachReason> = {
  [DETACH_REASON_TAG.transient]: 'transient',
  [DETACH_REASON_TAG.permanent]: 'permanent',
  [DETACH_REASON_TAG['recovery-failed']]: 'recovery-failed',
}

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
 *    `'transient'`       → `_onPeerDisconnect` (start reconnect timer)
 *    `'permanent'`       → `_onPeerClose` (terminal)
 *    `'recovery-failed'` → `_onPeerRecoveryFailure` (client dropped the channel) */
type ProxyDetachPayload = { kind: typeof ENVELOPE_KIND.DETACH; reason: DetachReason }

type ProxyPayload = ProxyFramePayload | ProxyAttachPayload | ProxyAttachAckPayload | ProxyDetachPayload

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
}

function dispatchEnvelope(handlers: ChannelSubstrateHandlers, env: ProxyEnvelope): void {
  const p = env.payload
  if (p.kind === ENVELOPE_KIND.FRAME) {
    if (env.direction === PROXY_DIRECTION.TO_HOME) handlers.onHomeFrame?.(env, p)
    else handlers.onPeerFrame?.(env, p)
    return
  }
  if (p.kind === ENVELOPE_KIND.DETACH) {
    if (env.direction === PROXY_DIRECTION.TO_HOME) handlers.onDetach?.(env, p)
    else handlers.onPeerDetach?.(env, p)
    return
  }
  if (p.kind === ENVELOPE_KIND.ATTACH) handlers.onAttach?.(env, p)
  else handlers.onAttachAck?.(env, p)
}

interface ChannelSubstrate {
  readonly selfInstanceId: string
  /** Heartbeat cadence the runtime uses for `refreshPins` (ms). */
  readonly heartbeatIntervalMs: number

  /** Announce this instance as home for `channelId` — cluster-wide only. Same-process
   *  waiters are the runtime's job and fire synchronously before this is called. */
  pinChannel(channelId: string): Promise<void>

  unpinChannel(channelId: string): Promise<void>

  /** Refresh TTL on every supplied pin in one batch. Implementations should pipeline. */
  refreshPins(channelIds: readonly string[]): Promise<void>

  /** Locate the home for `channelId` *on a different instance*.
   *
   *  `timeoutMs` is the additional wait *after* the synchronous lookup completes —
   *  implementations MUST always perform the lookup first, so `timeoutMs=0` means
   *  "fail fast if the directory is empty." Self pins MUST NOT be returned: the
   *  runtime races this against its own local-channel waiter. */
  locateRemoteHome(channelId: string, timeoutMs: number): Promise<string | null>

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
  async refreshPins(_channelIds: readonly string[]): Promise<void> {}
  async locateRemoteHome(_channelId: string, _timeoutMs: number): Promise<string | null> {
    return null
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
//   [u32 BE channelIdLen][channelId UTF-8]
//   [u32 BE fromInstanceLen][fromInstance UTF-8]
//   [payload]
//
// Payload by kind:
//   ATTACH:     [u32 reconnectTimeout][u32 ix][u32 lastSeq]
//   ATTACH_ACK: [u32 lastClientSeq]
//   DETACH:     [u8 reasonTag]
//   FRAME:      [bytes…] (length implied by remaining buffer)
// ───────────────────────────────────────────────────────────────────────────

function encodeProxyEnvelope(env: ProxyEnvelope): Uint8Array<ArrayBuffer> {
  const tags = new Uint8Array([env.payload.kind, env.direction])
  const channelId = encodeLengthPrefixedString(env.channelId)
  const fromInstance = encodeLengthPrefixedString(env.fromInstance)
  const payload = env.payload
  let payloadBytes: Uint8Array<ArrayBuffer>
  if (payload.kind === ENVELOPE_KIND.FRAME) {
    payloadBytes = payload.frame as Uint8Array<ArrayBuffer>
  } else if (payload.kind === ENVELOPE_KIND.DETACH) {
    const tag = DETACH_REASON_TAG[payload.reason]
    assert(tag !== undefined, `Unknown detach reason "${payload.reason}"`)
    payloadBytes = new Uint8Array([tag])
  } else if (payload.kind === ENVELOPE_KIND.ATTACH_ACK) {
    payloadBytes = encodeU32(payload.lastClientSeq)
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
      kind === ENVELOPE_KIND.FRAME,
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
  if (kind === ENVELOPE_KIND.FRAME) {
    payload = { kind, frame: bytes.subarray(offset) }
  } else if (kind === ENVELOPE_KIND.DETACH) {
    const reasonTag = bytes[offset]
    assert(reasonTag !== undefined, 'Malformed substrate envelope — detach payload truncated')
    const reason = DETACH_REASON_BY_TAG[reasonTag]
    assert(reason !== undefined, `Malformed substrate envelope — unknown detach reason tag ${reasonTag}`)
    payload = { kind, reason }
  } else if (kind === ENVELOPE_KIND.ATTACH_ACK) {
    payload = { kind, lastClientSeq: decodeU32(view, offset) }
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

function getChannelSubstrate(): ChannelSubstrate {
  return globalObject.substrate
}

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
