export { SubstrateMux }

import { assert } from '../../../utils/assert.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { SUBSTRATE_ATTACH_ACK_TIMEOUT_MS } from '../../constants.js'
import { decode, encode } from '../../shared-ws.js'
import type { ChannelFrame, ReconcilePayload } from '../../shared-ws.js'
import { IndexedPeer, type PeerSender } from '../IndexedPeer.js'
import { ChannelMux, type ChannelHandle, type SendFn } from './mux.js'
import {
  DETACH_REASON,
  ENVELOPE_KIND,
  PROXY_DIRECTION,
  type ChannelSubstrate,
  type DetachReason,
  type ProxyAttachPayload,
} from './protocol.js'

// All cross-instance plumbing lives here: proxy/home routing, cluster sessionId
// verification, cross-instance upgrade finalize, and a heartbeat that doubles as
// peer-liveness probe. Delete this file → single-instance still works; multi-instance
// (`installChannelSubstrate`) breaks.

/** Per-proxied-channel state on the PROXY side: who the home is, the client-allocated
 *  ix (needed to synthesize an `abort` on the local wire if the home dies), and the
 *  closure that writes server→client frames onto the local wire. */
type ProxyChannelState = {
  homeInstance: string
  ix: number
  writeFrame: SendFn
}

class SubstrateMux extends ChannelMux {
  /** HOME side: channelId → owning proxy instance. The fromInstance check in `detachHome`
   *  rejects stale frames/detaches from a handed-off proxy. */
  private readonly homeAttached = new Map<string, string>()

  /** PROXY side: channelId → routing state for the home it forwards to. */
  private readonly proxyStates = new Map<string, ProxyChannelState>()

  /** Connections this instance owns (it holds the SSE response wire). Refreshed each
   *  heartbeat so non-owner POST receivers can locate `owner` via `locateConnection`. */
  private readonly ownedConnections = new Set<string>()

  private readonly unsubscribe: () => void
  private readonly heartbeatTimer: ReturnType<typeof setInterval>

  constructor(private readonly substrate: ChannelSubstrate) {
    super(substrate.selfInstanceId)
    this.unsubscribe = substrate.listen({
      onAttach: (env, payload) => void this.attachHome(env.channelId, env.fromInstance, payload),
      onDetach: (env, payload) => this.detachHome(env.channelId, env.fromInstance, payload.reason),
      onHomeFrame: (env, payload) => this.dispatchHomeFrame(env.channelId, payload.frame),
      onPeerFrame: (env, payload) => this.writePeerFrame(env.channelId, payload.frame),
      onPeerDetach: (env) => this.proxyStates.delete(env.channelId),
      onConnectionFrameToServer: (env, payload) => this.routeConnectionFrameToServer(env.channelId, payload.frame),
      onConnectionFrameToClient: (env, payload) => this.routeConnectionFrameToClient(env.channelId, payload.frame),
      onUpgradeFinalize: (env, payload) =>
        void this.routeUpgradeFinalize(env.channelId, payload.prevSessionId, new Set(payload.keptIxes)),
      // `onAttachAck` is registered ad-hoc by `awaitAttachAck` for the duration of one call.
    })
    this.heartbeatTimer = unrefTimer(
      setInterval(() => void this.heartbeat().catch(() => {}), substrate.heartbeatIntervalMs),
    )
    void substrate.pinInstance()
  }

  // ── Channel registry hooks ──────────────────────────────────────────

  protected override onChannelRegistered(channelId: string): void {
    void this.substrate.pinChannel(channelId)
  }

  protected override onChannelUnregistered(channelId: string): void {
    const proxyInstance = this.homeAttached.get(channelId)
    if (proxyInstance !== undefined) {
      this.homeAttached.delete(channelId)
      void this.substrate.forward(proxyInstance, {
        channelId,
        fromInstance: this.selfInstanceId,
        direction: PROXY_DIRECTION.TO_PEER,
        payload: { kind: ENVELOPE_KIND.DETACH, reason: DETACH_REASON.PERMANENT },
      })
    }
    void this.substrate.unpinChannel(channelId)
  }

  // ── Connection lifecycle hooks ──────────────────────────────────────

  protected override onConnectionClosing(_connection: unknown, connId: string | null): void {
    // Independent of session state: an SSE→WS upgrade tears down the SSE session before
    // SSE closes, so a session-gated cleanup would leak the owner pin.
    if (connId !== null) this.unregisterOwnedConnection(connId)
  }

  protected override onSessionRegistered(connId: string, sessionId: string): void {
    this.ownedConnections.add(connId)
    void this.substrate.pinConnection(connId, { owner: this.selfInstanceId, sessionId })
  }

  private unregisterOwnedConnection(connId: string): void {
    if (!this.ownedConnections.delete(connId)) return
    void this.substrate.unpinConnection(connId)
  }

  /** Local hit short-circuits via super; otherwise locate the prior-wire owner in the
   *  cluster directory and forward UPGRADE_FINALIZE so it drains + retires its session.
   *  Owner runs its own finalizer (drain → fin → drain), then drops kept ix's silently
   *  (re-attached here) and fires `RECOVERY_FAILED` on dropped ones. */
  protected override async buildUpgradeFinalizer(
    ctrl: ReconcilePayload,
    transportConnId: string | null,
  ): Promise<(() => void) | null> {
    if (!ctrl.sessionId) return null
    const localFinalizer = await super.buildUpgradeFinalizer(ctrl, transportConnId)
    if (localFinalizer) return localFinalizer
    const lookupConnId = ctrl.prevConnId ?? transportConnId
    if (!lookupConnId) return null
    const record = await this.substrate.locateConnection(lookupConnId)
    if (record === null || record.owner === this.selfInstanceId) return null
    const owner = record.owner
    const prevSessionId = ctrl.sessionId
    const keptIxes = ctrl.open.map((entry) => entry.ix)
    return () =>
      void this.substrate.forward(owner, {
        channelId: lookupConnId,
        fromInstance: this.selfInstanceId,
        direction: PROXY_DIRECTION.TO_HOME,
        payload: { kind: ENVELOPE_KIND.UPGRADE_FINALIZE, prevSessionId, keptIxes },
      })
  }

  // ── Attach: local + remote race ─────────────────────────────────────

  protected override async attach(
    entry: ReconcilePayload['open'][number],
    send: SendFn,
  ): Promise<ChannelHandle | null> {
    const local = this.localChannels.get(entry.id)
    if (local) return this.attachLocalChannel(local, entry.ix, entry.lastSeq, send)

    if (!entry.initial) {
      // Established channel: one bounded pin lookup, no fanout wait.
      const home = await this.locateRemoteHome(entry.id, 0)
      if (home === null) return null
      return this.attachAsProxy(entry, home, send).catch(() => null)
    }

    // First reconcile: server-side `new ServerChannel()` may still be in flight. Race the
    // same-instance local-waiter against a substrate locate. Whichever resolves first wins.
    return new Promise<ChannelHandle | null>((resolve) => {
      let settled = false
      const settle = (h: ChannelHandle | null): void => {
        if (settled) return
        settled = true
        resolve(h)
      }
      const stopWaiter = this.waitForLocalChannel(entry.id, this.options.connectTtl, (channel) => {
        if (!channel) {
          // Local-waiter timed out. If the remote-locate branch hasn't resolved yet, it
          // will (or its own timeout will). Don't settle null here — let remote win or
          // its catch-all settle null.
          return
        }
        this.attachLocalChannel(channel, entry.ix, entry.lastSeq, send).then(settle, () => settle(null))
      })
      void this.locateRemoteHome(entry.id, this.options.connectTtl).then(
        (home) => {
          if (settled) return
          if (home === null) {
            // No remote pin within TTL — local-waiter may still fire; otherwise its timeout
            // is our backstop. Force settle on the next tick if neither resolved.
            settle(null)
            return
          }
          stopWaiter()
          this.attachAsProxy(entry, home, send).then(settle, () => settle(null))
        },
        () => settle(null),
      )
    })
  }

  /** Substrate contract: `locateRemoteHome` MUST filter self pins (the runtime's local
   *  waiter handles same-instance). A faulty substrate would silently bounce envelopes. */
  private async locateRemoteHome(channelId: string, timeoutMs: number): Promise<string | null> {
    const home = await this.substrate.locateRemoteHome(channelId, timeoutMs)
    assert(
      home !== this.selfInstanceId,
      `Substrate returned self instance "${home}" from locateRemoteHome — implementations must filter self pins`,
    )
    return home
  }

  /** Install the proxy state before sending ATTACH — replay frames from the home arrive
   *  ahead of the ack and need a routing entry to land on. Identity-equality cleanup on
   *  error avoids clobbering a superseding `attachAsProxy` for the same channel. */
  private async attachAsProxy(
    entry: ReconcilePayload['open'][number],
    homeInstance: string,
    send: SendFn,
  ): Promise<ChannelHandle> {
    const myState: ProxyChannelState = { homeInstance, ix: entry.ix, writeFrame: send }
    this.proxyStates.set(entry.id, myState)
    try {
      const lastClientSeq = await this.awaitAttachAck(entry.id, homeInstance, entry.ix, entry.lastSeq)
      return { kind: 'remote', channelId: entry.id, homeInstance, ix: entry.ix, lastClientSeq }
    } catch (err) {
      if (this.proxyStates.get(entry.id) === myState) this.proxyStates.delete(entry.id)
      throw err
    }
  }

  private awaitAttachAck(channelId: string, homeInstance: string, ix: number, lastSeq: number): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      const cleanup = (): void => {
        clearTimeout(timer)
        unsubscribe()
      }
      const timer = setTimeout(() => {
        cleanup()
        reject(new Error(`attach-ack for channel "${channelId}" timed out after ${SUBSTRATE_ATTACH_ACK_TIMEOUT_MS}ms`))
      }, SUBSTRATE_ATTACH_ACK_TIMEOUT_MS)
      const unsubscribe = this.substrate.listen({
        onAttachAck: (env, payload) => {
          if (env.channelId !== channelId) return
          cleanup()
          resolve(payload.lastClientSeq)
        },
      })
      void this.substrate.forward(homeInstance, {
        channelId,
        fromInstance: this.selfInstanceId,
        direction: PROXY_DIRECTION.TO_HOME,
        payload: {
          kind: ENVELOPE_KIND.ATTACH,
          reconnectTimeout: getServerConfig().channel.reconnectTimeout,
          ix,
          lastSeq,
        },
      })
    })
  }

  // ── Detach ──────────────────────────────────────────────────────────

  protected override detachHandle(h: ChannelHandle, reason: DetachReason): void {
    if (h.kind === 'local') return super.detachHandle(h, reason)
    this.detachProxyChannel(h.channelId, reason)
  }

  private detachProxyChannel(channelId: string, reason: DetachReason): void {
    const state = this.proxyStates.get(channelId)
    if (!state) return
    this.proxyStates.delete(channelId)
    void this.substrate.forward(state.homeInstance, {
      channelId,
      fromInstance: this.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.DETACH, reason },
    })
  }

  // ── Per-frame routing overrides ─────────────────────────────────────

  protected override onProxiedClientFrame(channelId: string, rawFrame: Uint8Array<ArrayBuffer>): void {
    const state = this.proxyStates.get(channelId)
    // `handleClientFrame` only reaches here when the session registry resolves the ix to
    // a remote handle — its existence implies a proxy state, hence the assert.
    assert(state, `forwardProxiedClientFrame called for unknown proxy channel "${channelId}"`)
    void this.substrate.forward(state.homeInstance, {
      channelId,
      fromInstance: this.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.FRAME, frame: rawFrame },
    })
  }

  override async routeClientFrame(channelId: string, home: string, rawFrame: Uint8Array<ArrayBuffer>): Promise<void> {
    if (home === this.selfInstanceId) return super.routeClientFrame(channelId, home, rawFrame)
    await this.substrate.forward(home, {
      channelId,
      fromInstance: this.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.FRAME, frame: rawFrame },
    })
  }

  override async sendConnectionFrameToClient(
    ownerInstance: string,
    connId: string,
    frame: Uint8Array<ArrayBuffer>,
  ): Promise<void> {
    if (ownerInstance === this.selfInstanceId) return super.sendConnectionFrameToClient(ownerInstance, connId, frame)
    await this.substrate.forward(ownerInstance, {
      channelId: connId,
      fromInstance: this.selfInstanceId,
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.CONNECTION_FRAME, frame },
    })
  }

  override async forwardConnectionFrameToServer(
    ownerInstance: string,
    connId: string,
    payload: Uint8Array<ArrayBuffer>,
  ): Promise<void> {
    if (ownerInstance === this.selfInstanceId) {
      // Owner is us but the local lookup missed — race with a close, drop silently.
      return
    }
    await this.substrate.forward(ownerInstance, {
      channelId: connId,
      fromInstance: this.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.CONNECTION_FRAME, frame: payload },
    })
  }

  // ── Substrate envelope handlers ─────────────────────────────────────

  /** A remote proxy claims this channel. Drain replay since `payload.lastSeq`, send
   *  ATTACH_ACK, then wire up the home channel's outbound to forward as TO_PEER frames. */
  private async attachHome(channelId: string, proxyInstance: string, payload: ProxyAttachPayload): Promise<void> {
    const channel = this.localChannels.get(channelId)
    if (!channel) return // channel shut down between proxy lookup and envelope arrival
    const replay = channel._replayBuffer
    assert(replay !== null, `Substrate attachHome on unregistered channel "${channel.id}"`)

    // Replay frames the client missed. They land on the wire ahead of the ack because sends
    // serialise by call order (synchronous transport enqueue).
    for (const frame of replay.getAfter(payload.lastSeq)) {
      void this.substrate.forward(proxyInstance, {
        channelId: channel.id,
        fromInstance: this.selfInstanceId,
        direction: PROXY_DIRECTION.TO_PEER,
        payload: { kind: ENVELOPE_KIND.FRAME, frame },
      })
    }

    void this.substrate.forward(proxyInstance, {
      channelId: channel.id,
      fromInstance: this.selfInstanceId,
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: channel._lastClientSeq },
    })

    const sender: PeerSender = {
      send: (frame, onCommit) => {
        // Commit-on-send: matches the local connection's semantics. The frame is "sent"
        // the moment the substrate accepts the envelope.
        onCommit?.()
        return this.substrate.forward(proxyInstance, {
          channelId: channel.id,
          fromInstance: this.selfInstanceId,
          direction: PROXY_DIRECTION.TO_PEER,
          payload: { kind: ENVELOPE_KIND.FRAME, frame },
        })
      },
    }
    this.homeAttached.set(channel.id, proxyInstance)
    channel._attachPeer(new IndexedPeer(sender, payload.ix, replay))
  }

  private detachHome(channelId: string, fromInstance: string, reason: DetachReason): void {
    if (this.homeAttached.get(channelId) !== fromInstance) return
    this.homeAttached.delete(channelId)
    const channel = this.localChannels.get(channelId)
    if (!channel) return
    switch (reason) {
      case DETACH_REASON.PERMANENT:
        channel._onPeerClose()
        return
      case DETACH_REASON.TRANSIENT:
        channel._onPeerDisconnect(getServerConfig().channel.reconnectTimeout)
        return
      case DETACH_REASON.RECOVERY_FAILED:
        channel._onPeerRecoveryFailure()
        return
    }
  }

  /** Accept inbound client→server FRAMEs from any cluster instance (data POSTs round-robin
   *  freely). `homeAttached` is only consulted for outbound TO_PEER; channel seq dedups. */
  private dispatchHomeFrame(channelId: string, rawFrame: Uint8Array): void {
    const channel = this.localChannels.get(channelId)
    if (!channel) return
    channel._dispatchFrame(decode(rawFrame as Uint8Array<ArrayBuffer>) as ChannelFrame)
  }

  private writePeerFrame(channelId: string, frame: Uint8Array): void {
    const state = this.proxyStates.get(channelId)
    if (!state) return // proxy gone away; envelope is stale
    void state.writeFrame(frame as Uint8Array<ArrayBuffer>)
  }

  /** Re-inject a client→server alias-0 frame on this owner's local connection. */
  private routeConnectionFrameToServer(connId: string, frame: Uint8Array): void {
    const connection = this.connectionsByConnId.get(connId)
    if (connection === undefined) return
    void this.onConnectionRawMessage(connection, frame as Uint8Array<ArrayBuffer>)
  }

  /** Push a server→client frame onto this owner's local SSE wire (forwarded here by a
   *  non-owner that received the long-lived stream-request POST). */
  private routeConnectionFrameToClient(connId: string, frame: Uint8Array): void {
    const connection = this.connectionsByConnId.get(connId)
    if (connection === undefined) return
    const entry = this.connectionStates.get(connection)
    if (entry) entry.transport.sendNow(connection, frame as Uint8Array<ArrayBuffer>)
  }

  /** Owner-side UPGRADE_FINALIZE: run the local finalizer, then silently retire kept ix's
   *  (re-attached on the requester) and fire `RECOVERY_FAILED` on dropped ones. */
  private async routeUpgradeFinalize(
    prevConnId: string,
    prevSessionId: string,
    keptIxes: ReadonlySet<number>,
  ): Promise<void> {
    const finalizer = this.sessionFinalizers.get(prevSessionId)
    if (!finalizer) return // local connection already torn down — nothing to drain
    finalizer()
    this.sessionFinalizers.delete(prevSessionId)
    this.unregisterOwnedConnection(prevConnId)
    const session = this.sessions.removeSession(prevSessionId)
    if (!session) return
    for (const [ix, handle] of session) {
      if (!keptIxes.has(ix)) {
        this.detachHandle(handle, DETACH_REASON.RECOVERY_FAILED)
      } else if (handle.kind === 'remote') {
        // Channel was re-attached on the requester via the new home; this proxy entry is stale.
        this.proxyStates.delete(handle.channelId)
      }
    }
  }

  // ── Heartbeat + peer liveness ───────────────────────────────────────

  /** Refresh own pins (batched per kind) + detect dead peers: dead OWNERs disconnect their
   *  channels here; dead HOMEs get an `error` ctrl on the local wire so only their channel
   *  closes on the client. */
  private async heartbeat(): Promise<void> {
    const channelIds = Array.from(this.localChannels.keys())
    const connIds = Array.from(this.ownedConnections)
    await Promise.all([
      this.substrate.pinInstance(),
      this.substrate.refreshChannels(channelIds),
      this.substrate.refreshConnections(connIds),
    ])
    await this.checkPeerLiveness()
  }

  private async checkPeerLiveness(): Promise<void> {
    const peers = new Set<string>()
    for (const owner of this.homeAttached.values()) peers.add(owner)
    for (const state of this.proxyStates.values()) peers.add(state.homeInstance)
    if (peers.size === 0) return
    const aliveness = await Promise.all(
      Array.from(peers, async (peer) => [peer, await this.substrate.isInstanceAlive(peer)] as const),
    )
    for (const [peer, alive] of aliveness) {
      if (!alive) this.onPeerInstanceDead(peer)
    }
  }

  /** The dead instance may have been an owner for some channels (we're their home) and a
   *  home for others (we're their owner). Handle both, idempotently. */
  private onPeerInstanceDead(deadInstance: string): void {
    const reconnectTimeout = getServerConfig().channel.reconnectTimeout
    for (const [channelId, owner] of this.homeAttached) {
      if (owner !== deadInstance) continue
      this.homeAttached.delete(channelId)
      this.localChannels.get(channelId)?._onPeerDisconnect(reconnectTimeout)
    }
    for (const [channelId, state] of this.proxyStates) {
      if (state.homeInstance !== deadInstance) continue
      this.proxyStates.delete(channelId)
      void state.writeFrame(encode.error(state.ix))
    }
  }

  // ── Dispose ─────────────────────────────────────────────────────────

  override dispose(): void {
    this.unsubscribe()
    clearInterval(this.heartbeatTimer)
    this.homeAttached.clear()
    this.proxyStates.clear()
    this.ownedConnections.clear()
    void this.substrate.unpinInstance()
    super.dispose()
  }
}
