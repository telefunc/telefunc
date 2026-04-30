export { ChannelMux }
export type { DetachReason, SendFn }

import { assert } from '../../utils/assert.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { TAG, decode, encodeCtrl } from '../shared-ws.js'
import type { CtrlMessage, CtrlReconcile } from '../shared-ws.js'
import { SUBSTRATE_ATTACH_ACK_TIMEOUT_MS } from '../constants.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import type { ServerChannel } from './channel.js'
import { ENVELOPE_KIND, PROXY_DIRECTION } from './substrate.js'
import type { ChannelSubstrate, ProxyAttachPayload } from './substrate.js'

type DetachReason = 'transient' | 'permanent' | 'recovery-failed'

/** Invoking preserves wire order via the connection's serialised send chain. */
type SendFn = (frame: Uint8Array<ArrayBuffer>, onCommit?: () => void) => void | Promise<void>

type ChannelHandle =
  | { kind: 'local'; channel: ServerChannel; ix: number }
  | { kind: 'proxy'; channelId: string; ix: number; lastClientSeq: number }

type ProxyChannelState = {
  homeInstance: string
  writeFrame: SendFn
}

/** Forward (`bySession`: sessionId → ix → handle) for per-frame routing, reverse
 *  (`byChannel`: channelId → sessionId → ix) for O(bindings) channel eviction.
 *  Mutations are atomic across both. Proxy handles aren't reverse-indexed. */
class SessionRegistry {
  private readonly bySession = new Map<string, Map<number, ChannelHandle>>()
  private readonly byChannel = new Map<string, Map<string, number>>()

  get(sessionId: string, ix: number): ChannelHandle | undefined {
    return this.bySession.get(sessionId)?.get(ix)
  }

  setSession(sessionId: string, handles: Iterable<ChannelHandle>): void {
    this.removeSession(sessionId)
    const session = new Map<number, ChannelHandle>()
    for (const h of handles) {
      session.set(h.ix, h)
      if (h.kind !== 'local') continue
      let bindings = this.byChannel.get(h.channel.id)
      if (!bindings) {
        bindings = new Map()
        this.byChannel.set(h.channel.id, bindings)
      }
      bindings.set(sessionId, h.ix)
    }
    this.bySession.set(sessionId, session)
  }

  /** Returns the removed session so callers can drive per-handle lifecycle side effects. */
  removeSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    const session = this.bySession.get(sessionId)
    if (!session) return undefined
    this.bySession.delete(sessionId)
    for (const handle of session.values()) {
      if (handle.kind !== 'local') continue
      const bindings = this.byChannel.get(handle.channel.id)
      if (!bindings) continue
      bindings.delete(sessionId)
      if (bindings.size === 0) this.byChannel.delete(handle.channel.id)
    }
    return session
  }

  removeChannel(channelId: string): void {
    const bindings = this.byChannel.get(channelId)
    if (!bindings) return
    this.byChannel.delete(channelId)
    for (const [sessionId, ix] of bindings) this.bySession.get(sessionId)?.delete(ix)
  }

  clear(): void {
    this.bySession.clear()
    this.byChannel.clear()
  }
}

class ChannelMux {
  private readonly substrate: ChannelSubstrate
  private readonly localChannels = new Map<string, ServerChannel>()
  /** Same-process attach waiters, fired synchronously by `registerChannel` so a
   *  deferred-attach race resolves locally without involving the substrate. */
  private readonly localWaiters = new Map<string, Set<(channel: ServerChannel) => void>>()
  private readonly sessions = new SessionRegistry()
  /** Channels substrate-attached here (HOME role) → owning proxy instance. The
   *  `fromInstance` check rejects stale frames/detaches from a handed-off proxy. */
  private readonly homeAttached = new Map<string, string>()
  private readonly proxyStates = new Map<string, ProxyChannelState>()
  private readonly unsubscribe: () => void
  private readonly heartbeatTimer: ReturnType<typeof setInterval>

  constructor(substrate: ChannelSubstrate) {
    this.substrate = substrate
    this.unsubscribe = substrate.listen({
      onAttach: (env, payload) => this.attachHome(env.channelId, env.fromInstance, payload),
      onDetach: (env, payload) => this.detachHome(env.channelId, env.fromInstance, payload.reason),
      onHomeFrame: (env, payload) => this.dispatchHomeFrame(env.channelId, env.fromInstance, payload.frame),
      onPeerFrame: (env, payload) => this.writePeerFrame(env.channelId, payload.frame),
      onPeerDetach: (env) => this.proxyStates.delete(env.channelId),
      // `onAttachAck` is registered ad-hoc by `awaitAttachAck` for the duration of one call.
    })
    this.heartbeatTimer = unrefTimer(
      setInterval(() => void this.heartbeat().catch(() => {}), substrate.heartbeatIntervalMs),
    )
  }

  /** Callers must not invoke `channel._registerChannel()` directly. */
  registerChannel(channel: ServerChannel<any, any>): void {
    channel._registerChannel()
    this.localChannels.set(channel.id, channel)
    const waiters = this.localWaiters.get(channel.id)
    if (waiters) {
      this.localWaiters.delete(channel.id)
      for (const cb of waiters) cb(channel)
    }
    channel._onShutdown(() => this.unregisterChannel(channel.id))
    void this.substrate.pinChannel(channel.id)
  }

  unregisterChannel(channelId: string): void {
    this.localChannels.delete(channelId)
    this.sessions.removeChannel(channelId)
    const proxyInstance = this.homeAttached.get(channelId)
    if (proxyInstance !== undefined) {
      this.homeAttached.delete(channelId)
      void this.substrate.forward(proxyInstance, {
        channelId,
        fromInstance: this.substrate.selfInstanceId,
        direction: PROXY_DIRECTION.TO_PEER,
        payload: { kind: ENVELOPE_KIND.DETACH, reason: 'permanent' },
      })
    }
    void this.substrate.unpinChannel(channelId)
  }

  findLocal(channelId: string): ServerChannel | null {
    return this.localChannels.get(channelId) ?? null
  }

  hasChannels(): boolean {
    return this.localChannels.size > 0
  }

  /** One batched substrate call per tick — implementations pipeline so the heartbeat
   *  cost stays O(1) round trips regardless of locally-hosted channel count. */
  private async heartbeat(): Promise<void> {
    if (this.localChannels.size === 0) return
    await this.substrate.refreshPins(Array.from(this.localChannels.keys()))
  }

  async reconcileSession(args: {
    prevSessionId: string | undefined
    newSessionId: string
    open: CtrlReconcile['open']
    send: SendFn
  }): Promise<CtrlReconcile['open']> {
    const handles = (await Promise.all(args.open.map((entry) => this.attach(entry, args.send)))).filter(
      (h): h is ChannelHandle => h !== null,
    )

    // Channels in the previous session that the client did NOT re-include are recovery-failed.
    if (args.prevSessionId) {
      const prev = this.sessions.removeSession(args.prevSessionId)
      if (prev) {
        const keptIxes = new Set(handles.map((h) => h.ix))
        for (const [ix, prevHandle] of prev) if (!keptIxes.has(ix)) this.detachHandle(prevHandle, 'recovery-failed')
      }
    }
    this.sessions.setSession(args.newSessionId, handles)

    return handles.map((h) =>
      h.kind === 'local'
        ? { id: h.channel.id, ix: h.ix, lastSeq: h.channel._lastClientSeq }
        : { id: h.channelId, ix: h.ix, lastSeq: h.lastClientSeq },
    )
  }

  handleClientFrame(sessionId: string, rawFrame: Uint8Array<ArrayBuffer>): void {
    const decoded = decode(rawFrame)
    assert(decoded.tag !== TAG.CTRL, 'handleClientFrame called with a ctrl frame — use handleClientCtrl')
    const h = this.sessions.get(sessionId, decoded.index)
    // Race: client closed a channel and the server reconciled it out, but a frame for the dropped
    // ix is still in flight. Indistinguishable from a bogus-ix violation without a per-session
    // history of detached ixs — drop, don't escalate.
    if (!h) return
    if (h.kind === 'local') h.channel._dispatchFrame(decoded)
    else void this._forwardClientFrame(h.channelId, rawFrame)
  }

  handleClientCtrl(sessionId: string, ctrl: Extract<CtrlMessage, { ix: number }>): void {
    const h = this.sessions.get(sessionId, ctrl.ix)
    if (!h) return // same close-race as handleClientFrame
    if (h.kind === 'local') h.channel._dispatchCtrl(ctrl)
    else void this._forwardClientFrame(h.channelId, encodeCtrl(ctrl))
  }

  detachSession(sessionId: string, reason: DetachReason): void {
    const session = this.sessions.removeSession(sessionId)
    if (!session) return
    for (const handle of session.values()) this.detachHandle(handle, reason)
  }

  /** No `_didShutdown` guard needed: `attachLocalChannel` returns null on shutdown during its
   *  replay-drain await, and `unregisterChannel` synchronously evicts the reverse-indexed
   *  session entry — stale handles never reach this path. */
  private detachHandle(h: ChannelHandle, reason: DetachReason): void {
    if (h.kind === 'proxy') {
      this._detachProxy(h.channelId, reason)
      return
    }
    this.dispatchPeerDetach(h.channel, reason)
  }

  /** Single source for `DetachReason → _onPeer*()` so local and home-from-proxy paths agree. */
  private dispatchPeerDetach(channel: ServerChannel, reason: DetachReason): void {
    switch (reason) {
      case 'permanent':
        channel._onPeerClose()
        return
      case 'transient':
        channel._onPeerDisconnect(getServerConfig().channel.reconnectTimeout)
        return
      case 'recovery-failed':
        channel._onPeerRecoveryFailure()
        return
    }
  }

  dispose(): void {
    this.unsubscribe()
    clearInterval(this.heartbeatTimer)
    this.localChannels.clear()
    this.localWaiters.clear()
    this.homeAttached.clear()
    this.proxyStates.clear()
    this.sessions.clear()
  }

  /** Substrate contract: `locateRemoteHome` MUST filter self pins (the runtime's local waiter
   *  handles same-instance). A faulty substrate would silently bounce envelopes through
   *  the cluster. */
  private async locateRemoteHome(channelId: string, timeoutMs: number): Promise<string | null> {
    const home = await this.substrate.locateRemoteHome(channelId, timeoutMs)
    assert(
      home !== this.substrate.selfInstanceId,
      `Substrate returned self instance "${home}" from locateRemoteHome — implementations must filter self pins`,
    )
    return home
  }

  /** Two regimes:
   *   - `initial: true` — first reconcile for this channel; server creation may still be in
   *     flight. Race local-create + cross-instance pin + TTL up to `connectTtl`.
   *   - falsy — established channel; one bounded pin lookup, fail fast if absent.
   *
   *  The `claim()` race-arbiter ensures attach side effects run on exactly one branch. */
  private async attach(entry: CtrlReconcile['open'][number], send: SendFn): Promise<ChannelHandle | null> {
    const local = this.findLocal(entry.id)
    if (local) return attachLocalChannel(local, { ix: entry.ix, lastSeq: entry.lastSeq, send })

    if (!entry.initial) {
      // `timeoutMs=0` = synchronous lookup, no fanout wait. Fails fast if not pinned.
      const home = await this.locateRemoteHome(entry.id, 0)
      if (home === null) return null
      return this.attachAsProxy(entry, home, send).then(
        (h) => h,
        () => null,
      )
    }

    const ttl = getServerConfig().channel.connectTtl
    return new Promise<ChannelHandle | null>((resolve) => {
      let settled = false
      let waiterSet!: Set<(channel: ServerChannel) => void>
      let timer!: ReturnType<typeof setTimeout>

      const localWaiter = (channel: ServerChannel) => {
        if (!claim()) return
        attachLocalChannel(channel, { ix: entry.ix, lastSeq: entry.lastSeq, send }).then(resolve, () => resolve(null))
      }

      /** Wins the race exactly once and tears down the loser branches. */
      const claim = (): boolean => {
        if (settled) return false
        settled = true
        waiterSet.delete(localWaiter)
        if (waiterSet.size === 0) this.localWaiters.delete(entry.id)
        clearTimeout(timer)
        return true
      }

      waiterSet = this.localWaiters.get(entry.id) ?? new Set()
      this.localWaiters.set(entry.id, waiterSet)
      waiterSet.add(localWaiter)
      timer = setTimeout(() => {
        if (claim()) resolve(null)
      }, ttl)

      void this.locateRemoteHome(entry.id, ttl).then((home) => {
        if (home === null || !claim()) return
        // Surface attach-ack timeout as `null` so the rest of the reconcile proceeds.
        this.attachAsProxy(entry, home, send).then(resolve, () => resolve(null))
      })
    })
  }

  /** Routing entry must be installed before the ack arrives — the home sends replay frames
   *  ahead of the ack. Identity-equality cleanup on error prevents clobbering a superseding
   *  `attachAsProxy` for the same channel. */
  private async attachAsProxy(
    entry: CtrlReconcile['open'][number],
    homeInstance: string,
    send: SendFn,
  ): Promise<ChannelHandle> {
    const myProxyState: ProxyChannelState = { homeInstance, writeFrame: send }
    this.proxyStates.set(entry.id, myProxyState)
    try {
      const lastClientSeq = await this.awaitAttachAck(entry.id, homeInstance, entry.ix, entry.lastSeq)
      return { kind: 'proxy', channelId: entry.id, ix: entry.ix, lastClientSeq }
    } catch (err) {
      if (this.proxyStates.get(entry.id) === myProxyState) this.proxyStates.delete(entry.id)
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
        fromInstance: this.substrate.selfInstanceId,
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

  private _forwardClientFrame(channelId: string, frame: Uint8Array<ArrayBuffer>): Promise<void> {
    const state = this.proxyStates.get(channelId)
    assert(state, `_forwardClientFrame called for unknown proxy channel "${channelId}"`)
    return this.substrate.forward(state.homeInstance, {
      channelId,
      fromInstance: this.substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.FRAME, frame },
    })
  }

  private _detachProxy(channelId: string, reason: DetachReason): void {
    const state = this.proxyStates.get(channelId)
    if (!state) return
    this.proxyStates.delete(channelId)
    void this.substrate.forward(state.homeInstance, {
      channelId,
      fromInstance: this.substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.DETACH, reason },
    })
  }

  private writePeerFrame(channelId: string, frame: Uint8Array): void {
    const state = this.proxyStates.get(channelId)
    if (!state) return // proxy connection has gone away; envelope is stale
    void state.writeFrame(frame as Uint8Array<ArrayBuffer>)
  }

  private attachHome(channelId: string, proxyInstance: string, payload: ProxyAttachPayload): void {
    const channel = this.findLocal(channelId)
    if (!channel) return // channel shut down between proxy lookup and envelope arrival
    const replay = channel._replayBuffer
    assert(replay !== null, `Substrate attachHome on unregistered channel "${channel.id}"`)

    // Replay frames the client missed. They land on the wire ahead of `CtrlReconciled`
    // because the proxy's per-connection sendChain serialises by enqueue order.
    for (const frame of replay.getAfter(payload.lastSeq)) {
      void this.substrate.forward(proxyInstance, {
        channelId: channel.id,
        fromInstance: this.substrate.selfInstanceId,
        direction: PROXY_DIRECTION.TO_PEER,
        payload: { kind: ENVELOPE_KIND.FRAME, frame },
      })
    }

    void this.substrate.forward(proxyInstance, {
      channelId: channel.id,
      fromInstance: this.substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: channel._lastClientSeq },
    })

    const sender: PeerSender = {
      send: (frame, onCommit) => {
        // Commit-on-send: matches the local connection's semantics. The frame is "sent" the
        // moment the substrate accepts the envelope.
        onCommit?.()
        return this.substrate.forward(proxyInstance, {
          channelId: channel.id,
          fromInstance: this.substrate.selfInstanceId,
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
    const channel = this.findLocal(channelId)
    if (!channel) return
    this.dispatchPeerDetach(channel, reason)
  }

  private dispatchHomeFrame(channelId: string, fromInstance: string, rawFrame: Uint8Array): void {
    if (this.homeAttached.get(channelId) !== fromInstance) return
    const channel = this.findLocal(channelId)
    if (!channel) return
    channel._dispatchFrame(decode(rawFrame as Uint8Array<ArrayBuffer>))
  }
}

/** Drains replay frames missed since `lastSeq` then attaches an `IndexedPeer`. Returns null
 *  if the channel shut down during the drain. */
async function attachLocalChannel(
  channel: ServerChannel,
  args: { ix: number; lastSeq: number; send: SendFn },
): Promise<ChannelHandle | null> {
  const replay = channel._replayBuffer
  assert(replay !== null, `Channel "${channel.id}" attached without a replay buffer`)
  for (const frame of replay.getAfter(args.lastSeq)) {
    const pending = args.send(frame as Uint8Array<ArrayBuffer>)
    if (pending) await pending
  }
  if (channel._didShutdown) return null
  const sender: PeerSender = { send: args.send }
  channel._attachPeer(new IndexedPeer(sender, args.ix, replay))
  return { kind: 'local', channel, ix: args.ix }
}
