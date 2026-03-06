export { getTelefuncChannelHooks }

import { defineHooks } from 'crossws'
import type { ServerChannel } from './channel.js'
import { getChannelRegistry } from './channel.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { TAG, encode, decode, encodeCtrl } from '../shared-ws.js'
import type { CtrlMessage } from '../shared-ws.js'
import { hasProp } from '../../utils/hasProp.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { ReplayBuffer } from '../replay-buffer.js'

// Client sends CTRL ping every 5s. If the server doesn't receive a ping
// within PING_TIMEOUT_MS (2× the interval), the client is dead.
const PING_TIMEOUT_MS = 10_000
const SERVER_REPLAY_BYTES = 256 * 1024

// ── Global WsState — persists across reconnects ──
//
// Keyed by the primary channelId (from the WS URL). Tracks which channels
// (by index) belong to each client session, so reconcile can diff.
// This is the SOLE source of truth for channel→index mappings.

type WsStateEntry = { channel: ServerChannel; lastClientSeq: number; replay: ReplayBuffer }
type WsState = {
  channels: Map<number, WsStateEntry>
}

const globalObject = getGlobalObject('ws-state.ts', {
  wsStates: new Map<string, WsState>(),
})

function getOrCreateWsState(primaryId: string): WsState {
  let state = globalObject.wsStates.get(primaryId)
  if (!state) {
    state = { channels: new Map() }
    globalObject.wsStates.set(primaryId, state)
  }
  return state
}

function deleteWsState(primaryId: string): void {
  globalObject.wsStates.delete(primaryId)
}

// ── Per-connection context (only pingTimer + primaryId) ──

declare module 'crossws' {
  interface PeerContext {
    primaryId: string
    pingTimer: ReturnType<typeof setTimeout> | null
  }
}

type WsPeer = { send(data: string | Uint8Array): void; terminate(): void }

/** Wraps a crossws peer, encodes frames with a fixed channel index.
 *  Assigns sequence numbers and stores frames in the replay buffer. */
class IndexedPeer {
  constructor(
    private ws: WsPeer,
    private index: number,
    private replay: ReplayBuffer,
    private onDetach: () => void,
  ) {}

  sendText(data: string): void {
    const seq = this.replay.nextSeq()
    const frame = encode.text(this.index, data, seq)
    this.replay.push(seq, frame)
    this.ws.send(frame)
  }

  sendBinary(data: Uint8Array): void {
    const seq = this.replay.nextSeq()
    const frame = encode.binary(this.index, data, seq)
    this.replay.push(seq, frame)
    this.ws.send(frame)
  }

  close(): void {
    try {
      this.ws.send(encodeCtrl({ t: 'close', ix: this.index }))
    } catch {
      /* WS may already be closed */
    }
    this.onDetach()
  }
}

function getTelefuncChannelHooks() {
  return defineHooks({
    upgrade(req) {
      const url = new URL(req.url, 'http://localhost')
      if (url.pathname !== getServerConfig().telefuncUrl) return

      const channelId = url.searchParams.get('channelId')
      if (!channelId) return
      // Validate the primary channel still exists (alive or in grace period)
      if (!getChannelRegistry().has(channelId)) return

      return {
        context: {
          primaryId: channelId,
          pingTimer: null as ReturnType<typeof setTimeout> | null,
        },
      }
    },

    open(peer) {
      resetPingTimer(peer.context, peer)
    },

    message(peer, message) {
      const ctx = peer.context
      const state = getOrCreateWsState(ctx.primaryId)
      const frame = decode(message.uint8Array())

      switch (frame.tag) {
        case TAG.CTRL:
          handleCtrl(frame.ctrl, state, ctx, peer)
          break
        case TAG.TEXT: {
          const entry = state.channels.get(frame.index)
          if (!entry) break
          if (frame.seq && frame.seq <= entry.lastClientSeq) break
          if (frame.seq) entry.lastClientSeq = frame.seq
          entry.channel._onPeerMessage(frame.text)
          break
        }
        case TAG.BINARY: {
          const entry = state.channels.get(frame.index)
          if (!entry) break
          if (frame.seq && frame.seq <= entry.lastClientSeq) break
          if (frame.seq) entry.lastClientSeq = frame.seq
          entry.channel._onPeerBinaryMessage(frame.data)
          break
        }
      }
    },

    close(peer, details) {
      const ctx = peer.context
      clearPingTimer(ctx)
      const state = getOrCreateWsState(ctx.primaryId)
      // 1000 = explicit programmatic close
      // 1001 = browser going away (refresh, navigation, tab close)
      // Both are permanent — client JS state is gone, channels can never reconnect.
      // Only network drops (1006, etc.) get a grace period for reconnect.
      const isPermanent = details?.code === 1000 || details?.code === 1001
      if (isPermanent) {
        for (const { channel } of state.channels.values()) {
          if (!channel.isClosed) channel._onPeerClose()
        }
        deleteWsState(ctx.primaryId)
      } else {
        // Connection lost — each channel enters disconnect/grace period.
        // WsState + replay buffer persist for the next reconcile.
        for (const { channel } of state.channels.values()) channel._onPeerDisconnect()
      }
    },

    error(peer) {
      clearPingTimer(peer.context)
    },
  })
}

// ── Ping timer ──

type PeerCtx = { pingTimer: ReturnType<typeof setTimeout> | null }

function resetPingTimer(ctx: PeerCtx, peer: WsPeer): void {
  clearPingTimer(ctx)
  ctx.pingTimer = setTimeout(() => {
    ctx.pingTimer = null
    peer.terminate()
  }, PING_TIMEOUT_MS)
  if (hasProp(ctx.pingTimer, 'unref', 'function')) {
    ctx.pingTimer.unref()
  }
}

function clearPingTimer(ctx: PeerCtx): void {
  if (ctx.pingTimer) {
    clearTimeout(ctx.pingTimer)
    ctx.pingTimer = null
  }
}

// ── CTRL handling ──

function handleCtrl(ctrl: CtrlMessage, state: WsState, ctx: PeerCtx, peer: WsPeer): void {
  switch (ctrl.t) {
    // ── Heartbeat ──
    case 'ping': {
      peer.send(encodeCtrl({ t: 'pong' }))
      resetPingTimer(ctx, peer)
      break
    }

    // ── Channel lifecycle ──
    case 'close': {
      const entry = state.channels.get(ctrl.ix)
      if (!entry) break
      state.channels.delete(ctrl.ix)
      entry.channel._onPeerClose()
      break
    }

    // ── Reconciliation ──
    //
    // 1. Replay: send any frames the client missed (seq > lastSeq).
    // 2. For each channel the client mentions:
    //    - Alive in registry → attach at client's ix, include in response
    //    - Dead → simply omit from response (client diffs to discover)
    // 3. For each channel in WsState NOT mentioned by client → _onPeerClose()
    // 4. Respond with reconciled { open } = all channels we actually attached
    // 5. Client diffs its map against our response → closes its orphans
    case 'reconcile': {
      const registry = getChannelRegistry()
      const clientIxs = new Set<number>()
      const attached: { id: string; ix: number; lastSeq: number }[] = []

      // Replay missed frames and attach peers for alive channels
      for (const { id, ix, lastSeq } of ctrl.open) {
        clientIxs.add(ix)
        const ch = registry.get(id)
        if (!ch || ch.isClosed) continue

        // Get or create per-channel replay buffer
        const existing = state.channels.get(ix)
        const replay = existing?.replay ?? new ReplayBuffer(SERVER_REPLAY_BYTES)
        const lastClientSeq = existing?.lastClientSeq ?? 0

        // Replay missed server→client frames
        const missed = replay.getAfter(lastSeq)
        for (const frame of missed) peer.send(frame)

        state.channels.set(ix, { channel: ch, lastClientSeq, replay })
        ch.attachPeer(new IndexedPeer(peer, ix, replay, () => state.channels.delete(ix)))
        attached.push({ id, ix, lastSeq: lastClientSeq })
      }

      // Channels in WsState that client didn't mention → client closed them
      for (const [ix, { channel }] of state.channels) {
        if (!clientIxs.has(ix)) {
          state.channels.delete(ix)
          if (!channel.isClosed) channel._onPeerClose()
        }
      }

      // Respond with what we attached — client diffs to find its own orphans
      peer.send(encodeCtrl({ t: 'reconciled', open: attached }))
      break
    }

    // ── Backpressure ──
    case 'pause': {
      const entry = state.channels.get(ctrl.ix)
      if (entry) entry.channel._onPeerPause()
      break
    }
    case 'resume': {
      const entry = state.channels.get(ctrl.ix)
      if (entry) entry.channel._onPeerResume()
      break
    }
  }
}
