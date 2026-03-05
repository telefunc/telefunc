export { getTelefuncChannelHooks }

import { defineHooks } from 'crossws'
import type { ServerChannel } from './channel.js'
import { getChannelRegistry } from './channel.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { TAG, encode, decode, encodeCtrl } from '../shared-ws.js'
import type { CtrlMessage } from '../shared-ws.js'
import { hasProp } from '../../utils/hasProp.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'

// Client sends CTRL ping every 5s. If the server doesn't receive a ping
// within PING_TIMEOUT_MS (2× the interval), the client is dead.
const PING_TIMEOUT_MS = 10_000

// ── Global WsState — persists across reconnects ──
//
// Keyed by the primary channelId (from the WS URL). Tracks which channels
// (by index) belong to each client session, so reconcile can diff.
// This is the SOLE source of truth for channel→index mappings.

type WsStateEntry = { channel: ServerChannel }
type WsState = Map<number, WsStateEntry>

const globalObject = getGlobalObject('ws-state.ts', {
  wsStates: new Map<string, WsState>(),
})

function getOrCreateWsState(primaryId: string): WsState {
  let state = globalObject.wsStates.get(primaryId)
  if (!state) {
    state = new Map()
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

/** Wraps a crossws peer, encodes frames with a fixed channel index. */
class IndexedPeer {
  constructor(
    private ws: WsPeer,
    private index: number,
    private onDetach: () => void,
  ) {}

  sendText(data: string): void {
    this.ws.send(encode.text(this.index, data))
  }

  sendBinary(data: Uint8Array): void {
    this.ws.send(encode.binary(this.index, data))
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
          const entry = state.get(frame.index)
          if (entry) entry.channel._onPeerMessage(frame.text)
          break
        }
        case TAG.BINARY: {
          const entry = state.get(frame.index)
          if (entry) entry.channel._onPeerBinaryMessage(frame.data)
          break
        }
      }
    },

    close(peer, details) {
      const ctx = peer.context
      clearPingTimer(ctx)
      const state = getOrCreateWsState(ctx.primaryId)
      const isDrop = details?.code !== 1000
      if (isDrop) {
        // Connection lost — each channel enters disconnect/grace period.
        // WsState persists for the next reconcile.
        for (const { channel } of state.values()) channel._onPeerDisconnect()
      } else {
        // Intentional close (code 1000) — tear down all channels + WsState.
        for (const { channel } of state.values()) {
          if (channel.isOpen) channel._onPeerClose()
        }
        deleteWsState(ctx.primaryId)
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

    // ── Channel lifecycle (client owns ix) ──
    case 'open': {
      const channel = getChannelRegistry().get(ctrl.id)
      if (!channel || !channel.isOpen) {
        peer.send(encodeCtrl({ t: 'close', ix: ctrl.ix }))
        return
      }
      state.set(ctrl.ix, { channel })
      channel.attachPeer(new IndexedPeer(peer, ctrl.ix, () => state.delete(ctrl.ix)))
      peer.send(encodeCtrl({ t: 'opened', ix: ctrl.ix }))
      break
    }
    case 'close': {
      const entry = state.get(ctrl.ix)
      if (!entry) break
      state.delete(ctrl.ix)
      entry.channel._onPeerClose()
      break
    }

    // ── Reconciliation ──
    //
    // Symmetric diff:
    //
    // 1. For each channel the client mentions:
    //    - Alive in registry → attach at client's ix, include in response
    //    - Dead → simply omit from response (client diffs to discover)
    // 2. For each channel in WsState NOT mentioned by client → _onPeerClose()
    // 3. Respond with reconciled { open } = all channels we actually attached
    // 4. Client diffs its map against our response → closes its orphans
    case 'reconcile': {
      const registry = getChannelRegistry()
      const clientIxs = new Set<number>()
      const attached: { id: string; ix: number }[] = []

      for (const { id, ix } of ctrl.open) {
        clientIxs.add(ix)
        const ch = registry.get(id)
        if (ch && ch.isOpen) {
          state.set(ix, { channel: ch })
          ch.attachPeer(new IndexedPeer(peer, ix, () => state.delete(ix)))
          attached.push({ id, ix })
        }
      }

      // Channels in WsState that client didn't mention → client closed them
      for (const [ix, { channel }] of state) {
        if (!clientIxs.has(ix)) {
          state.delete(ix)
          if (channel.isOpen) channel._onPeerClose()
        }
      }

      // Respond with what we attached — client diffs to find its own orphans
      peer.send(encodeCtrl({ t: 'reconciled', open: attached }))
      break
    }

    // ── Backpressure ──
    case 'pause': {
      const entry = state.get(ctrl.ix)
      if (entry) entry.channel._onPeerPause()
      break
    }
    case 'resume': {
      const entry = state.get(ctrl.ix)
      if (entry) entry.channel._onPeerResume()
      break
    }
  }
}
