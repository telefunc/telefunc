export { getTelefuncChannelHooks }

import { defineHooks, type Peer } from 'crossws'
import type { ServerChannel } from './channel.js'
import { getChannelRegistry } from './channel.js'
import { TAG, encode, decode, encodeCtrl } from '../shared-ws.js'
import type { CtrlMessage } from '../shared-ws.js'
import { hasProp } from '../../utils/hasProp.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { ReplayBuffer } from '../replay-buffer.js'

// Client sends CTRL ping every 5s. If the server doesn't receive a ping
// within PING_TIMEOUT_MS (2× the interval), the client is dead.
const PING_TIMEOUT_MS = 10_000
const SERVER_REPLAY_BYTES = 256 * 1024

// ── Global state — flat map, persists across reconnects ──
//
// Keyed by channelId (each channel's own ID) for direct O(1) lookup.
// Survives WS reconnects so replay buffers and seq tracking are preserved
// regardless of which channelId appears in the WS URL.

type ChannelEntry = { channel: ServerChannel; lastClientSeq: number; replay: ReplayBuffer }

type PeerState = {
  ixMap: Map<number, ChannelEntry>
  pingTimer: ReturnType<typeof setTimeout> | null
  timedOut: boolean
}

const globalObject = getGlobalObject('ws-state.ts', {
  peerStates: new Map<string, PeerState>(),
  wsChannels: new Map<string, ChannelEntry>(),
})

const { peerStates, wsChannels } = globalObject

function getOrCreatePeerState(peer: Peer): PeerState {
  let s = peerStates.get(peer.id)
  if (!s) {
    s = { ixMap: new Map(), pingTimer: null, timedOut: false }
    peerStates.set(peer.id, s)
  }
  return s
}

/** Wraps a crossws peer, encodes frames with a fixed channel index.
 *  Assigns sequence numbers and stores frames in the replay buffer. */
class IndexedPeer {
  constructor(
    private ws: Peer,
    private index: number,
    private replay: ReplayBuffer,
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
  }
}

function getTelefuncChannelHooks() {
  return defineHooks({
    open(peer) {
      getOrCreatePeerState(peer)
      resetPingTimer(peer)
    },

    message(peer, message) {
      const state = getOrCreatePeerState(peer)
      const frame = decode(message.uint8Array())

      switch (frame.tag) {
        case TAG.CTRL:
          handleCtrl(frame.ctrl, peer)
          break
        case TAG.TEXT: {
          const entry = state.ixMap.get(frame.index)
          if (!entry) break
          if (frame.seq && frame.seq <= entry.lastClientSeq) break
          if (frame.seq) entry.lastClientSeq = frame.seq
          entry.channel._onPeerMessage(frame.text)
          break
        }
        case TAG.BINARY: {
          const entry = state.ixMap.get(frame.index)
          if (!entry) break
          if (frame.seq && frame.seq <= entry.lastClientSeq) break
          if (frame.seq) entry.lastClientSeq = frame.seq
          entry.channel._onPeerBinaryMessage(frame.data)
          break
        }
      }
    },

    close(peer, details) {
      const state = getOrCreatePeerState(peer)
      clearPingTimer(peer)
      const isPermanent = !state.timedOut && (details?.code === 1000 || details?.code === 1001)
      if (isPermanent) {
        for (const entry of state.ixMap.values()) {
          if (!entry.channel.isClosed) entry.channel._onPeerClose()
        }
        state.ixMap.clear()
      } else {
        for (const entry of state.ixMap.values()) {
          entry.channel._onPeerDisconnect()
        }
      }
      peerStates.delete(peer.id)
    },

    error(peer) {
      clearPingTimer(peer)
      peerStates.delete(peer.id)
    },
  })
}

// ── Ping timer ──

function resetPingTimer(peer: Peer): void {
  const state = getOrCreatePeerState(peer)
  clearPingTimer(peer)
  state.pingTimer = setTimeout(() => {
    state.pingTimer = null
    peer.terminate()
    state.timedOut = true
  }, PING_TIMEOUT_MS)
  if (hasProp(state.pingTimer, 'unref', 'function')) {
    state.pingTimer.unref()
  }
}

function clearPingTimer(peer: Peer): void {
  const state = peerStates.get(peer.id)
  if (state?.pingTimer) {
    clearTimeout(state.pingTimer)
    state.pingTimer = null
  }
}

// ── CTRL handling ──

function handleCtrl(ctrl: CtrlMessage, peer: Peer): void {
  const state = getOrCreatePeerState(peer)
  switch (ctrl.t) {
    // ── Heartbeat ──
    case 'ping': {
      peer.send(encodeCtrl({ t: 'pong' }))
      resetPingTimer(peer)
      break
    }

    // ── Channel lifecycle ──
    case 'close': {
      const entry = state.ixMap.get(ctrl.ix)
      if (!entry) break
      state.ixMap.delete(ctrl.ix)
      entry.channel._onPeerClose()
      break
    }

    // ── Reconciliation ──
    //
    // 1. Replay: send any frames the client missed (seq > lastSeq).
    // 2. For each channel the client mentions:
    //    - Alive in registry → attach at client's ix, include in response
    //    - Dead → simply omit from response (client diffs to discover)
    // 3. For channels in ixMap NOT mentioned by client → _onPeerClose()
    // 4. Respond with reconciled { open } = all channels we actually attached
    // 5. Client diffs its map against our response → closes its orphans
    case 'reconcile': {
      const registry = getChannelRegistry()
      const reconciledIxs = new Set<number>()
      const attached: { id: string; ix: number; lastSeq: number }[] = []

      // Snapshot old ixMap for orphan detection, then rebuild
      const prevIxMap = new Map(state.ixMap)
      state.ixMap.clear()

      // Replay missed frames and attach peers for alive channels
      for (const { id, ix, lastSeq } of ctrl.open) {
        reconciledIxs.add(ix)
        const ch = registry.get(id)
        if (!ch || ch.isClosed) continue

        // Get existing entry (with replay buffer) or create new
        const existing = wsChannels.get(id)
        const replay = existing?.replay ?? new ReplayBuffer(SERVER_REPLAY_BYTES)
        const lastClientSeq = existing?.lastClientSeq ?? 0

        // Replay missed server→client frames
        const missed = replay.getAfter(lastSeq)
        for (const frame of missed) peer.send(frame)

        const entry: ChannelEntry = { channel: ch, lastClientSeq, replay }
        state.ixMap.set(ix, entry)

        wsChannels.set(id, entry)
        ch.onClose(() => wsChannels.delete(id))

        ch.attachPeer(new IndexedPeer(peer, ix, replay))
        attached.push({ id, ix, lastSeq: lastClientSeq })
      }

      // Channels in previous ixMap that client didn't mention → client closed them
      for (const [ix, entry] of prevIxMap) {
        if (reconciledIxs.has(ix)) continue
        if (!entry.channel.isClosed) entry.channel._onPeerClose()
      }

      // Respond with what we attached — client diffs to find its own orphans
      peer.send(encodeCtrl({ t: 'reconciled', open: attached }))
      break
    }

    // ── Backpressure ──
    case 'pause': {
      const entry = state.ixMap.get(ctrl.ix)
      if (entry) entry.channel._onPeerPause()
      break
    }
    case 'resume': {
      const entry = state.ixMap.get(ctrl.ix)
      if (entry) entry.channel._onPeerResume()
      break
    }
  }
}
