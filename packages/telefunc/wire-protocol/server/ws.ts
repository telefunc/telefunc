export { getTelefuncChannelHooks }
export type { TelefuncWebSocketOptions }

import { defineHooks, type Peer } from 'crossws'
import type { ServerChannel } from './channel.js'
import { getChannelRegistry, onChannelCreated, setChannelDefaults } from './channel.js'
import { TAG, decode, encodeCtrl } from '../shared-ws.js'
import type { CtrlMessage } from '../shared-ws.js'
import { hasProp } from '../../utils/hasProp.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { IndexedPeer } from './IndexedPeer.js'
import {
  WS_PING_INTERVAL,
  WS_PING_INTERVAL_MIN,
  WS_SERVER_REPLAY_BUFFER,
  WS_CLIENT_REPLAY_BUFFER,
  WS_RECONNECT_TIMEOUT,
  WS_IDLE_TIMEOUT,
  WS_CHANNEL_CONNECT_TTL_MS,
  WS_CHANNEL_SEND_BUFFER,
} from '../constants.js'

type TelefuncWebSocketOptions = {
  /**
   * How long (in milliseconds) to keep the connection alive after a client
   * disconnects, waiting for a reconnect before permanently closing all channels.
   *
   * @default 60_000
   */
  reconnectTimeout?: number
  /**
   * How long (in milliseconds) to keep the WebSocket connection open after
   * the last channel is closed, before closing it.
   *
   * A warm connection avoids the cost of a new WebSocket handshake when
   * a new channel is opened shortly after.
   *
   * @default 60_000
   */
  idleTimeout?: number
  /**
   * How often (in milliseconds) the client sends a ping to the server.
   * The server considers the client dead after 2× this interval without a ping.
   *
   * Clamped to a minimum of 1000ms.
   *
   * @default 5_000
   */
  pingInterval?: number
  /**
   * Size (in bytes) of the per-channel replay buffer on the server.
   * Frames within this budget are replayed to a reconnecting client.
   * Larger values survive more missed data at the cost of server memory.
   *
   * @default 262_144 (256 KB)
   */
  serverReplayBuffer?: number
  /**
   * Size (in bytes) of the per-channel replay buffer on the client.
   * Frames within this budget are replayed to the server on reconnect.
   * Larger values survive more missed data at the cost of client memory.
   *
   * @default 1_048_576 (1 MB)
   */
  clientReplayBuffer?: number
  /**
   * How long (ms) the server waits for the client to connect to a newly created channel.
   * If the client does not connect within this window, the channel is closed automatically.
   *
   * @default 5_000
   */
  channelConnectTtl?: number
  /**
   * Maximum bytes buffered per channel for messages sent before the client connects.
   * Covers the same window as `channelConnectTtl`.
   *
   * @default 524_288 (512 KB)
   */
  channelSendBuffer?: number
}

type ChannelEntry = { channel: ServerChannel; lastClientSeq: number; replay: ReplayBuffer }

type PeerState = {
  ixMap: Map<number, ChannelEntry>
  pingTimer: ReturnType<typeof setTimeout> | null
  terminatePermanently: boolean | null
  reconciling: boolean
}

function getTelefuncChannelHooks(opts?: TelefuncWebSocketOptions) {
  const reconnectTimeout = opts?.reconnectTimeout ?? WS_RECONNECT_TIMEOUT
  const idleTimeout = opts?.idleTimeout ?? WS_IDLE_TIMEOUT
  const pingInterval = Math.max(opts?.pingInterval ?? WS_PING_INTERVAL, WS_PING_INTERVAL_MIN)
  const pingDeadline = pingInterval * 2
  const serverReplayBuffer = opts?.serverReplayBuffer ?? WS_SERVER_REPLAY_BUFFER
  const clientReplayBuffer = opts?.clientReplayBuffer ?? WS_CLIENT_REPLAY_BUFFER
  const channelConnectTtl = opts?.channelConnectTtl ?? WS_CHANNEL_CONNECT_TTL_MS
  const channelSendBuffer = opts?.channelSendBuffer ?? WS_CHANNEL_SEND_BUFFER
  // Frames must survive the full worst-case reconnect window:
  //   pingDeadline     — both sides start their timer at the last ping; they fire concurrently
  //   reconnectTimeout — client reconnect window after detection
  //   1 000 ms         — RTT margin for the reconnect handshake + reconcile to arrive
  const replayMaxAge = pingDeadline + reconnectTimeout + 1_000

  // Propagate channel-level defaults so createChannel() picks them up automatically.
  setChannelDefaults({ connectTtlMs: channelConnectTtl, sendBufferBytes: channelSendBuffer })

  const peerStates = new Map<string, PeerState>()
  const wsChannels = new Map<string, ChannelEntry>()

  function getOrCreatePeerState(peer: Peer): PeerState {
    let s = peerStates.get(peer.id)
    if (!s) {
      s = { ixMap: new Map(), pingTimer: null, terminatePermanently: null, reconciling: false }
      peerStates.set(peer.id, s)
    }
    return s
  }

  function resetPingTimer(peer: Peer): void {
    const state = getOrCreatePeerState(peer)
    clearPingTimer(peer)
    state.pingTimer = setTimeout(() => {
      state.pingTimer = null
      if (state.reconciling) return
      peer.terminate()
      state.terminatePermanently = false
    }, pingDeadline)
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

  async function handleCtrl(ctrl: CtrlMessage, peer: Peer): Promise<void> {
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

      case 'abort': {
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
      //    - Dead + defer → wait up to 5s for it to be created (in-flight POST)
      //    - Dead + !ctrl.defer → omit from response (client diffs to discover)
      // 3. For channels in ixMap NOT mentioned by client → _onPeerClose()
      // 4. Respond with reconciled { open } = all channels we actually attached
      // 5. Client diffs its map against our response → closes its orphans
      case 'reconcile': {
        state.reconciling = true
        resetPingTimer(peer)
        const registry = getChannelRegistry()
        const reconciledIxs = new Set<number>()
        const attached: { id: string; ix: number; lastSeq: number }[] = []

        // Snapshot old ixMap for orphan detection, then rebuild
        const prevIxMap = new Map(state.ixMap)
        state.ixMap.clear()

        // If the reconcile contains deferred channels (client has in-flight POST), wait for
        // each missing ServerChannel to be created before attaching.
        await Promise.all(
          ctrl.open
            .filter(({ id, defer }) => defer && (!registry.get(id) || registry.get(id)!.isClosed))
            .map(
              ({ id }) =>
                new Promise<void>((resolve) => {
                  onChannelCreated(id, resolve)
                  setTimeout(resolve, channelConnectTtl)
                }),
            ),
        )

        // Replay missed frames and attach peers for alive channels
        for (const { id, ix, lastSeq } of ctrl.open) {
          reconciledIxs.add(ix)
          const ch = registry.get(id)
          if (!ch || ch.isClosed) continue

          // Get existing entry (with replay buffer) or create new
          const existing = wsChannels.get(id)
          const replay = existing?.replay ?? new ReplayBuffer(serverReplayBuffer, replayMaxAge)
          const lastClientSeq = existing?.lastClientSeq ?? 0

          // Replay missed server→client frames
          const missed = replay.getAfter(lastSeq)
          for (const frame of missed) peer.send(frame)

          const entry: ChannelEntry = { channel: ch, lastClientSeq, replay }
          state.ixMap.set(ix, entry)

          wsChannels.set(id, entry)
          ch.onClose(() => {
            wsChannels.delete(id)
            replay.dispose()
          })

          ch.attachPeer(new IndexedPeer(peer, ix, replay))
          attached.push({ id, ix, lastSeq: lastClientSeq })
        }

        // Channels in previous ixMap that client didn't mention → client closed them
        for (const [ix, entry] of prevIxMap) {
          if (reconciledIxs.has(ix)) continue
          if (!entry.channel.isClosed) entry.channel._onPeerClose()
        }

        // Respond with what we attached — client diffs to find its own orphans
        state.reconciling = false
        resetPingTimer(peer)
        peer.send(
          encodeCtrl({
            t: 'reconciled',
            open: attached,
            reconnectTimeout,
            idleTimeout,
            pingInterval,
            clientReplayBuffer,
          }),
        )
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

  return defineHooks({
    open(peer) {
      getOrCreatePeerState(peer)
      resetPingTimer(peer)
    },

    async message(peer, message) {
      const state = getOrCreatePeerState(peer)
      try {
        const frame = decode(message.uint8Array())

        switch (frame.tag) {
          case TAG.CTRL:
            await handleCtrl(frame.ctrl, peer)
            break
          case TAG.TEXT: {
            const entry = state.ixMap.get(frame.index)
            if (!entry) break
            if (frame.seq && frame.seq <= entry.lastClientSeq) break
            if (frame.seq) entry.lastClientSeq = frame.seq
            entry.channel._onPeerMessage(frame.text)
            break
          }
          case TAG.TEXT_ACK_REQ: {
            const entry = state.ixMap.get(frame.index)
            if (!entry) break
            if (frame.seq && frame.seq <= entry.lastClientSeq) break
            if (frame.seq) entry.lastClientSeq = frame.seq
            entry.channel._onPeerAckReqMessage(frame.text, frame.seq)
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
          case TAG.ACK_RES: {
            const entry = state.ixMap.get(frame.index)
            if (!entry) break
            entry.channel._onPeerAckRes(frame.ackedSeq, frame.text)
            break
          }
        }
      } catch {
        // Malformed frame from a misbehaving client — permanently close channels and terminate.
        state.terminatePermanently = true
        peer.terminate()
      }
    },

    close(peer, details) {
      const state = getOrCreatePeerState(peer)
      clearPingTimer(peer)
      const isPermanent =
        state.terminatePermanently === true ||
        (state.terminatePermanently === null && (details?.code === 1000 || details?.code === 1001))
      if (isPermanent) {
        for (const entry of state.ixMap.values()) {
          if (!entry.channel.isClosed) entry.channel._onPeerClose()
        }
        state.ixMap.clear()
      } else {
        for (const entry of state.ixMap.values()) {
          entry.channel._onPeerDisconnect(reconnectTimeout)
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
