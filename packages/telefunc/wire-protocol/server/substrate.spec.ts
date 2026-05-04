import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { decode, encode, type ChannelFrame } from '../shared-ws.js'
import { ServerChannel } from './channel.js'
import {
  decodeProxyEnvelope,
  DETACH_REASON,
  dispatchEnvelope,
  encodeProxyEnvelope,
  ENVELOPE_KIND,
  InMemoryChannelSubstrate,
  PROXY_DIRECTION,
  _resetChannelSubstrateForTesting,
  getChannelMux,
  type ChannelSubstrate,
  type ChannelSubstrateHandlers,
  type ConnectionRecord,
  type ProxyEnvelope,
} from './substrate.js'

// ===========================================================================
// Section 1 — Foundational primitives
//
// Below the scenarios live three pure functions: encode/decode envelopes,
// dispatch routing, and the InMemoryChannelSubstrate stub. These get unit
// tests because they have no I/O and a bug in any one of them would surface
// as a confusing failure deep inside a scenario test, not at the layer that
// caused it. The tests here pin behavior, not byte layout.
// ===========================================================================

const HEADER = { channelId: 'room:abc', fromInstance: 'instance-A' } as const
const roundtrip = (env: ProxyEnvelope): ProxyEnvelope => decodeProxyEnvelope(encodeProxyEnvelope(env))

describe('proxy envelope codec', () => {
  it('round-trips ATTACH preserving u32 timeout/ix/lastSeq', () => {
    const env: ProxyEnvelope = {
      ...HEADER,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 12_345, ix: 7, lastSeq: 42 },
    }
    expect(roundtrip(env)).toEqual(env)
  })

  it('round-trips ATTACH_ACK preserving lastClientSeq', () => {
    const env: ProxyEnvelope = {
      ...HEADER,
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: 9001 },
    }
    expect(roundtrip(env)).toEqual(env)
  })

  it.each([DETACH_REASON.TRANSIENT, DETACH_REASON.PERMANENT, DETACH_REASON.RECOVERY_FAILED])(
    'round-trips DETACH reason=%i without remapping',
    (reason) => {
      const env: ProxyEnvelope = {
        ...HEADER,
        direction: PROXY_DIRECTION.TO_HOME,
        payload: { kind: ENVELOPE_KIND.DETACH, reason },
      }
      const decoded = roundtrip(env)
      if (decoded.payload.kind !== ENVELOPE_KIND.DETACH) throw new Error('Expected DETACH')
      expect(decoded.payload.reason).toBe(reason)
    },
  )

  // High-bit bytes (0x80, 0xff) catch sign-bit mishandling and any UTF-8 transcoding
  // that touches the binary frame. Real wire frames span the full byte range.
  it('round-trips FRAME preserving high-bit bytes (0x00, 0x7f, 0x80, 0xff)', () => {
    const env: ProxyEnvelope = {
      ...HEADER,
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.FRAME, frame: new Uint8Array([0x00, 0x7f, 0x80, 0xff]) },
    }
    expect(roundtrip(env)).toEqual(env)
  })

  it('round-trips CONNECTION_FRAME', () => {
    const env: ProxyEnvelope = {
      ...HEADER,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.CONNECTION_FRAME, frame: new Uint8Array([0xab, 0xcd]) },
    }
    expect(roundtrip(env)).toEqual(env)
  })

  // channelIds in real apps include emoji + non-ASCII (`chat:café`, `room:🎉-中文-…`).
  // If the length-prefix uses `s.length` instead of byte length, the next field truncates.
  it('preserves multi-byte UTF-8 in channelId AND fromInstance', () => {
    const env: ProxyEnvelope = {
      channelId: 'room:café-🎉-中文',
      fromInstance: 'instance-α-β',
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 0, ix: 0, lastSeq: 0 },
    }
    expect(roundtrip(env)).toEqual(env)
  })

  // Decoder must reject garbage rather than producing a half-built envelope. The exact
  // error message is impl detail; only that it throws is the behavioral contract.
  it.each([
    ['truncated header', new Uint8Array([ENVELOPE_KIND.ATTACH])],
    ['unknown kind byte', new Uint8Array([0xff, PROXY_DIRECTION.TO_HOME, 0, 0, 0, 0, 0, 0, 0, 0])],
    ['unknown direction byte', new Uint8Array([ENVELOPE_KIND.ATTACH, 0xff, 0, 0, 0, 0, 0, 0, 0, 0])],
  ])('throws on %s', (_label, bytes) => {
    expect(() => decodeProxyEnvelope(bytes)).toThrow()
  })
})

describe('dispatchEnvelope — direction-sensitive routing', () => {
  // Only DETACH and FRAME use direction to pick a handler. The bug class: someone
  // swaps onHomeFrame ↔ onPeerFrame or onDetach ↔ onPeerDetach in the switch.
  // Both swaps would compile fine and silently misroute every cross-instance frame.

  function envelope(direction: ProxyEnvelope['direction'], payload: ProxyEnvelope['payload']): ProxyEnvelope {
    return { channelId: 'c', fromInstance: 'i', direction, payload }
  }

  it('FRAME splits on direction: TO_HOME → onHomeFrame, TO_PEER → onPeerFrame', () => {
    const log: string[] = []
    const handlers: ChannelSubstrateHandlers = {
      onHomeFrame: () => log.push('home'),
      onPeerFrame: () => log.push('peer'),
    }
    const frame = { kind: ENVELOPE_KIND.FRAME, frame: new Uint8Array() } as const
    dispatchEnvelope(handlers, envelope(PROXY_DIRECTION.TO_HOME, frame))
    dispatchEnvelope(handlers, envelope(PROXY_DIRECTION.TO_PEER, frame))
    expect(log).toEqual(['home', 'peer'])
  })

  it('DETACH splits on direction: TO_HOME → onDetach, TO_PEER → onPeerDetach', () => {
    const log: string[] = []
    const handlers: ChannelSubstrateHandlers = {
      onDetach: () => log.push('home'),
      onPeerDetach: () => log.push('peer'),
    }
    const detach = { kind: ENVELOPE_KIND.DETACH, reason: DETACH_REASON.TRANSIENT } as const
    dispatchEnvelope(handlers, envelope(PROXY_DIRECTION.TO_HOME, detach))
    dispatchEnvelope(handlers, envelope(PROXY_DIRECTION.TO_PEER, detach))
    expect(log).toEqual(['home', 'peer'])
  })

  // Contract: every consumer subscribes with a partial handler set. Removing the `?.`
  // calls would crash for any kind that isn't subscribed.
  it('treats every handler as optional (missing handler = no-op)', () => {
    expect(() =>
      dispatchEnvelope(
        {},
        envelope(PROXY_DIRECTION.TO_HOME, {
          kind: ENVELOPE_KIND.ATTACH,
          reconnectTimeout: 0,
          ix: 0,
          lastSeq: 0,
        }),
      ),
    ).not.toThrow()
  })
})

describe('InMemoryChannelSubstrate — single-process default', () => {
  // Three contracts the mux relies on. If any of these flips, the mux's local-waiter
  // fallback path breaks silently in single-process deployments.

  it('locateRemoteHome resolves null → forces mux to use the local-waiter path', async () => {
    const substrate = new InMemoryChannelSubstrate()
    await substrate.pinChannel('room:foo')
    expect(await substrate.locateRemoteHome('room:foo', 50)).toBe(null)
  })

  it('listen returns a callable unsubscribe → mux can detach on swap/dispose', () => {
    const substrate = new InMemoryChannelSubstrate()
    const unsubscribe = substrate.listen({ onAttach: () => {} })
    expect(typeof unsubscribe).toBe('function')
    expect(() => unsubscribe()).not.toThrow()
  })

  // Critical: forward MUST NOT loopback to the local listener. The mux already serves
  // local channels from its registry, so loopback would cause double-delivery.
  it('forward swallows envelopes — no in-process loopback to listeners', async () => {
    const substrate = new InMemoryChannelSubstrate()
    const received: ProxyEnvelope[] = []
    substrate.listen({ onAttach: (env) => received.push(env), onHomeFrame: (env) => received.push(env) })

    await substrate.forward('any-target', {
      channelId: 'room:foo',
      fromInstance: substrate.selfInstanceId,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 1000, ix: 0, lastSeq: 0 },
    })

    expect(received).toEqual([])
  })
})

// ===========================================================================
// Section 2 — Real scenarios
//
// These tests drive the substrate end-to-end through the actual ChannelMux,
// using a LoopbackSubstrate as the "remote bus" stand-in. Each scenario is a
// multi-step interaction that mirrors what happens in production when a
// client lands on a different instance from the channel's home.
// ===========================================================================

class LoopbackSubstrate implements ChannelSubstrate {
  readonly selfInstanceId: string
  readonly heartbeatIntervalMs = 60_000
  private readonly watchers = new Map<string, Set<(home: string) => void>>()
  private readonly remoteHomes = new Map<string, string>()
  private readonly listeners = new Set<ChannelSubstrateHandlers>()
  /** Every envelope handed to forward(). Tests assert on these. */
  readonly capturedForwards: { target: string; envelope: ProxyEnvelope }[] = []

  constructor(selfInstanceId: string = 'home-A') {
    this.selfInstanceId = selfInstanceId
  }

  /** Pin a channelId to a remote instance so `locateRemoteHome` can resolve. */
  setRemoteHome(channelId: string, instance: string): void {
    this.remoteHomes.set(channelId, instance)
    const set = this.watchers.get(channelId)
    if (!set) return
    this.watchers.delete(channelId)
    for (const cb of set) cb(instance)
  }

  async pinChannel(_channelId: string): Promise<void> {}
  async unpinChannel(_channelId: string): Promise<void> {}
  async refreshChannels(_channelIds: readonly string[]): Promise<void> {}
  async locateRemoteHome(channelId: string, _timeoutMs: number): Promise<string | null> {
    const remote = this.remoteHomes.get(channelId)
    if (remote !== undefined) return remote
    return new Promise((resolve) => {
      let set = this.watchers.get(channelId)
      if (!set) {
        set = new Set()
        this.watchers.set(channelId, set)
      }
      set.add(resolve)
    })
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
    return true
  }
  async forward(target: string, envelope: ProxyEnvelope): Promise<void> {
    this.capturedForwards.push({ target, envelope })
  }
  listen(handlers: ChannelSubstrateHandlers): () => void {
    this.listeners.add(handlers)
    return () => {
      this.listeners.delete(handlers)
    }
  }
  async dispose(): Promise<void> {
    this.watchers.clear()
    this.listeners.clear()
  }

  /** Feed an envelope into the mux as if it arrived from a peer instance. */
  deliver(envelope: ProxyEnvelope): void {
    for (const handlers of this.listeners) dispatchEnvelope(handlers, envelope)
  }
  toPeer(
    payload: Extract<ProxyEnvelope['payload'], { kind: typeof ENVELOPE_KIND.FRAME | typeof ENVELOPE_KIND.ATTACH_ACK }>,
    channelId: string,
    fromInstance: string,
  ): void {
    this.deliver({ channelId, fromInstance, direction: PROXY_DIRECTION.TO_PEER, payload })
  }
  toHome(
    payload: Extract<
      ProxyEnvelope['payload'],
      { kind: typeof ENVELOPE_KIND.ATTACH | typeof ENVELOPE_KIND.FRAME | typeof ENVELOPE_KIND.DETACH }
    >,
    channelId: string,
    fromInstance: string,
  ): void {
    this.deliver({ channelId, fromInstance, direction: PROXY_DIRECTION.TO_HOME, payload })
  }
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

afterEach(() => _resetChannelSubstrateForTesting(new InMemoryChannelSubstrate()))

// ───────────────────────────────────────────────────────────────────────────
// Scenario A — Cold-start handoff
//
// A telefunction creates a channel and begins sending while the client is
// still in flight. The client then lands on a remote instance, which sends
// an ATTACH envelope. The home must construct a substrate-backed peer,
// flush the prePeerBuffer through it, and ack the attach so the proxy can
// seed its CtrlReconciled with the home's lastClientSeq.
// ───────────────────────────────────────────────────────────────────────────

describe('scenario: client lands on a remote instance — home flushes its buffer to the new proxy', () => {
  const PROXY = 'proxy-B'
  const CLIENT_IX = 7

  let substrate: LoopbackSubstrate
  let homeChannel: ServerChannel<{ text: string }, { text: string }>

  beforeEach(() => {
    substrate = new LoopbackSubstrate('home-A')
    _resetChannelSubstrateForTesting(substrate)
    homeChannel = new ServerChannel<{ text: string }, { text: string }>()
    getChannelMux().registerChannel(homeChannel as ServerChannel)
  })

  it('flushes the buffered messages on first attach, in order, and acks the attach', () => {
    // The home publishes three messages BEFORE any client peer is attached.
    // These land in `_prePeerBuffer` because there is nothing to send through yet.
    void homeChannel.send({ text: 'one' })
    void homeChannel.send({ text: 'two' })
    void homeChannel.send({ text: 'three' })

    expect(substrate.capturedForwards).toEqual([])

    // Client lands on PROXY. PROXY sends an ATTACH envelope to the home.
    substrate.toHome(
      { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 30_000, ix: CLIENT_IX, lastSeq: 0 },
      homeChannel.id,
      PROXY,
    )

    // The home must respond with two things:
    //   1. an ATTACH_ACK so the proxy can seed reconcile with the home's lastClientSeq
    //   2. all three buffered frames, forwarded as TO_PEER envelopes targeted at PROXY
    const toPeer = substrate.capturedForwards.filter((e) => e.envelope.direction === PROXY_DIRECTION.TO_PEER)
    const acks = toPeer.filter(({ envelope }) => envelope.payload.kind === ENVELOPE_KIND.ATTACH_ACK)
    const frames = toPeer.filter(({ envelope }) => envelope.payload.kind === ENVELOPE_KIND.FRAME)

    expect(acks).toHaveLength(1)
    expect(frames).toHaveLength(3)
    for (const { target, envelope } of frames) {
      expect(target).toBe(PROXY)
      expect(envelope.channelId).toBe(homeChannel.id)
      expect(envelope.fromInstance).toBe(substrate.selfInstanceId)
    }
  })

  it('also flushes binary messages (publishBinary path goes through the same buffer)', () => {
    void homeChannel.sendBinary(new Uint8Array([0xaa, 0xbb]))
    void homeChannel.sendBinary(new Uint8Array([0xcc, 0xdd]))

    substrate.toHome(
      { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 30_000, ix: CLIENT_IX, lastSeq: 0 },
      homeChannel.id,
      PROXY,
    )

    const frames = substrate.capturedForwards
      .filter((e) => e.envelope.direction === PROXY_DIRECTION.TO_PEER)
      .filter(({ envelope }) => envelope.payload.kind === ENVELOPE_KIND.FRAME)
    expect(frames).toHaveLength(2)
  })

  // Reconnect-with-history: when the client reconnects after disconnecting, it tells the
  // proxy "I last received seq N". The proxy needs to know how many client→server frames
  // the home already saw, so it carries the home's `_lastClientSeq` in CtrlReconciled.
  // The substrate must surface this via the ATTACH_ACK payload.
  it('attach-ack carries the home channel’s current lastClientSeq for reconnect replay', () => {
    // Simulate the home having already received 7 client→server frames before the client
    // moved to a new proxy.
    ;(homeChannel as unknown as { _lastClientSeq: number })._lastClientSeq = 7

    substrate.toHome({ kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 30_000, ix: 1, lastSeq: 0 }, homeChannel.id, PROXY)

    const ack = substrate.capturedForwards.find(({ envelope }) => envelope.payload.kind === ENVELOPE_KIND.ATTACH_ACK)
    expect(ack).toBeDefined()
    expect(ack!.target).toBe(PROXY)
    if (ack!.envelope.payload.kind !== ENVELOPE_KIND.ATTACH_ACK) throw new Error('Expected ATTACH_ACK')
    expect(ack!.envelope.payload.lastClientSeq).toBe(7)
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Scenario B — Multi-tab / multi-region fan-out
//
// The same channel can be attached from multiple proxies at once: a user has
// the app open in two tabs, each tab lands on a different region's instance.
// A single home-side send must be forwarded to every attached proxy independently.
// ───────────────────────────────────────────────────────────────────────────

describe('scenario: same channel attached from two different proxies — home fans out independently', () => {
  it('every attached proxy receives its own attach-ack from the home', () => {
    const substrate = new LoopbackSubstrate('home-A')
    _resetChannelSubstrateForTesting(substrate)
    const homeChannel = new ServerChannel<{ text: string }, { text: string }>()
    getChannelMux().registerChannel(homeChannel as ServerChannel)

    void homeChannel.send({ text: 'broadcast-me' })

    // Two proxies attach at different ixes (different sessions, same channel).
    substrate.toHome(
      { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 30_000, ix: 7, lastSeq: 0 },
      homeChannel.id,
      'proxy-B',
    )
    substrate.toHome(
      { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 30_000, ix: 11, lastSeq: 0 },
      homeChannel.id,
      'proxy-C',
    )

    const ackTargets = substrate.capturedForwards
      .filter(({ envelope }) => envelope.payload.kind === ENVELOPE_KIND.ATTACH_ACK)
      .map(({ target }) => target)

    // The contract is "every attached proxy gets its own ack" — sort to avoid asserting on
    // the order of envelopes, since order is irrelevant for parallel tabs.
    expect(ackTargets.sort()).toEqual(['proxy-B', 'proxy-C'])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Scenario C — Full proxy lifecycle from the proxy side
//
// This is the inverse of Scenario A. The client lands on THIS instance, but
// the channel's home is elsewhere. The mux must reconcile the session: send
// an ATTACH envelope, wait for ATTACH_ACK, then route every client frame to
// the home as TO_HOME, and write every TO_PEER frame from the home to the
// local socket. On detach, the proxy state must be cleaned up so late TO_PEER
// frames are dropped instead of writing to a closed socket.
// ───────────────────────────────────────────────────────────────────────────

describe('scenario: proxied channel — open, exchange frames, detach', () => {
  const HOME = 'home-A'
  const CHANNEL_ID = 'broadcast:room:foo'
  const SESSION_ID = 'session-1'
  const CLIENT_IX = 3

  let substrate: LoopbackSubstrate
  let writtenFrames: Uint8Array[]

  /** Drives reconcileSession + delivers the ATTACH_ACK so the proxy is fully open. */
  async function openProxiedChannel(): Promise<void> {
    const openPromise = getChannelMux().reconcileSession({
      prevSessionId: undefined,
      newSessionId: SESSION_ID,
      open: [{ id: CHANNEL_ID, ix: CLIENT_IX, lastSeq: 12 }],
      send: (frame) => {
        writtenFrames.push(frame)
      },
    })
    await tick() // wait for substrate's `locateRemoteHome` + `forward(attach)` to settle
    substrate.toPeer({ kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: 42 }, CHANNEL_ID, HOME)
    await openPromise
  }

  beforeEach(() => {
    substrate = new LoopbackSubstrate('proxy-self')
    substrate.setRemoteHome(CHANNEL_ID, HOME)
    _resetChannelSubstrateForTesting(substrate)
    writtenFrames = []
  })

  // The full open handshake: reconcile sends ATTACH with ix + lastSeq the client had,
  // home replies with ATTACH_ACK carrying its own lastClientSeq, reconcile resolves
  // with the merged view (home-reported lastSeq replaces client's optimistic lastSeq).
  it('reconcile resolves with the home-reported lastSeq after attach-ack', async () => {
    const openPromise = getChannelMux().reconcileSession({
      prevSessionId: undefined,
      newSessionId: SESSION_ID,
      open: [{ id: CHANNEL_ID, ix: CLIENT_IX, lastSeq: 12 }],
      send: () => {},
    })
    await tick()

    // The proxy sent a TO_HOME ATTACH envelope to the resolved home.
    const attach = substrate.capturedForwards[0]
    expect(attach?.target).toBe(HOME)
    if (attach?.envelope.payload.kind !== ENVELOPE_KIND.ATTACH) throw new Error('Expected ATTACH')
    expect(attach.envelope.payload.ix).toBe(CLIENT_IX)
    expect(attach.envelope.payload.lastSeq).toBe(12)

    // Until ATTACH_ACK arrives, the open promise is pending. Ack with seq=42.
    substrate.toPeer({ kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: 42 }, CHANNEL_ID, HOME)
    expect(await openPromise).toEqual([{ id: CHANNEL_ID, ix: CLIENT_IX, lastSeq: 42, home: HOME }])
  })

  // Bidirectional frame routing: client → proxy → home as TO_HOME, and home → proxy →
  // socket via TO_PEER. Both directions and both wire kinds (text + binary).
  it.each([
    ['text', encode.text(CLIENT_IX, '"hello"', 1)],
    ['binary', encode.binary(CLIENT_IX, new Uint8Array([0x01, 0x02, 0x03]), 1)],
  ])('routes a client %s frame to the home as TO_HOME', async (_kind, clientFrame) => {
    await openProxiedChannel()
    getChannelMux().handleClientFrame(SESSION_ID, decode(clientFrame) as ChannelFrame, clientFrame)
    await tick()

    const toHome = substrate.capturedForwards.find(
      ({ envelope }) => envelope.direction === PROXY_DIRECTION.TO_HOME && envelope.payload.kind === ENVELOPE_KIND.FRAME,
    )
    expect(toHome).toBeDefined()
    expect(toHome!.target).toBe(HOME)
    if (toHome!.envelope.payload.kind !== ENVELOPE_KIND.FRAME) throw new Error('Expected FRAME')
    // The forwarded frame is the same bytes the client sent — no transcoding by the proxy.
    expect(Array.from(toHome!.envelope.payload.frame)).toEqual(Array.from(clientFrame))
  })

  it('writes a TO_PEER frame from the home to the local socket as-is', async () => {
    await openProxiedChannel()
    const peerFrame = new Uint8Array([0x11, 0x22]) as Uint8Array<ArrayBuffer>
    substrate.toPeer({ kind: ENVELOPE_KIND.FRAME, frame: peerFrame }, CHANNEL_ID, HOME)

    expect(writtenFrames.map((f) => Array.from(f))).toEqual([[0x11, 0x22]])
  })

  // Detach cleanup: detachSession forwards a DETACH to the home AND removes the proxy
  // state locally. After detach, late TO_PEER frames (from a slow home or a delayed
  // network path) must be dropped rather than written to a now-closed socket.
  it('detach cleans up: forwards DETACH to home AND drops subsequent TO_PEER frames', async () => {
    await openProxiedChannel()
    getChannelMux().detachSession(SESSION_ID, DETACH_REASON.PERMANENT)

    const detach = substrate.capturedForwards.find(
      ({ envelope }) =>
        envelope.direction === PROXY_DIRECTION.TO_HOME && envelope.payload.kind === ENVELOPE_KIND.DETACH,
    )
    expect(detach).toBeDefined()
    expect(detach!.target).toBe(HOME)
    if (detach!.envelope.payload.kind !== ENVELOPE_KIND.DETACH) throw new Error('Expected DETACH')
    expect(detach!.envelope.payload.reason).toBe(DETACH_REASON.PERMANENT)

    // A late frame from the home — must be dropped, not written to the closed socket.
    substrate.toPeer(
      { kind: ENVELOPE_KIND.FRAME, frame: new Uint8Array([0x33]) as Uint8Array<ArrayBuffer> },
      CHANNEL_ID,
      HOME,
    )
    expect(writtenFrames).toHaveLength(0)
  })
})
