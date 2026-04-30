import { afterEach, describe, expect, it } from 'vitest'
import { TAG, decode, encode } from '../shared-ws.js'
import { channel, type ServerChannel } from './channel.js'
import {
  ENVELOPE_KIND,
  PROXY_DIRECTION,
  dispatchEnvelope,
  getChannelMux,
  getChannelSubstrate,
  _resetChannelSubstrateForTesting,
  type ChannelSubstrate,
  type ChannelSubstrateHandlers,
  type ProxyEnvelope,
} from './substrate.js'

// Loopback substrate — captures `forward()` calls and lets the test deliver envelopes
// to the runtime's listener directly, without running a second ServerConnection.

class LoopbackSubstrate implements ChannelSubstrate {
  readonly selfInstanceId: string
  readonly heartbeatIntervalMs = 60_000
  private readonly watchers = new Map<string, Set<(home: string) => void>>()
  private readonly remoteHomes = new Map<string, string>()
  private readonly listeners = new Set<ChannelSubstrateHandlers>()
  /** Every envelope handed to forward(). The home-side test asserts on these. */
  readonly capturedForwards: { target: string; envelope: ProxyEnvelope }[] = []

  constructor(selfInstanceId: string = 'home-A') {
    this.selfInstanceId = selfInstanceId
  }

  /** Test helper: pin a channelId to a remote instance so locator calls return it. */
  setRemoteHome(channelId: string, instance: string): void {
    this.remoteHomes.set(channelId, instance)
    this.fireWatchers(channelId, instance)
  }

  async pinChannel(_channelId: string): Promise<void> {
    // Self pins are not relevant to `locateRemoteHome` — the runtime's local waiter handles them.
  }

  async unpinChannel(_channelId: string): Promise<void> {}

  async refreshPins(_channelIds: readonly string[]): Promise<void> {}

  async locateRemoteHome(channelId: string, _timeoutMs: number): Promise<string | null> {
    const remote = this.remoteHomes.get(channelId)
    if (remote !== undefined) return remote
    return new Promise((resolve) => {
      const cb = (home: string) => {
        const set = this.watchers.get(channelId)
        set?.delete(cb)
        if (set?.size === 0) this.watchers.delete(channelId)
        resolve(home)
      }
      let set = this.watchers.get(channelId)
      if (!set) {
        set = new Set()
        this.watchers.set(channelId, set)
      }
      set.add(cb)
    })
  }

  private fireWatchers(channelId: string, home: string): void {
    const set = this.watchers.get(channelId)
    if (!set) return
    this.watchers.delete(channelId)
    for (const cb of set) cb(home)
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

  /** Test helper: feed an envelope to the runtime as if it had arrived from a peer instance. */
  deliver(envelope: ProxyEnvelope): void {
    for (const handlers of this.listeners) dispatchEnvelope(handlers, envelope)
  }
}

// ───────────────────────────────────────────────────────────────────────────
// Spec
// ───────────────────────────────────────────────────────────────────────────

const previousSubstrate = getChannelSubstrate()
afterEach(() => _resetChannelSubstrateForTesting(previousSubstrate))

describe('cross-instance prePeerBuffer flush via ChannelSubstrate', () => {
  it('flushes home-side prePeerBuffer through the substrate-backed peer when a remote proxy attaches', async () => {
    const substrate = new LoopbackSubstrate('home-A')
    _resetChannelSubstrateForTesting(substrate)

    // Step 1: home creates a channel — gets registered locally + pinned.
    const ch = channel<{ text: string }, { text: string }>()
    getChannelMux().registerChannel(ch as ServerChannel)
    expect(getChannelMux().findLocal(ch.id)).toBe(ch)

    // Step 2: home-side telefunction code sends 3 messages while no peer is
    // attached. Each lands in `_prePeerBuffer` because `_peer` is null.
    void ch.send({ text: 'one' })
    void ch.send({ text: 'two' })
    void ch.send({ text: 'three' })

    // Pre-condition: nothing has been forwarded yet — there's no proxy.
    expect(substrate.capturedForwards).toEqual([])

    // Step 3: simulate the client landing on a different instance ('proxy-B'),
    // which sends an `attach` envelope to the home for this channel.
    const PROXY_B = 'proxy-B'
    const CLIENT_IX = 7
    substrate.deliver({
      channelId: ch.id,
      fromInstance: PROXY_B,
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 30_000, ix: CLIENT_IX, lastSeq: 0 },
    })

    // Step 4: the runtime constructed a substrate-backed `IndexedPeer` and
    // called `channel._attachPeer(peer)` — which flushes `_prePeerBuffer`
    // through the new peer. Each flushed frame becomes a `TO_PEER: frame`
    // envelope forwarded to `proxy-B`. The home also sends a single
    // `TO_PEER: attach-ack` carrying the channel's current `_lastClientSeq`.
    const peerEnvelopes = substrate.capturedForwards.filter((e) => e.envelope.direction === PROXY_DIRECTION.TO_PEER)
    const ackEnvelopes = peerEnvelopes.filter(({ envelope }) => envelope.payload.kind === ENVELOPE_KIND.ATTACH_ACK)
    const frameEnvelopes = peerEnvelopes.filter(({ envelope }) => envelope.payload.kind === ENVELOPE_KIND.FRAME)
    expect(ackEnvelopes).toHaveLength(1)
    expect(ackEnvelopes[0]!.envelope.payload).toEqual({ kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: 0 })
    expect(frameEnvelopes).toHaveLength(3)

    for (const { target, envelope } of frameEnvelopes) {
      expect(target).toBe(PROXY_B)
      expect(envelope.channelId).toBe(ch.id)
      expect(envelope.fromInstance).toBe(substrate.selfInstanceId)
    }

    // Decode the raw wire frames inside the envelopes — they should be TEXT
    // frames carrying the 3 serialized messages, with the client's `ix`.
    const decoded = frameEnvelopes.map(({ envelope }) => {
      if (envelope.payload.kind !== ENVELOPE_KIND.FRAME) throw new Error('Expected frame payload')
      return decode(envelope.payload.frame as Uint8Array<ArrayBuffer>)
    })
    for (const d of decoded) {
      expect(d.tag).toBe(TAG.TEXT)
      if (d.tag !== TAG.TEXT) throw new Error('Expected TEXT frame')
      expect(d.index).toBe(CLIENT_IX)
    }
    expect(decoded.map((d) => d.tag === TAG.TEXT && d.text)).toEqual([
      '{"text":"one"}',
      '{"text":"two"}',
      '{"text":"three"}',
    ])
  })

  it('forwards proxied client frames as TO_HOME envelopes via reconcileSession', async () => {
    const substrate = new LoopbackSubstrate('proxy-self')
    substrate.setRemoteHome('pubsub:room:foo', 'home-A')
    _resetChannelSubstrateForTesting(substrate)

    const writtenFrames: Uint8Array[] = []
    const send = (frame: Uint8Array<ArrayBuffer>): void => {
      writtenFrames.push(frame)
    }
    const sessionId = 'session-1'
    const openPromise = getChannelMux().reconcileSession({
      prevSessionId: undefined,
      newSessionId: sessionId,
      open: [{ id: 'pubsub:room:foo', ix: 3, lastSeq: 12 }],
      send,
    })

    // Wait for the substrate's `locateChannel` + `forward(attach)` to settle (each is async).
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(substrate.capturedForwards).toHaveLength(1)
    const attach = substrate.capturedForwards[0]!
    expect(attach.target).toBe('home-A')
    expect(attach.envelope.payload.kind).toBe(ENVELOPE_KIND.ATTACH)
    if (attach.envelope.payload.kind !== ENVELOPE_KIND.ATTACH) throw new Error('Expected attach payload')
    expect(attach.envelope.payload.ix).toBe(3)
    expect(attach.envelope.payload.lastSeq).toBe(12)

    // The home replies with attach-ack carrying its lastClientSeq. Until this arrives,
    // reconcileSession's promise is pending.
    substrate.deliver({
      channelId: 'pubsub:room:foo',
      fromInstance: 'home-A',
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: 42 },
    })
    const open = await openPromise
    expect(open).toEqual([{ id: 'pubsub:room:foo', ix: 3, lastSeq: 42 }])

    // A client frame routed through the mux becomes a TO_HOME envelope. The frame must be
    // a valid encoded TEXT frame at ix=3 so the mux can extract the index for routing.
    const clientFrame = encode.text(3, '"hello"', 1)
    getChannelMux().handleClientFrame(sessionId, clientFrame)
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(substrate.capturedForwards).toHaveLength(2)
    const forwarded = substrate.capturedForwards[1]!
    expect(forwarded.target).toBe('home-A')
    expect(forwarded.envelope.direction).toBe(PROXY_DIRECTION.TO_HOME)
    expect(forwarded.envelope.payload.kind).toBe(ENVELOPE_KIND.FRAME)
    if (forwarded.envelope.payload.kind !== ENVELOPE_KIND.FRAME) throw new Error('Expected frame payload')
    expect(Array.from(forwarded.envelope.payload.frame)).toEqual(Array.from(clientFrame))

    // A TO_PEER envelope arriving for this proxied channel writes its frame to the local socket.
    const peerFrame = new Uint8Array([0x11, 0x22]) as Uint8Array<ArrayBuffer>
    substrate.deliver({
      channelId: 'pubsub:room:foo',
      fromInstance: 'home-A',
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.FRAME, frame: peerFrame },
    })
    expect(writtenFrames).toHaveLength(1)
    expect(Array.from(writtenFrames[0]!)).toEqual([0x11, 0x22])

    // detachSession removes the local proxy state and forwards a TO_HOME detach envelope.
    getChannelMux().detachSession(sessionId, 'permanent')
    expect(substrate.capturedForwards).toHaveLength(3)
    const detach = substrate.capturedForwards[2]!
    expect(detach.target).toBe('home-A')
    expect(detach.envelope.direction).toBe(PROXY_DIRECTION.TO_HOME)
    expect(detach.envelope.payload).toEqual({ kind: ENVELOPE_KIND.DETACH, reason: expect.any(String) })

    // After detach, TO_PEER envelopes for that channel are dropped.
    substrate.deliver({
      channelId: 'pubsub:room:foo',
      fromInstance: 'home-A',
      direction: PROXY_DIRECTION.TO_PEER,
      payload: { kind: ENVELOPE_KIND.FRAME, frame: new Uint8Array([0x33]) as Uint8Array<ArrayBuffer> },
    })
    expect(writtenFrames).toHaveLength(1)
  })

  it("reports the home's lastClientSeq in the attach-ack so the proxy uses it in CtrlReconciled", async () => {
    const substrate = new LoopbackSubstrate('home-A')
    _resetChannelSubstrateForTesting(substrate)

    const ch = channel<{ text: string }, { text: string }>()
    getChannelMux().registerChannel(ch as ServerChannel)
    // Simulate the home having received 7 client→server frames for this channel.
    ;(ch as unknown as { _lastClientSeq: number })._lastClientSeq = 7

    substrate.deliver({
      channelId: ch.id,
      fromInstance: 'proxy-B',
      direction: PROXY_DIRECTION.TO_HOME,
      payload: { kind: ENVELOPE_KIND.ATTACH, reconnectTimeout: 30_000, ix: 1, lastSeq: 0 },
    })

    const ack = substrate.capturedForwards.find(({ envelope }) => envelope.payload.kind === ENVELOPE_KIND.ATTACH_ACK)
    expect(ack).toBeDefined()
    expect(ack!.target).toBe('proxy-B')
    expect(ack!.envelope.payload).toEqual({ kind: ENVELOPE_KIND.ATTACH_ACK, lastClientSeq: 7 })
  })
})
