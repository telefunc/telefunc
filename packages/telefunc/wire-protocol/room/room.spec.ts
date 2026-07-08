import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { Broadcast } from '../server/server-broadcast.js'
import {
  DefaultBroadcastAdapter,
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  type BroadcastAdapter,
} from '../server/broadcast.js'
import { IndexedPeer } from '../server/IndexedPeer.js'
import { ReplayBuffer } from '../replay-buffer.js'
import { ACK_STATUS, TAG, decode } from '../shared-ws.js'
import { room, RoomStubChannel, ServerRoom, type ServerLocalParticipant } from './server.js'
import { ClientRoom, ClientStandaloneParticipant } from './client.js'
import { frameWithMemberId, roomMainKey, unframeMemberId, type RoomSnapshotMetadata } from './shared.js'
import type { ClientBroadcast, ClientChannel } from '../client/channel.js'
import type { ChannelPublishInfo } from '../channel.js'

const previousAdapter = getBroadcastAdapter()
beforeEach(() => _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter()))
afterEach(() => _resetBroadcastAdapterForTesting(previousAdapter))

/** Let fire-and-forget async chains (KV deletes, leave publishes) settle. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 0))

// ───────────────────────────────────────────────────────────────────────────
// Entry point & KV lifecycle — bug classes targeted: state not shared across
// instances, phantom rooms after close, key collisions between room IDs.
// ───────────────────────────────────────────────────────────────────────────

describe('room entry point', () => {
  it('create → get returns the same room config; room(id) is a get shorthand', async () => {
    await room.create('lobby', { meta: { topic: 'general' }, size: 5 })
    const viaGet = await room.get('lobby')
    const viaCall = await room('lobby')
    for (const lobby of [viaGet, viaCall]) {
      expect(lobby.id).toBe('lobby')
      expect(lobby.meta).toEqual({ topic: 'general' })
      expect(lobby.size).toBe(5)
      expect(lobby.count).toBe(0)
      expect(lobby.isEmpty).toBe(true)
      expect(lobby.isClosed).toBe(false)
    }
  })

  it('create throws on an existing room; get throws on a missing one', async () => {
    await room.create('dup')
    await expect(room.create('dup')).rejects.toThrow('Room already exists: dup')
    await expect(room.get('nope')).rejects.toThrow('Room not found: nope')
  })

  it('size defaults to Infinity and is a hint — joins beyond capacity are not rejected', async () => {
    const lobby = await room.create('unbounded')
    expect(lobby.size).toBe(Infinity)
    expect(lobby.isFull).toBe(false)

    const tiny = await room.create('tiny', { size: 1 })
    await tiny.join()
    await tiny.join() // not enforced
    expect(tiny.count).toBe(2)
  })

  it('list() reflects rooms and their live member counts', async () => {
    await room.create('a')
    const b = await room.create('b', { meta: { topic: 'x' }, size: 2 })
    await b.join()

    const rooms = (await room.list()).sort((x, y) => x.id.localeCompare(y.id))
    expect(rooms).toEqual([
      { id: 'a', meta: {}, size: Infinity, count: 0, isEmpty: true, isFull: false },
      { id: 'b', meta: { topic: 'x' }, size: 2, count: 1, isEmpty: false, isFull: false },
    ])
  })

  // Room IDs may contain `:` (e.g. `video:demo`). A room whose ID extends another room's
  // member-key prefix must not leak into that room's member enumeration or `list()`.
  it('room IDs containing colons do not collide with member records', async () => {
    const a = await room.create('a')
    await a.join()
    await room.create('a:m:b')

    const rooms = await room.list()
    expect(rooms.map((r) => r.id).sort()).toEqual(['a', 'a:m:b'])
    expect(rooms.find((r) => r.id === 'a')!.count).toBe(1)
    expect(rooms.find((r) => r.id === 'a:m:b')!.count).toBe(0)
  })

  it('update() is a full replace — omitted size resets to Infinity — and fires onUpdate', async () => {
    const lobby = await room.create('conf', { meta: { topic: 'a' }, size: 2 })
    const updates: Array<[unknown, unknown]> = []
    lobby.onUpdate((meta, prev) => updates.push([meta, prev]))

    await room.update('conf', { meta: { topic: 'b' } })

    expect(updates).toEqual([[{ topic: 'b' }, { topic: 'a' }]])
    expect(lobby.meta).toEqual({ topic: 'b' })
    expect(lobby.size).toBe(Infinity)
    await expect(room.update('conf', { isolated: true })).rejects.toThrow('fixed at creation')
    await expect(room.update('gone', {})).rejects.toThrow('Room not found: gone')
  })

  it('close() fires onClose on observers, removes the room, and fails later joins', async () => {
    const lobby = await room.create('closing')
    const me = await lobby.join()
    let closed = false
    let meLeft = false
    lobby.onClose(() => (closed = true))
    me.onLeave(() => (meLeft = true))

    await room.close('closing')

    expect(closed).toBe(true)
    expect(meLeft).toBe(true)
    expect(lobby.isClosed).toBe(true)
    expect(lobby.count).toBe(0)
    await expect(room.get('closing')).rejects.toThrow('Room not found')
    await expect(lobby.join()).rejects.toThrow('Room is closed')
    await expect(room.close('closing')).rejects.toThrow('Room not found')
    expect(await room.list()).toEqual([])
  })

  it('removeParticipant() kicks: the member leaves everywhere, its LocalParticipant fires onLeave', async () => {
    const lobby = await room.create('kick')
    const me = await lobby.join({ name: 'Alice' })
    const observer = await room.get('kick')
    const kicked: string[] = []
    observer.onLeave((m) => kicked.push(m.id))
    let meLeft = false
    me.onLeave(() => (meLeft = true))

    await room.removeParticipant('kick', me.id)

    expect(kicked).toEqual([me.id])
    expect(meLeft).toBe(true)
    expect(lobby.count).toBe(0)
    await expect(me.publish({ text: 'too late' })).rejects.toThrow('Participant has left')
    await expect(room.removeParticipant('kick', me.id)).rejects.toThrow('Participant not found')
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Presence — bug classes targeted: double-fired events from the origin's own
// echo, missed cross-instance events, stale membership on unobserved rooms.
// ───────────────────────────────────────────────────────────────────────────

describe('presence', () => {
  it('join() announces the member on every observing instance — and exactly once on the origin', async () => {
    const a = await room.create('presence')
    const b = await room.get('presence')
    const joinsA: string[] = []
    const joinsB: unknown[] = []
    a.onJoin((m) => joinsA.push(m.id))
    b.onJoin((m) => joinsB.push(m.meta))

    const me = await a.join({ name: 'Alice' })

    expect(joinsA).toEqual([me.id]) // origin: local apply, echo absorbed
    expect(joinsB).toEqual([{ name: 'Alice' }]) // sibling: applied via the event
    expect(a.count).toBe(1)
    expect(b.count).toBe(1)
    expect(b.getParticipant(me.id)!.joinedAt).toBeGreaterThan(0)
  })

  it('leave() removes the member everywhere and fires onLeave + onEmpty', async () => {
    const a = await room.create('leaving')
    const b = await room.get('leaving')
    const events: string[] = []
    b.onLeave((m) => events.push(`leave:${m.id}`))
    b.onEmpty(() => events.push('empty'))

    const me = await a.join()
    await me.leave()

    expect(events).toEqual([`leave:${me.id}`, 'empty'])
    expect(a.count).toBe(0)
    expect(b.count).toBe(0)
    await me.leave() // idempotent
  })

  it('onFull fires when the room reaches capacity', async () => {
    const lobby = await room.create('full', { size: 2 })
    let full = 0
    lobby.onFull(() => full++)

    await lobby.join()
    expect(full).toBe(0)
    await lobby.join()
    expect(full).toBe(1)
    expect(lobby.isFull).toBe(true)
  })

  it('getParticipants() on an unobserved instance resyncs from KV', async () => {
    const a = await room.create('lazy')
    const me = await a.join({ name: 'Alice' })

    // `b` has no listeners/joins/stubs — it did not receive the join event.
    const b = await room.get('lazy')
    await me.setMeta({ name: 'Alicia' })
    const members = await b.getParticipants()

    expect(members.map((m) => m.meta)).toEqual([{ name: 'Alicia' }])
    expect(b.count).toBe(1)
  })

  it('setMeta() propagates to remote views — firing onUpdate exactly once on the origin', async () => {
    const a = await room.create('meta')
    const b = await room.get('meta')
    const seenOnB: unknown[] = []
    b.onJoin((m) => m.onUpdate((meta, prev) => seenOnB.push([meta, prev])))

    const me = await a.join({ name: 'Alice', score: 0 })
    const seenOnA: unknown[] = []
    a.getParticipant(me.id)!.onUpdate((meta, prev) => seenOnA.push([meta, prev]))

    await me.setMeta({ name: 'Alice', score: 42 })

    const expected = [
      [
        { name: 'Alice', score: 42 },
        { name: 'Alice', score: 0 },
      ],
    ]
    expect(seenOnA).toEqual(expected) // origin: local apply, echo deduped by eid
    expect(seenOnB).toEqual(expected)
    expect(me.meta).toEqual({ name: 'Alice', score: 42 })
    expect(b.getParticipant(me.id)!.meta).toEqual({ name: 'Alice', score: 42 })
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Data pub/sub — bug classes targeted: lost sender identity, cross-member
// bleed on per-member subscriptions, self-echo despite selfDelivery=false.
// ───────────────────────────────────────────────────────────────────────────

describe('data pub/sub', () => {
  it('publish() reaches room-level subscribers with sender identity and publish info', async () => {
    const a = await room.create('chat')
    const b = await room.get('chat')
    const received: Array<{ data: unknown; from: unknown; key: string; seq: number }> = []
    b.subscribe((data, info, from) => received.push({ data, from: from.meta, key: info.key, seq: info.seq }))

    const me = await a.join({ name: 'Alice' })
    const ack = await me.publish({ text: 'hello' })

    expect(received).toEqual([{ data: { text: 'hello' }, from: { name: 'Alice' }, key: 'chat', seq: ack.seq }])
    expect(ack.key).toBe('chat')
    expect(typeof ack.timestamp).toBe('number')
  })

  it("per-member subscribe receives only that member's messages", async () => {
    const lobby = await room.create('duo')
    const alice = await lobby.join({ name: 'Alice' })
    const bob = await lobby.join({ name: 'Bob' })

    const fromAlice: unknown[] = []
    lobby.getParticipant(alice.id)!.subscribe((data) => fromAlice.push(data))

    await alice.publish('a1')
    await bob.publish('b1')
    await alice.publish('a2')

    expect(fromAlice).toEqual(['a1', 'a2'])
  })

  it('publishers receive their own messages by default; selfDelivery=false suppresses only their holder', async () => {
    const a = await room.create('echo')
    const b = await room.get('echo')
    const seenOnA: unknown[] = []
    const seenOnB: unknown[] = []
    a.subscribe((data) => seenOnA.push(data))
    b.subscribe((data) => seenOnB.push(data))

    const me = await a.join()
    await me.publish('echoed')
    me.selfDelivery = false
    await me.publish('muted')

    expect(seenOnA).toEqual(['echoed']) // own holder: second publish suppressed
    expect(seenOnB).toEqual(['echoed', 'muted']) // everyone else: sees both
  })

  it('binary round-trips with the 16-byte member ID frame, preserving high-bit bytes', async () => {
    const a = await room.create('bin')
    const b = await room.get('bin')
    const received: Array<{ bytes: number[]; from: string }> = []
    b.subscribeBinary((data, _info, from) => received.push({ bytes: [...data], from: from.id }))

    const me = await a.join()
    await me.publishBinary(new Uint8Array([0x00, 0x7f, 0x80, 0xff]))

    expect(received).toEqual([{ bytes: [0x00, 0x7f, 0x80, 0xff], from: me.id }])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Isolated mode — bug class targeted: data landing on the shared key anyway
// (re-introducing the publish contention the mode exists to remove).
// ───────────────────────────────────────────────────────────────────────────

describe('isolated mode', () => {
  it('routes data over per-member keys while control stays on the main key', async () => {
    const a = await room.create('vid', { isolated: true })
    const b = await room.get('vid')
    const received: unknown[] = []
    b.subscribe((data, _info, from) => received.push([data, from.meta]))

    const mainKeyTraffic: string[] = []
    Broadcast.subscribe<{ __r: string }>(roomMainKey('vid'), (msg) => mainKeyTraffic.push(msg.__r))

    const me = await a.join({ name: 'Alice' })
    await me.publish('frame-1')

    expect(received).toEqual([['frame-1', { name: 'Alice' }]]) // delivered via the member key
    expect(mainKeyTraffic).toEqual(['join']) // the main key carried only control
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Room stub channel — the wire attachment a serialized Room gets. Bug classes
// targeted: impersonated publishes, members surviving their client's death,
// relay of binary frames the client never subscribed to.
// ───────────────────────────────────────────────────────────────────────────

describe('room stub channel', () => {
  function attachPeer(stub: RoomStubChannel) {
    const frames: Uint8Array[] = []
    stub._attachPeer(
      new IndexedPeer({ send: (frame) => frames.push(frame) }, 7, new ReplayBuffer(1024 * 1024, 60_000, 1024 * 1024)),
    )
    const decoded = () => frames.map((f) => decode(f as Uint8Array<ArrayBuffer>))
    return { frames, decoded }
  }

  async function createServedRoom(id: string) {
    const serverRoom = (await room.create(id)) as ServerRoom
    const stub = new RoomStubChannel(serverRoom)
    stub._registerChannel()
    serverRoom._attachStub(stub)
    return { serverRoom, stub, peer: attachPeer(stub) }
  }

  async function joinViaStub(stub: RoomStubChannel, peer: { decoded: () => any[] }, seq: number) {
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: { name: 'Remote' } }), seq)
    const ack = peer.decoded().find((f) => f.tag === TAG.ACK_RES && f.ackedSeq === seq)
    return JSON.parse(ack.text) as { ok: true; id: string; joinedAt: number }
  }

  it('req-join creates a member and acks its identity', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('served')
    const ack = await joinViaStub(stub, peer, 1)

    expect(ack.ok).toBe(true)
    expect(serverRoom.count).toBe(1)
    expect(serverRoom.getParticipant(ack.id)!.meta).toEqual({ name: 'Remote' })
  })

  it('room events are relayed to the client as PUBLISH frames', async () => {
    const { serverRoom, peer } = await createServedRoom('relay')
    await serverRoom.join({ name: 'Alice' })

    const relayed = peer.decoded().filter((f) => f.tag === TAG.PUBLISH)
    expect(relayed.length).toBe(1) // the join event
  })

  it('client publishes are validated: own member passes, impersonation is rejected', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('auth')
    const observer = await room.get('auth')
    const received: unknown[] = []
    observer.subscribe((data, _info, from) => received.push([data, from.id]))
    const { id } = await joinViaStub(stub, peer, 1)

    await stub._onPeerPublishAckReqMessage(stringify({ __r: 'data', from: id, data: 'legit' }), 2)
    await stub._onPeerPublishBinaryAckReqMessage(frameWithMemberId(crypto.randomUUID(), new Uint8Array([1])), 3)

    expect(received).toEqual([['legit', id]])
    const acks = peer.decoded().filter((f) => f.tag === TAG.ACK_RES)
    expect(acks.find((f) => f.ackedSeq === 2).status).toBe(ACK_STATUS.OK)
    expect(acks.find((f) => f.ackedSeq === 3).status).toBe(ACK_STATUS.ERROR)
    expect(serverRoom.count).toBe(1)
  })

  it('binary frames are relayed only after the client subscribes to the binary stream', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('lazy-bin')
    const me = await serverRoom.join()

    await me.publishBinary(new Uint8Array([1]))
    expect(peer.decoded().filter((f) => f.tag === TAG.PUBLISH_BINARY)).toEqual([])

    stub._onPeerBroadcastSubscribe(true)
    await me.publishBinary(new Uint8Array([2]))
    const relayed = peer.decoded().filter((f) => f.tag === TAG.PUBLISH_BINARY)
    expect(relayed.length).toBe(1)
    expect(unframeMemberId(relayed[0]!.data)!.from).toBe(me.id)
  })

  it('the client vanishing (channel shutdown) makes its members leave the room', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('vanish')
    const observer = await room.get('vanish')
    const leaves: string[] = []
    observer.onLeave((m) => leaves.push(m.id))
    const { id } = await joinViaStub(stub, peer, 1)

    stub._onPeerClose()
    await settle()

    expect(leaves).toEqual([id])
    expect(serverRoom.count).toBe(0)
    expect(await room.list()).toMatchObject([{ id: 'vanish', count: 0 }])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// ClientRoom — driven through a scripted stub standing in for the revived
// ClientBroadcast. Bug classes targeted: snapshot/event-stream double
// application, suppression not linking standalone participants to rooms.
// ───────────────────────────────────────────────────────────────────────────

type FakeStub = {
  emit: (envelope: unknown) => void
  emitBinary: (framed: Uint8Array) => void
  published: unknown[]
  binarySubscribed: () => boolean
  stub: ClientBroadcast
}

function createFakeStub(joinAck?: { id: string }): FakeStub {
  const textCbs: Array<(data: unknown, info: ChannelPublishInfo) => void> = []
  const binaryCbs: Array<(data: Uint8Array, info: ChannelPublishInfo) => void> = []
  const published: unknown[] = []
  let seq = 0
  const info = () => ({ key: 'fake', seq: ++seq, timestamp: 1 })
  const stub = {
    subscribe: (cb: (data: unknown, info: ChannelPublishInfo) => void) => {
      textCbs.push(cb)
      return () => textCbs.splice(textCbs.indexOf(cb), 1)
    },
    subscribeBinary: (cb: (data: Uint8Array, info: ChannelPublishInfo) => void) => {
      binaryCbs.push(cb)
      return () => binaryCbs.splice(binaryCbs.indexOf(cb), 1)
    },
    send: async (msg: { __r: string }) =>
      msg.__r === 'req-join' ? { ok: true, id: joinAck?.id ?? crypto.randomUUID(), joinedAt: 1 } : { ok: true },
    publish: async (envelope: unknown) => {
      published.push(envelope)
      return info()
    },
    publishBinary: async () => info(),
    onClose: () => {},
  }
  return {
    emit: (envelope) => [...textCbs].forEach((cb) => cb(envelope, info())),
    emitBinary: (framed) => [...binaryCbs].forEach((cb) => cb(framed, info())),
    published,
    binarySubscribed: () => binaryCbs.length > 0,
    stub: stub as unknown as ClientBroadcast,
  }
}

function createSnapshot(roomId: string, partial?: Partial<RoomSnapshotMetadata>): RoomSnapshotMetadata {
  return { channelId: 'ch1', roomId, meta: {}, size: null, isolated: false, closed: false, members: [], ...partial }
}

describe('ClientRoom', () => {
  it('seeds membership from the snapshot, then follows the event stream idempotently', () => {
    const fake = createFakeStub()
    const alice = crypto.randomUUID()
    const clientRoom = new ClientRoom(
      fake.stub,
      createSnapshot('snap', { members: [{ id: alice, meta: { name: 'Alice' }, joinedAt: 1 }] }),
    )
    const joins: string[] = []
    clientRoom.onJoin((m) => joins.push(m.id))

    expect(clientRoom.count).toBe(1)
    fake.emit({ __r: 'join', id: alice, meta: { name: 'Alice' }, joinedAt: 1 }) // overlap with snapshot
    expect(joins).toEqual([]) // absorbed
    const bob = crypto.randomUUID()
    fake.emit({ __r: 'join', id: bob, meta: { name: 'Bob' }, joinedAt: 2 })
    expect(joins).toEqual([bob])
    expect(clientRoom.count).toBe(2)
  })

  it('join() + publish() wrap the wire protocol; relayed data comes back with sender identity', async () => {
    const memberId = crypto.randomUUID()
    const fake = createFakeStub({ id: memberId })
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('flow'))
    const received: unknown[] = []
    clientRoom.subscribe((data, info, from) => received.push([data, info.key, from.id]))

    const me = await clientRoom.join({ name: 'Me' })
    expect(me.id).toBe(memberId)
    expect(clientRoom.count).toBe(1)

    await me.publish('hi')
    expect(fake.published).toEqual([{ __r: 'data', from: memberId, data: 'hi' }])

    fake.emit({ __r: 'data', from: memberId, data: 'hi' }) // server echo
    expect(received).toEqual([['hi', 'flow', memberId]])
  })

  it('applies leave/p-meta/update/closed events to state and local participants', async () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('events', { size: 5, meta: { topic: 'a' } }))
    const me = await clientRoom.join()
    const log: string[] = []
    clientRoom.onUpdate((meta) => log.push(`update:${JSON.stringify(meta)}`))
    clientRoom.onClose(() => log.push('closed'))
    me.onLeave(() => log.push('me-left'))

    fake.emit({ __r: 'p-meta', id: me.id, meta: { mood: 'happy' }, prev: {}, eid: 'e1' })
    expect(me.meta).toEqual({ mood: 'happy' })

    fake.emit({ __r: 'update', meta: { topic: 'b' }, prev: { topic: 'a' }, size: null, eid: 'e2' })
    expect(clientRoom.size).toBe(Infinity)

    fake.emit({ __r: 'leave', id: me.id }) // kicked
    fake.emit({ __r: 'closed' })

    expect(log).toEqual(['update:{"topic":"b"}', 'me-left', 'closed'])
    expect(clientRoom.isClosed).toBe(true)
    await expect(me.publish('x')).rejects.toThrow('Participant has left')
  })

  it('subscribes the wire binary stream only while binary listeners exist', () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('bin'))
    expect(fake.binarySubscribed()).toBe(false)

    const bytes: number[][] = []
    const unsubscribe = clientRoom.subscribeBinary((data) => bytes.push([...data]))
    expect(fake.binarySubscribed()).toBe(true)

    const sender = crypto.randomUUID()
    fake.emit({ __r: 'join', id: sender, meta: {}, joinedAt: 1 })
    fake.emitBinary(frameWithMemberId(sender, new Uint8Array([9, 8])))
    expect(bytes).toEqual([[9, 8]])

    unsubscribe()
    expect(fake.binarySubscribed()).toBe(false)
  })

  it('selfDelivery=false on a standalone participant suppresses its echo in a sibling room', async () => {
    const roomId = `sibling-${crypto.randomUUID()}`
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot(roomId))
    const received: unknown[] = []
    clientRoom.subscribe((data) => received.push(data))

    // The standalone participant arrives through its own channel, independent of the room stub.
    const channel = {
      listen: () => () => {},
      onClose: () => {},
      send: async () => ({ ok: true }),
      sendBinary: async () => ({ ok: true, ack: { key: roomId, seq: 1, timestamp: 1 } }),
      close: async () => 0,
    } as unknown as ClientChannel
    const me = new ClientStandaloneParticipant(channel, {
      channelId: 'ch2',
      roomId,
      id: crypto.randomUUID(),
      meta: {},
      joinedAt: 1,
    })

    fake.emit({ __r: 'join', id: me.id, meta: {}, joinedAt: 1 })
    me.selfDelivery = false
    fake.emit({ __r: 'data', from: me.id, data: 'own-frame' })
    expect(received).toEqual([]) // suppressed via the shared registry

    me.selfDelivery = true
    fake.emit({ __r: 'data', from: me.id, data: 'wanted' })
    expect(received).toEqual(['wanted'])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Adapter requirements — rooms need the adapter's KV; a custom adapter
// without it must fail loud and clear, not half-work.
// ───────────────────────────────────────────────────────────────────────────

describe('adapter KV requirement', () => {
  it('rejects with a clear error when the adapter lacks KV methods', async () => {
    const pubSubOnly: BroadcastAdapter = {
      subscribe: () => () => {},
      publish: () => ({ seq: 1, timestamp: 1 }),
      subscribeBinary: () => () => {},
      publishBinary: () => ({ seq: 1, timestamp: 1 }),
    }
    _resetBroadcastAdapterForTesting(pubSubOnly)
    await expect(room.create('kv-less')).rejects.toThrow(/KV methods required by `room\(\)`/)
  })
})
