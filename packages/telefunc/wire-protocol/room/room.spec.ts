import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
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
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_TTL_MS } from '../constants.js'
import { ACK_STATUS, TAG, decode } from '../shared-ws.js'
import { Room, ServerRoom, type ServerLocalParticipant } from './server.js'
import { RoomStubChannel } from './stubs.js'
import { ClientRoom, ClientStandaloneParticipant } from './client.js'
import {
  frameWithMemberId,
  roomMainKey,
  roomMemberKvKey,
  unframeMemberId,
  type RoomMemberRecord,
  type RoomSnapshotMetadata,
} from './shared.js'
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

describe('Room entry point', () => {
  it('create → get returns the same room config', async () => {
    await Room.create('lobby', { meta: { topic: 'general' }, size: 5 })
    const lobby = await Room.get('lobby')
    expect(lobby.id).toBe('lobby')
    expect(lobby.meta).toEqual({ topic: 'general' })
    expect(lobby.size).toBe(5)
    expect(lobby.count).toBe(0)
    expect(lobby.isEmpty).toBe(true)
    expect(lobby.isClosed).toBe(false)
  })

  it('create throws on an existing room; get throws on a missing one', async () => {
    await Room.create('dup')
    await expect(Room.create('dup')).rejects.toThrow('Room already exists: dup')
    await expect(Room.get('nope')).rejects.toThrow('Room not found: nope')
  })

  it('size defaults to Infinity and is a hint — joins beyond capacity are not rejected', async () => {
    const lobby = await Room.create('unbounded')
    expect(lobby.size).toBe(Infinity)
    expect(lobby.isFull).toBe(false)

    const tiny = await Room.create('tiny', { size: 1 })
    await tiny.join()
    await tiny.join() // not enforced
    expect(tiny.count).toBe(2)
  })

  it('Room.join() is a shorthand for get + join', async () => {
    await Room.create('shortcut')
    const observer = await Room.get('shortcut')

    const me = await Room.join('shortcut', { name: 'Bot' }, { selfDelivery: false })

    expect(me.meta).toEqual({ name: 'Bot' })
    expect(me.selfDelivery).toBe(false)
    expect((await observer.getParticipants()).map((m) => m.meta)).toEqual([{ name: 'Bot' }])
    await expect(Room.join('nope')).rejects.toThrow('Room not found: nope')
  })

  it('list() reflects rooms and their live member counts', async () => {
    await Room.create('a')
    const b = await Room.create('b', { meta: { topic: 'x' }, size: 2 })
    await b.join()

    const rooms = (await Room.list()).sort((x, y) => x.id.localeCompare(y.id))
    expect(rooms).toEqual([
      { id: 'a', meta: {}, size: Infinity, count: 0, isEmpty: true, isFull: false },
      { id: 'b', meta: { topic: 'x' }, size: 2, count: 1, isEmpty: false, isFull: false },
    ])
  })

  // Room IDs may contain `:` (e.g. `video:demo`). A room whose ID extends another room's
  // member-key prefix must not leak into that room's member enumeration or `list()`.
  it('room IDs containing colons do not collide with member records', async () => {
    const a = await Room.create('a')
    await a.join()
    await Room.create('a:m:b')

    const rooms = await Room.list()
    expect(rooms.map((r) => r.id).sort()).toEqual(['a', 'a:m:b'])
    expect(rooms.find((r) => r.id === 'a')!.count).toBe(1)
    expect(rooms.find((r) => r.id === 'a:m:b')!.count).toBe(0)
  })

  it('update() is a full replace — omitted size resets to Infinity — and fires onUpdate', async () => {
    const lobby = await Room.create('conf', { meta: { topic: 'a' }, size: 2 })
    const updates: Array<[unknown, unknown]> = []
    lobby.onUpdate((meta, prev) => updates.push([meta, prev]))

    await Room.update('conf', { meta: { topic: 'b' } })

    expect(updates).toEqual([[{ topic: 'b' }, { topic: 'a' }]])
    expect(lobby.meta).toEqual({ topic: 'b' })
    expect(lobby.size).toBe(Infinity)
    await expect(Room.update('conf', { isolated: true })).rejects.toThrow('fixed at creation')
    await expect(Room.update('gone', {})).rejects.toThrow('Room not found: gone')
  })

  it('close() fires onClose on observers, removes the room, and fails later joins', async () => {
    const lobby = await Room.create('closing')
    const me = await lobby.join()
    let closed = false
    let meLeft = false
    lobby.onClose(() => (closed = true))
    me.onLeave(() => (meLeft = true))

    await Room.close('closing')

    expect(closed).toBe(true)
    expect(meLeft).toBe(true)
    expect(lobby.isClosed).toBe(true)
    expect(lobby.count).toBe(0)
    await expect(Room.get('closing')).rejects.toThrow('Room not found')
    await expect(lobby.join()).rejects.toThrow('Room is closed')
    await expect(Room.close('closing')).rejects.toThrow('Room not found')
    expect(await Room.list()).toEqual([])
  })

  it('removeParticipant() kicks: the member leaves everywhere, its LocalParticipant fires onLeave', async () => {
    const lobby = await Room.create('kick')
    const me = await lobby.join({ name: 'Alice' })
    const observer = await Room.get('kick')
    const kicked: string[] = []
    observer.onLeave((m) => kicked.push(m.id))
    let meLeft = false
    me.onLeave(() => (meLeft = true))

    await Room.removeParticipant('kick', me.id)

    expect(kicked).toEqual([me.id])
    expect(meLeft).toBe(true)
    expect(lobby.count).toBe(0)
    await expect(me.publish({ text: 'too late' })).rejects.toThrow('Participant has left')
    await expect(Room.removeParticipant('kick', me.id)).rejects.toThrow('Participant not found')
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Presence — bug classes targeted: double-fired events from the origin's own
// echo, missed cross-instance events, stale membership on unobserved rooms.
// ───────────────────────────────────────────────────────────────────────────

describe('presence', () => {
  it('join() announces the member on every observing instance — and exactly once on the origin', async () => {
    const a = await Room.create('presence')
    const b = await Room.get('presence')
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
    const a = await Room.create('leaving')
    const b = await Room.get('leaving')
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
    const lobby = await Room.create('full', { size: 2 })
    let full = 0
    lobby.onFull(() => full++)

    await lobby.join()
    expect(full).toBe(0)
    await lobby.join()
    expect(full).toBe(1)
    expect(lobby.isFull).toBe(true)
  })

  it('getParticipants() on an unobserved instance resyncs from KV', async () => {
    const a = await Room.create('lazy')
    const me = await a.join({ name: 'Alice' })

    // `b` has no listeners/joins/stubs — it did not receive the join event.
    const b = await Room.get('lazy')
    await me.setMeta({ name: 'Alicia' })
    const members = await b.getParticipants()

    expect(members.map((m) => m.meta)).toEqual([{ name: 'Alicia' }])
    expect(b.count).toBe(1)
  })

  it('setMeta() propagates to remote views — firing onUpdate exactly once on the origin', async () => {
    const a = await Room.create('meta')
    const b = await Room.get('meta')
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
    const a = await Room.create('chat')
    const b = await Room.get('chat')
    const received: Array<{ data: unknown; from: unknown; key: string; seq: number }> = []
    b.subscribe((data, info, from) => received.push({ data, from: from.meta, key: info.key, seq: info.seq }))

    const me = await a.join({ name: 'Alice' })
    const ack = await me.publish({ text: 'hello' })

    expect(received).toEqual([{ data: { text: 'hello' }, from: { name: 'Alice' }, key: 'chat', seq: ack.seq }])
    expect(ack.key).toBe('chat')
    expect(typeof ack.timestamp).toBe('number')
  })

  it("per-member subscribe receives only that member's messages", async () => {
    const lobby = await Room.create('duo')
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
    const a = await Room.create('echo')
    const b = await Room.get('echo')
    const seenOnA: unknown[] = []
    const seenOnB: unknown[] = []
    a.subscribe((data) => seenOnA.push(data))
    b.subscribe((data) => seenOnB.push(data))

    const chatty = await a.join({ name: 'chatty' })
    const muted = await a.join({ name: 'muted' }, { selfDelivery: false })
    await chatty.publish('echoed')
    await muted.publish('not-here')

    expect(chatty.selfDelivery).toBe(true)
    expect(muted.selfDelivery).toBe(false)
    expect(seenOnA).toEqual(['echoed']) // own holder: the muted participant's publish suppressed
    expect(seenOnB).toEqual(['echoed', 'not-here']) // everyone else: sees both
  })

  it('rejects a stub binary publish from a participant that already left — like the text path', async () => {
    const lobby = await Room.create('late-frame')
    const me = (await lobby.join()) as ServerLocalParticipant
    await me.leave()
    expect(() => me._publishFramed(frameWithMemberId(me.id, new Uint8Array([1])))).toThrow('Participant has left')
  })

  it('binary round-trips with the 16-byte member ID frame, preserving high-bit bytes', async () => {
    const a = await Room.create('bin')
    const b = await Room.get('bin')
    const received: Array<{ bytes: number[]; from: string }> = []
    b.subscribeBinary((data, _info, from) => received.push({ bytes: [...data], from: from.id }))

    const me = await a.join()
    await me.publishBinary(new Uint8Array([0x00, 0x7f, 0x80, 0xff]))

    expect(received).toEqual([{ bytes: [0x00, 0x7f, 0x80, 0xff], from: me.id }])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Selective binary — bug classes targeted: unwanted members' frames crossing
// the wire (bandwidth), departed members' listeners pinning subscriptions
// open (the `onLeave(() => decoder.close())` pattern never unsubscribes).
// ───────────────────────────────────────────────────────────────────────────

describe('selective binary delivery', () => {
  it("isolated mode subscribes only the wanted members' upstream keys", async () => {
    const a = await Room.create('sel', { isolated: true })
    const cam1 = await a.join({ name: 'cam1' })
    const cam2 = await a.join({ name: 'cam2' })
    const b = await Room.get('sel')

    const frames: string[] = []
    b.getParticipant(cam1.id)!.subscribeBinary((data) => frames.push(`cam1:${data[0]}`))
    await cam1.publishBinary(new Uint8Array([1]))
    await cam2.publishBinary(new Uint8Array([2])) // nobody wants cam2 — b never subscribed its key

    expect(frames).toEqual(['cam1:1'])
  })

  it("releases a departed member's listeners — the decoder pattern must not pin subscriptions", async () => {
    const a = await Room.create('release', { isolated: true })
    const cam = await a.join({ name: 'cam' })
    const b = await Room.get('release')

    // The documented pattern: subscribe on join, close the decoder on leave — no unsubscribe.
    b.getParticipant(cam.id)!.subscribeBinary(() => {})
    expect(b.getParticipant(cam.id)).not.toBe(null)

    await cam.leave()

    const bState = (b as ServerRoom)._state
    expect(bState.binaryWants()).toEqual({ all: false, members: [] })
    expect(bState.binaryListenerCount).toBe(0)
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Direct messages — bug classes targeted: privacy leaks (a DM reaching room
// subscribers or a non-target holder), lost sender identity, dangling sends
// to unknown or departed participants.
// ───────────────────────────────────────────────────────────────────────────

describe('direct messages', () => {
  it('delivers privately across instances — only the target hears it', async () => {
    const a = await Room.create('dm')
    const b = await Room.get('dm')
    const alice = await a.join({ name: 'Alice' })
    const bob = await b.join({ name: 'Bob' })
    const bobInbox: unknown[] = []
    const aliceInbox: unknown[] = []
    const roomStream: unknown[] = []
    bob.listen((data, from) => bobInbox.push([data, from?.id, from?.meta]))
    alice.listen((data) => aliceInbox.push(data))
    a.subscribe((data) => roomStream.push(data))
    b.subscribe((data) => roomStream.push(data))

    await alice.send(a.getParticipant(bob.id)!, 'psst') // target as object — or pass the ID

    expect(bobInbox).toEqual([['psst', alice.id, { name: 'Alice' }]]) // live RemoteParticipant sender
    expect(aliceInbox).toEqual([]) // not echoed to the sender
    expect(roomStream).toEqual([]) // never on the room stream
  })

  it('rejects unknown targets and departed senders', async () => {
    const lobby = await Room.create('dm-err')
    const alice = await lobby.join()
    await expect(alice.send(crypto.randomUUID(), 'x')).rejects.toThrow('Participant not found')

    const bob = await lobby.join()
    await alice.leave()
    await expect(alice.send(bob.id, 'x')).rejects.toThrow('Participant has left')
  })

  it('join({ onSend }) guards sends: rejections reach the sender, deliveries carry rich identity', async () => {
    const lobby = await Room.create('guarded')
    const bob = await lobby.join({ name: 'Bob' })
    const inbox: unknown[] = []
    bob.listen((data, from) => inbox.push([data, from?.id, from?.meta]))
    const alice = await lobby.join(
      { name: 'Alice' },
      {
        onSend: (to, data) => {
          if (data === 'blocked') throw new Error('not friends')
          expect(to.id).toBe(bob.id) // the guard sees the resolved target, meta included
          expect(to.meta).toEqual({ name: 'Bob' })
        },
      },
    )

    await expect(alice.send(bob.id, 'blocked')).rejects.toThrow('not friends')
    await alice.send(bob.id, 'hi')

    expect(inbox).toEqual([['hi', alice.id, { name: 'Alice' }]])
  })

  it("delivers to a member the sender's stale local view doesn't know yet (KV fallback)", async () => {
    const a = await Room.create('dm-lag')
    const alice = await a.join({ name: 'Alice' })
    const b = await Room.get('dm-lag') // snapshot: alice only
    const bob = await a.join({ name: 'Bob' }) // b is unobserved — it missed this join
    const bobInbox: unknown[] = []
    bob.listen((data, from) => bobInbox.push([data, from?.id]))

    // b's local view lags; the KV member record is authoritative.
    await (b as ServerRoom)._sendDm(alice.id, bob.id, 'catch-up', null)

    expect(bobInbox).toEqual([['catch-up', alice.id]])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Room-authored messages — system notices without a synthetic member: they
// carry no sender, never pollute the member list, and can't be spoofed by
// clients (their publishes/DMs are validated against their own members).
// ───────────────────────────────────────────────────────────────────────────

describe('room-authored messages', () => {
  it('announce() reaches onAnnounce() everywhere — and never the participant streams', async () => {
    const a = await Room.create('sys')
    const b = await Room.get('sys')
    await a.join({ name: 'Alice' })
    const announced: unknown[] = []
    const streamed: unknown[] = []
    b.onAnnounce((data, info) => announced.push([data, info.key]))
    b.subscribe((data) => streamed.push(data))

    await Room.announce('sys', { text: 'maintenance at noon' })

    expect(announced).toEqual([[{ text: 'maintenance at noon' }, 'sys']])
    expect(streamed).toEqual([])
    await expect(Room.announce('gone', 'x')).rejects.toThrow('Room not found: gone')
  })

  it('Room.send() whispers to one participant — from is null (room-authored)', async () => {
    const lobby = await Room.create('automod')
    const alice = await lobby.join({ name: 'Alice' })
    const bob = await lobby.join({ name: 'Bob' })
    const aliceInbox: unknown[] = []
    const bobInbox: unknown[] = []
    alice.listen((data, from) => aliceInbox.push([data, from]))
    bob.listen((data) => bobInbox.push(data))

    await Room.send('automod', alice.id, { warning: 'watch the language' })

    expect(aliceInbox).toEqual([[{ warning: 'watch the language' }, null]]) // null = room-authored
    expect(bobInbox).toEqual([])
    await expect(Room.send('automod', crypto.randomUUID(), 'x')).rejects.toThrow('Participant not found')
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Isolated mode — bug class targeted: data landing on the shared key anyway
// (re-introducing the publish contention the mode exists to remove).
// ───────────────────────────────────────────────────────────────────────────

describe('isolated mode', () => {
  it('routes data over per-member keys while control stays on the main key', async () => {
    const a = await Room.create('vid', { isolated: true })
    const b = await Room.get('vid')
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
    const serverRoom = (await Room.create(id)) as ServerRoom
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
    const observer = await Room.get('auth')
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

  it("binary frames are relayed member-selectively, per the client's declared wants", async () => {
    const { serverRoom, stub, peer } = await createServedRoom('lazy-bin')
    const wanted = await serverRoom.join({ name: 'wanted' })
    const unwanted = await serverRoom.join({ name: 'unwanted' })

    await wanted.publishBinary(new Uint8Array([1]))
    expect(peer.decoded().filter((f) => f.tag === TAG.PUBLISH_BINARY)).toEqual([]) // nothing declared yet

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', all: false, members: [wanted.id] }), 60)
    await wanted.publishBinary(new Uint8Array([2]))
    await unwanted.publishBinary(new Uint8Array([3]))
    let relayed = peer.decoded().filter((f) => f.tag === TAG.PUBLISH_BINARY)
    expect(relayed.length).toBe(1) // only the wanted member's frame crossed the wire
    expect(unframeMemberId(relayed[0]!.data)!.from).toBe(wanted.id)

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', all: true, members: [] }), 61)
    await unwanted.publishBinary(new Uint8Array([4]))
    relayed = peer.decoded().filter((f) => f.tag === TAG.PUBLISH_BINARY)
    expect(relayed.length).toBe(2)
  })

  it('the client vanishing (channel shutdown) makes its members leave the room', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('vanish')
    const observer = await Room.get('vanish')
    const leaves: string[] = []
    observer.onLeave((m) => leaves.push(m.id))
    const { id } = await joinViaStub(stub, peer, 1)

    stub._onPeerClose()
    await settle()

    expect(leaves).toEqual([id])
    expect(serverRoom.count).toBe(0)
    expect(await Room.list()).toMatchObject([{ id: 'vanish', count: 0 }])
  })

  it('relays a DM only to the stub owning the target', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('dm-stub')
    const { id } = await joinViaStub(stub, peer, 1)
    const bystander = new RoomStubChannel(serverRoom)
    bystander._registerChannel()
    serverRoom._attachStub(bystander)
    const bystanderPeer = attachPeer(bystander)

    const sender = await serverRoom.join({ name: 'Srv' })
    await sender.send(id, 'psst')

    const dmFramesOf = (frames: any[]) =>
      frames.filter((f) => f.tag === TAG.PUBLISH && (JSON.parse(f.text) as { __r: string }).__r === 'dm')
    expect(dmFramesOf(peer.decoded()).length).toBe(1) // the owner's stub got it
    expect(dmFramesOf(bystanderPeer.decoded())).toEqual([]) // nobody else did
  })

  it("routes a stub member's DM to a server-held participant, validating the sender", async () => {
    const { serverRoom, stub, peer } = await createServedRoom('dm-stub-send')
    const target = await serverRoom.join({ name: 'Srv' })
    const inbox: unknown[] = []
    target.listen((data, from) => inbox.push([data, from?.id]))
    const { id } = await joinViaStub(stub, peer, 1)

    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-dm', id, to: target.id, data: 'hi' }), 2)
    // Impersonation: a sender ID not joined through this stub is rejected.
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-dm', id: target.id, to: target.id, data: 'spoof' }), 3)

    expect(inbox).toEqual([['hi', id]])
    const acks = peer.decoded().filter((f) => f.tag === TAG.ACK_RES)
    expect(JSON.parse(acks.find((f) => f.ackedSeq === 2).text).ok).toBe(true)
    expect(JSON.parse(acks.find((f) => f.ackedSeq === 3).text).ok).toBe(false)
  })

  it('a stub member joined with selfDelivery=false gets no echo of its own publishes', async () => {
    const { stub, peer } = await createServedRoom('quiet-stub')
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: {}, selfDelivery: false }), 1)
    const ack = peer.decoded().find((f) => f.tag === TAG.ACK_RES && f.ackedSeq === 1)
    const { id } = JSON.parse(ack.text) as { id: string }

    await stub._onPeerPublishAckReqMessage(stringify({ __r: 'data', from: id, data: 'own' }), 2)

    const dataFrames = peer
      .decoded()
      .filter((f) => f.tag === TAG.PUBLISH && (JSON.parse(f.text) as { __r: string }).__r === 'data')
    expect(dataFrames).toEqual([]) // the echo was skipped at the relay, not just client-side
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
  sent: Array<{ __r: string }>
  binarySubscribed: () => boolean
  stub: ClientBroadcast
}

function createFakeStub(joinAck?: { id: string }): FakeStub {
  const textCbs: Array<(data: unknown, info: ChannelPublishInfo) => void> = []
  const binaryCbs: Array<(data: Uint8Array, info: ChannelPublishInfo) => void> = []
  const published: unknown[] = []
  const sent: Array<{ __r: string }> = []
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
    send: async (msg: { __r: string }) => {
      sent.push(msg)
      return msg.__r === 'req-join' ? { ok: true, id: joinAck?.id ?? crypto.randomUUID(), joinedAt: 1 } : { ok: true }
    },
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
    sent,
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

  it('send() wires DMs through the stub; inbox relays route to the right participant', async () => {
    const memberId = crypto.randomUUID()
    const fake = createFakeStub({ id: memberId })
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('dms'))
    const me = await clientRoom.join({}, { selfDelivery: false })
    const inbox: unknown[] = []
    me.listen((data, from) => inbox.push([data, from]))

    const peer = crypto.randomUUID()
    await me.send(peer, 'psst')
    expect(fake.sent).toContainEqual({ __r: 'req-dm', id: memberId, to: peer, data: 'psst' })
    expect(fake.sent).toContainEqual({ __r: 'req-join', meta: {}, selfDelivery: false })

    fake.emit({ __r: 'dm', to: memberId, from: peer, fromMeta: { name: 'Peer' }, data: 'reply' })
    fake.emit({ __r: 'dm', to: crypto.randomUUID(), from: peer, data: 'not-mine' })
    expect(inbox).toEqual([['reply', { id: peer, meta: { name: 'Peer' } }]]) // snapshot sender — peer isn't in the local view
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

  it('declares its binary wants to the server — member-selective, sent synchronously, deduped', () => {
    const cam1 = crypto.randomUUID()
    const cam2 = crypto.randomUUID()
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(
      fake.stub,
      createSnapshot('wants', {
        members: [
          { id: cam1, meta: {}, joinedAt: 1 },
          { id: cam2, meta: {}, joinedAt: 2 },
        ],
      }),
    )
    const subBinaryMsgs = () => fake.sent.filter((m) => m.__r === 'sub-binary')

    // Each widening is declared synchronously — a publish right after subscribing must be
    // preceded by its declaration on the wire (same-connection FIFO).
    const unsub1 = clientRoom.getParticipant(cam1)!.subscribeBinary(() => {})
    expect(subBinaryMsgs()).toEqual([{ __r: 'sub-binary', all: false, members: [cam1] }])
    clientRoom.getParticipant(cam2)!.subscribeBinary(() => {})
    expect(subBinaryMsgs().at(-1)).toEqual({ __r: 'sub-binary', all: false, members: [cam1, cam2] })

    // A room-level listener upgrades the declaration to `all`; listener changes that leave the
    // effective set unchanged send nothing.
    const unsubAll = clientRoom.subscribeBinary(() => {})
    expect(subBinaryMsgs().at(-1)).toEqual({ __r: 'sub-binary', all: true, members: [] })
    const sentCount = subBinaryMsgs().length
    const unsubDup = clientRoom.getParticipant(cam1)!.subscribeBinary(() => {})
    expect(subBinaryMsgs().length).toBe(sentCount)

    // Dropping back to one member narrows it again.
    unsubDup()
    unsubAll()
    unsub1()
    expect(subBinaryMsgs().at(-1)).toEqual({ __r: 'sub-binary', all: false, members: [cam2] })
  })

  it("a member leaving releases its listeners — the client's declaration narrows without an unsubscribe", () => {
    const cam = crypto.randomUUID()
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(
      fake.stub,
      createSnapshot('wants-release', { members: [{ id: cam, meta: {}, joinedAt: 1 }] }),
    )
    clientRoom.getParticipant(cam)!.subscribeBinary(() => {}) // never unsubscribed
    expect(fake.binarySubscribed()).toBe(true)

    fake.emit({ __r: 'leave', id: cam })

    expect(fake.binarySubscribed()).toBe(false) // wire stream released
    expect(fake.sent.filter((m) => m.__r === 'sub-binary').at(-1)).toEqual({
      __r: 'sub-binary',
      all: false,
      members: [],
    })
  })

  it('a standalone participant joined with selfDelivery=false suppresses its echo in a sibling room', async () => {
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
      selfDelivery: false,
    })
    const other = crypto.randomUUID()
    fake.emit({ __r: 'join', id: me.id, meta: {}, joinedAt: 1 })
    fake.emit({ __r: 'join', id: other, meta: {}, joinedAt: 2 })

    fake.emit({ __r: 'data', from: me.id, data: 'own-frame' })
    fake.emit({ __r: 'data', from: other, data: 'their-frame' })

    expect(received).toEqual(['their-frame']) // own echo suppressed via the shared registry, from revive on
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Liveness — graceful departures travel as events; a hard node crash leaves
// member records behind. Owners heartbeat `seenAt`; stale records are reaped
// on read and on every heartbeat, with the leave announced to all observers.
// ───────────────────────────────────────────────────────────────────────────

describe('liveness', () => {
  async function backdate(roomId: string, memberId: string): Promise<void> {
    const adapter = getBroadcastAdapter()
    const key = roomMemberKvKey(roomId, memberId)
    const record = parse((await adapter.get!(key)) as string) as RoomMemberRecord
    const stale: RoomMemberRecord = { ...record, seenAt: Date.now() - ROOM_MEMBER_TTL_MS - 1 }
    await adapter.set!(key, stringify(stale))
  }

  it('reaps members with a stale heartbeat on read, announcing the leave everywhere', async () => {
    const a = await Room.create('crashed')
    const me = await a.join({ name: 'Ghost' })
    const observer = await Room.get('crashed')
    const leaves: string[] = []
    observer.onLeave((m) => leaves.push(m.id))

    await backdate('crashed', me.id) // simulate: the owning node died 2 minutes ago

    expect(await Room.list()).toMatchObject([{ id: 'crashed', count: 0 }])
    expect(leaves).toEqual([me.id])
    expect(observer.count).toBe(0)
    expect(a.count).toBe(0) // the (supposed) owner learned via the reaper's event too
    await expect(me.publish('boo')).rejects.toThrow('Participant has left')
  })

  it('owners refresh their members every heartbeat interval', async () => {
    vi.useFakeTimers()
    try {
      const a = await Room.create('hb')
      const me = await a.join()
      const adapter = getBroadcastAdapter()
      const key = roomMemberKvKey('hb', me.id)
      const before = (parse((await adapter.get!(key)) as string) as RoomMemberRecord).seenAt

      await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)

      const after = (parse((await adapter.get!(key)) as string) as RoomMemberRecord).seenAt
      expect(after).toBe(before + ROOM_HEARTBEAT_INTERVAL_MS)
    } finally {
      vi.useRealTimers()
    }
  })

  it('a heartbeat discovering its member gone (reaped elsewhere) applies the leave locally', async () => {
    vi.useFakeTimers()
    try {
      const a = await Room.create('hb-gone')
      const me = await a.join()
      let left = false
      me.onLeave(() => (left = true))

      await getBroadcastAdapter().delete!(roomMemberKvKey('hb-gone', me.id))
      await vi.advanceTimersByTimeAsync(ROOM_HEARTBEAT_INTERVAL_MS)

      expect(left).toBe(true)
      expect(a.count).toBe(0)
    } finally {
      vi.useRealTimers()
    }
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
    await expect(Room.create('kv-less')).rejects.toThrow(/KV methods required by `Room`/)
  })
})
