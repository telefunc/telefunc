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
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_KV_TTL_MS, ROOM_MEMBER_TTL_MS } from '../constants.js'
import { ACK_STATUS, TAG, decode } from '../shared-ws.js'
import { Room, ServerRoom, type ServerLocalParticipant } from './server.js'
import { RoomStubChannel } from './stubs.js'
import { ClientRoom, ClientStandaloneParticipant } from './client.js'
import {
  RoomState,
  frameWithMemberId,
  roomCtrlKey,
  roomTextKey,
  roomMemberDataKey,
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

  it('getOrCreate creates once and converges — concurrent callers, existing rooms, later options ignored', async () => {
    // Two concurrent boots: both read "missing", one wins the create, the loser gets.
    const [a, b] = await Promise.all([
      Room.getOrCreate('boot', { meta: { topic: 'first' }, size: 3 }),
      Room.getOrCreate('boot', { meta: { topic: 'first' }, size: 3 }),
    ])
    expect(a.meta).toEqual({ topic: 'first' })
    expect(b.meta).toEqual({ topic: 'first' })
    expect(a.size).toBe(3)

    // Already exists: returned as-is, options don't overwrite.
    const again = await Room.getOrCreate('boot', { meta: { topic: 'second' }, size: 99 })
    expect(again.meta).toEqual({ topic: 'first' })
    expect(again.size).toBe(3)
  })

  it('update replaces provided fields only — updating the topic never resets the capacity', async () => {
    await Room.create('cfg', { meta: { topic: 'general' }, size: 5 })
    const observer = await Room.get('cfg')
    observer.onUpdate(() => {}) // observing — receives update events

    await Room.update('cfg', { meta: { topic: 'renamed' } })
    expect((await Room.get('cfg')).size).toBe(5) // omitted — kept, not reset to Infinity
    expect((await Room.get('cfg')).meta).toEqual({ topic: 'renamed' })

    await Room.update('cfg', { size: 10 })
    const after = await Room.get('cfg')
    expect(after.meta).toEqual({ topic: 'renamed' }) // omitted — kept
    expect(after.size).toBe(10)
    expect(observer.size).toBe(10) // the update event carried the effective config
    expect(observer.meta).toEqual({ topic: 'renamed' })
  })

  it('list({ prefix }) filters by room-ID prefix', async () => {
    await Room.create('chat:a')
    await Room.create('chat:b')
    await Room.create('voice:c')
    expect((await Room.list({ prefix: 'chat:' })).map((r) => r.id).sort()).toEqual(['chat:a', 'chat:b'])
    expect((await Room.list()).length).toBe(3)
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

  it('Room.get() never reads member records up front — one scan for the count, roster lazy', async () => {
    await Room.create('scan')
    await Room.join('scan', { n: 1 })
    await Room.join('scan', { n: 2 })
    await settle()

    const reads = vi.spyOn(getBroadcastAdapter(), 'get')
    const scans = vi.spyOn(getBroadcastAdapter(), 'keys')
    const room = await Room.get('scan')

    expect(room.count).toBe(2) // exact, from the scan
    expect(scans.mock.calls.length).toBe(1)
    expect(reads.mock.calls.filter((c) => String(c[0]).includes(':m:'))).toEqual([]) // no member reads
    expect((await room.getParticipants()).length).toBe(2) // the roster loads on first need
    reads.mockRestore()
    scans.mockRestore()
  })

  it('Room.join() pays not even the count scan — a pure joiner loads no roster, ever', async () => {
    await Room.create('scan2')
    const scans = vi.spyOn(getBroadcastAdapter(), 'keys')
    const reads = vi.spyOn(getBroadcastAdapter(), 'get')
    await Room.join('scan2', { n: 1 })
    await settle()
    expect(scans.mock.calls.length).toBe(0) // roster loads are need-driven; a joiner has no need
    expect(reads.mock.calls.filter((c) => String(c[0]).includes(':m:'))).toEqual([])
    scans.mockRestore()
    reads.mockRestore()
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

  it('update() replaces provided fields, keeps the rest, and fires onUpdate', async () => {
    const lobby = await Room.create('conf', { meta: { topic: 'a' }, size: 2 })
    const updates: Array<[unknown, unknown]> = []
    lobby.onUpdate((meta, prev) => updates.push([meta, prev]))

    await Room.update('conf', { meta: { topic: 'b' } })

    expect(updates).toEqual([[{ topic: 'b' }, { topic: 'a' }]])
    expect(lobby.meta).toEqual({ topic: 'b' })
    expect(lobby.size).toBe(2) // omitted — kept
    await expect(Room.update('conf', { isolated: true })).rejects.toThrow('fixed at creation')
    await expect(Room.update('gone', {})).rejects.toThrow('Room not found: gone')
  })

  it('concurrent updates converge to the same winner on every node, whatever the arrival order', () => {
    const view = (events: Array<{ at: number; by: string; topic: string }>) => {
      const state = new RoomState({
        roomId: 'lww',
        meta: { topic: 'seed' },
        size: Infinity,
        seed: { members: [] },
        updateStamp: { at: 0, by: '' },
        onListenersChanged: () => {},
        onCallbackError: () => {},
      })
      for (const e of events) state.applyRoomUpdate({ topic: e.topic }, {}, Infinity, e.at, e.by)
      return state.meta
    }
    const a = { at: 5, by: 'writer-a', topic: 'from-a' }
    const b = { at: 5, by: 'writer-b', topic: 'from-b' } // same instant — the tie breaks by writer
    expect(view([a, b])).toEqual({ topic: 'from-b' })
    expect(view([b, a])).toEqual({ topic: 'from-b' }) // arrival order is irrelevant
    expect(view([a, b, a])).toEqual({ topic: 'from-b' }) // replays and echoes are absorbed
  })

  it('back-to-back updates from one writer always order — the stamp outruns a frozen clock', async () => {
    const lobby = await Room.create('rapid', { meta: { v: 0 } })
    lobby.onUpdate(() => {}) // observe — an unobserved room doesn't follow events
    await Room.update('rapid', { meta: { v: 1 } })
    await Room.update('rapid', { meta: { v: 2 } }) // same millisecond as v1 — must still win
    expect(lobby.meta).toEqual({ v: 2 })
  })

  it('a stale p-meta revision never overwrites a newer one', async () => {
    const lobby = await Room.create('rev')
    const me = await lobby.join({ v: 0 })
    await me.setMeta({ v: 1 })
    await me.setMeta({ v: 2 })

    // A duplicate delivery of the older event (broker redelivery, echo) arrives late.
    await getBroadcastAdapter().publish(
      roomCtrlKey('rev'),
      stringify({ __r: 'p-meta', id: me.id, meta: { v: 1 }, prev: { v: 0 }, seq: 1 }),
    )

    expect((await lobby.getParticipant(me.id))!.meta).toEqual({ v: 2 })
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

  it('every leave carries its cause — kick reasons travel with the removal, nothing to race', async () => {
    const lobby = await Room.create('causes')
    const observer = await Room.get('causes')
    const observed: Array<[string, unknown]> = []
    observer.onLeave((m, cause) => observed.push([String(m.meta.name), cause]))

    // Voluntary leave.
    const alice = await lobby.join({ name: 'Alice' })
    const aliceCauses: unknown[] = []
    alice.onLeave((cause) => aliceCauses.push(cause))
    await alice.leave()
    expect(aliceCauses).toEqual([{ type: 'left' }])

    // Kick, with the reason riding the removal event itself.
    const bob = await lobby.join({ name: 'Bob' })
    const bobCauses: unknown[] = []
    bob.onLeave((cause) => bobCauses.push(cause))
    await Room.removeParticipant('causes', bob.id, { reason: { rule: 'spam' } })
    expect(bobCauses).toEqual([{ type: 'removed', reason: { rule: 'spam' } }])

    expect(observed).toEqual([
      ['Alice', { type: 'left' }],
      ['Bob', { type: 'removed', reason: { rule: 'spam' } }],
    ])

    // Room closure.
    const carol = await lobby.join({ name: 'Carol' })
    const carolCauses: unknown[] = []
    carol.onLeave((cause) => carolCauses.push(cause))
    await Room.close('causes')
    expect(carolCauses).toEqual([{ type: 'closed' }])

    // Late subscribers still learn the cause.
    const late: unknown[] = []
    carol.onLeave((cause) => late.push(cause))
    expect(late).toEqual([{ type: 'closed' }])
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
    expect((await b.getParticipant(me.id))!.joinedAt).toBeGreaterThan(0)
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
    ;(await a.getParticipant(me.id))!.onUpdate((meta, prev) => seenOnA.push([meta, prev]))

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
    expect((await b.getParticipant(me.id))!.meta).toEqual({ name: 'Alice', score: 42 })
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
    ;(await lobby.getParticipant(alice.id))!.subscribe((data) => fromAlice.push(data))

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

  it('text rides its own lane — the control key never carries data', async () => {
    const room = await Room.create('lanes')
    const ctrlTraffic: string[] = []
    const textTraffic: string[] = []
    Broadcast.subscribe<{ __r: string }>(roomCtrlKey('lanes'), (msg) => ctrlTraffic.push(msg.__r))
    Broadcast.subscribe<{ __r: string }>(roomTextKey('lanes'), (msg) => textTraffic.push(msg.__r))

    const me = await room.join({ name: 'Alice' })
    await me.publish('hello')
    await Room.announce('lanes', 'notice')

    expect(ctrlTraffic).toEqual(['join', 'announce'])
    expect(textTraffic).toEqual(['data'])
  })

  it('binary rides per-publisher keys in shared mode too — upstream subscriptions are member-selective', async () => {
    const a = await Room.create('per-pub')
    const cam1 = await a.join({ name: 'cam1' })
    const cam2 = await a.join({ name: 'cam2' })
    const b = await Room.get('per-pub')
    await b.getParticipants() // materialize the lazy roster
    const subscribed = vi.spyOn(getBroadcastAdapter(), 'subscribeBinary')

    const frames: number[][] = []
    ;(await b.getParticipant(cam1.id))!.subscribeBinary((data) => frames.push([...data]))

    expect(subscribed.mock.calls.map((c) => c[0])).toEqual([roomMemberDataKey('per-pub', cam1.id)])
    await cam1.publishBinary(new Uint8Array([1]))
    await cam2.publishBinary(new Uint8Array([2]))
    expect(frames).toEqual([[1]])
    subscribed.mockRestore()
  })

  it("a message racing ahead of its sender's join delivers with the envelope's verified identity", async () => {
    const room = await Room.create('race')
    const received: Array<{ data: unknown; id: string; meta: unknown }> = []
    room.subscribe((data, _info, from) => received.push({ data, id: from.id, meta: from.meta }))

    // Control and data travel on separate lanes, so a message can beat its sender's join —
    // the envelope's node-stamped identity makes delivery immediate and correct anyway.
    const ghost = crypto.randomUUID()
    await getBroadcastAdapter().publish(
      roomTextKey('race'),
      stringify({ __r: 'data', from: ghost, fromMeta: { name: 'Zoe' }, data: 'first!' }),
    )

    expect(received).toEqual([{ data: 'first!', id: ghost, meta: { name: 'Zoe' } }])
    expect(await room.getParticipant(ghost)).toBe(null) // presence stays event-driven — no ghost member
  })

  it('an unknown sender heals the drifted view — narrated, and re-synced to clients', async () => {
    const a = await Room.create('drift')
    const observer = await Room.get('drift')
    observer.subscribe(() => {}) // materialize + observe the roster (currently empty)
    const joins: string[] = []
    observer.onJoin((m) => joins.push(String(m.meta.name)))
    await settle()

    // Simulate a dropped join event: the member exists in KV, but its ctrl event never arrived.
    const adapter = getBroadcastAdapter()
    const realPublish = adapter.publish.bind(adapter)
    const drop = vi
      .spyOn(adapter, 'publish')
      .mockImplementation((key, payload) =>
        key === roomCtrlKey('drift') && payload.includes('"join"')
          ? { seq: 0, timestamp: 0 }
          : realPublish(key, payload),
      )
    const ghostly = await a.join({ name: 'Casper' })
    drop.mockRestore()
    expect(await observer.getParticipant(ghostly.id)).toBe(null) // the view drifted

    // A client stub seeded from the drifted view must be re-synced too.
    const stub = new RoomStubChannel(observer as ServerRoom)
    stub._registerChannel()
    ;(observer as ServerRoom)._attachStub(stub)

    // The ghost's message delivers immediately (identity rides the envelope)…
    const seen: Array<{ data: unknown; id: string; meta: unknown }> = []
    observer.subscribe((data, _info, from) => seen.push({ data, id: from.id, meta: from.meta }))
    await ghostly.publish('boo')
    expect(seen).toEqual([{ data: 'boo', id: ghostly.id, meta: { name: 'Casper' } }])

    // …and acts as the drift signal: the view heals, live object included, and the discovery
    // is narrated — onJoin fires from the authoritative KV read (never from the message).
    await settle()
    expect((await observer.getParticipant(ghostly.id))?.meta).toEqual({ name: 'Casper' })
    expect(joins).toEqual(['Casper'])
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
    await b.getParticipants() // materialize the lazy roster

    const frames: string[] = []
    ;(await b.getParticipant(cam1.id))!.subscribeBinary((data) => frames.push(`cam1:${data[0]}`))
    await cam1.publishBinary(new Uint8Array([1]))
    await cam2.publishBinary(new Uint8Array([2])) // nobody wants cam2 — b never subscribed its key

    expect(frames).toEqual(['cam1:1'])
  })

  it('isolated mode narrows the upstream text keys to the wanted members too', async () => {
    const a = await Room.create('sel-text', { isolated: true })
    const alice = await a.join({ name: 'alice' })
    const bob = await a.join({ name: 'bob' })
    const b = await Room.get('sel-text')
    await b.getParticipants() // materialize the lazy roster

    const subscribed = vi.spyOn(getBroadcastAdapter(), 'subscribe')
    const heard: unknown[] = []
    ;(await b.getParticipant(alice.id))!.subscribe((data) => heard.push(data))
    const keys = () => subscribed.mock.calls.map(([key]) => key)
    expect(keys()).toContain(roomMemberDataKey('sel-text', alice.id))
    expect(keys()).not.toContain(roomMemberDataKey('sel-text', bob.id))

    await alice.publish('a1')
    await bob.publish('b1') // b never subscribed bob's key — nothing arrives, nothing to filter
    expect(heard).toEqual(['a1'])

    // A room-level subscription widens upstream to every member's key.
    b.subscribe(() => {})
    expect(keys()).toContain(roomMemberDataKey('sel-text', bob.id))
  })

  it('a shared-mode member-scoped listener still brings up the room text lane', async () => {
    const a = await Room.create('sel-shared')
    const alice = await a.join({ name: 'alice' })
    const bob = await a.join({ name: 'bob' })
    const b = await Room.get('sel-shared')
    await b.getParticipants() // materialize the lazy roster

    const heard: unknown[] = []
    ;(await b.getParticipant(alice.id))!.subscribe((data) => heard.push(data))
    await alice.publish('a1')
    await bob.publish('b1') // one shared key — it reaches the node; the view filters it out
    expect(heard).toEqual(['a1'])
  })

  it("releases a departed member's listeners — the decoder pattern must not pin subscriptions", async () => {
    const a = await Room.create('release', { isolated: true })
    const cam = await a.join({ name: 'cam' })
    const b = await Room.get('release')
    await b.getParticipants() // materialize the lazy roster

    // The documented pattern: subscribe on join, close the decoder on leave — no unsubscribe.
    ;(await b.getParticipant(cam.id))!.subscribeBinary(() => {})
    expect(await b.getParticipant(cam.id)).not.toBe(null)

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

    await alice.send((await a.getParticipant(bob.id))!, 'psst') // target as object — or pass the ID

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

  it('Room.guard({ onSend }) guards sends: rejections reach the sender, the guard sees rich identities', async () => {
    await Room.create('guarded')
    const lobby = await Room.get('guarded')
    Room.guard(lobby, {
      onSend: (from, to, data) => {
        if (data === 'blocked') throw new Error('not friends')
        expect(from.meta).toEqual({ name: 'Alice' }) // resolved sender, meta included
        expect(to.meta).toEqual({ name: 'Bob' }) // resolved target, meta included
      },
    })
    const bob = await lobby.join({ name: 'Bob' })
    const inbox: unknown[] = []
    bob.listen((data, from) => inbox.push([data, from?.id, from?.meta]))
    const alice = await lobby.join({ name: 'Alice' })

    await expect(alice.send(bob.id, 'blocked')).rejects.toThrow('not friends')
    await alice.send(bob.id, 'hi')

    expect(inbox).toEqual([['hi', alice.id, { name: 'Alice' }]])
  })

  it('Room.guard({ onSend }) guards client-side joins made through that instance', async () => {
    await Room.create('gated')
    const served = (await Room.get('gated')) as ServerRoom
    Room.guard(served, {
      onSend: (from, _to, data) => {
        if (data === 'blocked') throw new Error(`no messages from ${from.meta.name}`)
      },
    })
    const stub = new RoomStubChannel(served)
    stub._registerChannel()
    served._attachStub(stub)
    const target = await served.join({ name: 'T' })
    const inbox: unknown[] = []
    target.listen((data) => inbox.push(data))
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: { name: 'C' } }), 1)

    const memberId = [...stub._stubMembers.keys()][0]!
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-dm', id: memberId, to: target.id, data: 'blocked' }), 2)
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-dm', id: memberId, to: target.id, data: 'hi' }), 3)

    expect(inbox).toEqual(['hi']) // the guarded send never delivered
  })

  it('Room.guard({ onPublish }) gates room-wide messages — text and binary, server and client joins', async () => {
    await Room.create('moderated')
    const served = (await Room.get('moderated')) as ServerRoom
    Room.guard(served, {
      onPublish: (from, data) => {
        if (data === 'slur' || (data instanceof Uint8Array && data[0] === 0xff)) {
          throw new Error(`blocked: ${from.meta.name}`)
        }
      },
    })
    const observer = await Room.get('moderated')
    const seen: unknown[] = []
    observer.subscribe((data) => seen.push(data))
    const me = await served.join({ name: 'Mallory' })

    await expect(me.publish('slur')).rejects.toThrow('blocked: Mallory')
    await expect(me.publishBinary(new Uint8Array([0xff, 1]))).rejects.toThrow('blocked: Mallory')
    await me.publish('fine')
    expect(seen).toEqual(['fine'])

    // Client-side joins through the same instance hit the same guard, and the rejection
    // travels back through the publish ack.
    const stub = new RoomStubChannel(served)
    stub._registerChannel()
    served._attachStub(stub)
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: { name: 'C' } }), 1)
    const memberId = [...stub._stubMembers.keys()][0]!
    await stub._onPeerPublishAckReqMessage(stringify({ __r: 'data', from: memberId, data: 'slur' }), 2)
    expect(seen).toEqual(['fine']) // never published
  })

  it('Room.guard({ onJoin }) gates admission — server and client joins, a rejected join writes nothing', async () => {
    await Room.create('door')
    const served = (await Room.get('door')) as ServerRoom
    const seen: { id: string; meta: Record<string, unknown> }[] = []
    Room.guard(served, {
      onJoin: (member) => {
        seen.push(member)
        if (member.meta.name === 'Banned') throw new Error(`no entry for ${member.meta.name}`)
      },
    })

    await expect(served.join({ name: 'Banned' })).rejects.toThrow('no entry for Banned')
    // The guard runs before any state is written — a rejected join leaves no trace.
    expect(served.count).toBe(0)
    expect(await (await Room.get('door')).getParticipants()).toEqual([])

    const alice = await served.join({ name: 'Alice' })
    expect(seen.map((m) => m.meta)).toEqual([{ name: 'Banned' }, { name: 'Alice' }])
    expect(seen[1]!.id).toBe(alice.id) // the guard saw the definitive member ID

    // Client-side joins through the same instance hit the same guard; the rejection rides the ack.
    const stub = new RoomStubChannel(served)
    stub._registerChannel()
    served._attachStub(stub)
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: { name: 'Banned' } }), 1)
    expect(stub._stubMembers.size).toBe(0) // rejected — nothing admitted
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: { name: 'Casey' } }), 2)
    expect(stub._stubMembers.size).toBe(1)
  })

  it('guards ride the granted instance — the Room.join() static is server-authored and unguarded', async () => {
    await Room.create('velvet')
    const granted = await Room.get('velvet')
    Room.guard(granted, {
      onJoin: () => {
        throw new Error('nobody enters')
      },
    })
    await expect(granted.join()).rejects.toThrow('nobody enters')
    const me = await Room.join('velvet', { name: 'Direct' }) // no grant involved — like Room.announce() vs onPublish
    expect(me.meta).toEqual({ name: 'Direct' })
  })

  it('Room.guard() is one-shot and validates its arguments', async () => {
    await Room.create('strict')
    const room = await Room.get('strict')
    Room.guard(room, { onSend: () => {} })
    expect(() => Room.guard(room, { onSend: () => {} })).toThrow('already called')
    // @ts-expect-error — runtime validation
    expect(() => Room.guard(room, { onPublish: 'nope' })).toThrow('should be a function')
    // @ts-expect-error — runtime validation
    expect(() => Room.guard(room, { onJoin: 'nope' })).toThrow('should be a function')
    expect(() => Room.guard({} as never, {})).toThrow('expects a room')
  })

  it("delivers to a member the sender's stale local view doesn't know yet (KV fallback)", async () => {
    const a = await Room.create('dm-lag')
    const alice = await a.join({ name: 'Alice' })
    const b = await Room.get('dm-lag') // snapshot: alice only
    const bob = await a.join({ name: 'Bob' }) // b is unobserved — it missed this join
    const bobInbox: unknown[] = []
    bob.listen((data, from) => bobInbox.push([data, from?.id]))

    // b's local view lags; the KV member record is authoritative.
    await (b as ServerRoom)._sendDm(alice.id, bob.id, 'catch-up')

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
  it('routes text over per-member keys while control stays on the control key', async () => {
    const a = await Room.create('vid', { isolated: true })
    const b = await Room.get('vid')
    const received: unknown[] = []
    b.subscribe((data, _info, from) => received.push([data, from.meta]))

    const ctrlTraffic: string[] = []
    const textTraffic: string[] = []
    Broadcast.subscribe<{ __r: string }>(roomCtrlKey('vid'), (msg) => ctrlTraffic.push(msg.__r))
    Broadcast.subscribe<{ __r: string }>(roomTextKey('vid'), (msg) => textTraffic.push(msg.__r))

    const me = await a.join({ name: 'Alice' })
    await me.publish('frame-1')

    expect(received).toEqual([['frame-1', { name: 'Alice' }]]) // delivered via the member key
    expect(ctrlTraffic).toEqual(['join']) // the control key carried only control
    expect(textTraffic).toEqual([]) // isolated: not even the shared text key is touched
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
    expect((await serverRoom.getParticipant(ack.id))!.meta).toEqual({ name: 'Remote' })
  })

  it('room events are relayed to the client as PUBLISH frames, behind the streamed roster', async () => {
    const { serverRoom, peer } = await createServedRoom('relay')
    await settle() // the roster streams once the peer attaches
    await serverRoom.join({ name: 'Alice' })

    const relayed = peer
      .decoded()
      .filter((f) => f.tag === TAG.PUBLISH)
      .map((f) => (JSON.parse(f.text) as { __r: string }).__r)
    expect(relayed).toEqual(['roster', 'join']) // roster first — join applies on top of it
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

  it('a client-forged `fromMeta` never reaches the room — the server stamps the verified one', async () => {
    const { stub, peer } = await createServedRoom('stamp')
    const observer = await Room.get('stamp')
    const metas: unknown[] = []
    observer.subscribe((_data, _info, from) => metas.push(from.meta))
    const { id } = await joinViaStub(stub, peer, 1) // joins as { name: 'Remote' }

    await stub._onPeerPublishAckReqMessage(
      stringify({ __r: 'data', from: id, fromMeta: { name: 'Admin' }, data: 'hi' }),
      2,
    )

    expect(metas).toEqual([{ name: 'Remote' }])
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

  it('text is relayed only after the client subscribes; control always flows', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('lazy-text-stub')
    const me = await serverRoom.join({ name: 'Alice' })

    await me.publish('before-subscribe')
    const relayedTags = () =>
      peer
        .decoded()
        .filter((f) => f.tag === TAG.PUBLISH)
        .map((f) => (JSON.parse(f.text) as { __r: string }).__r)
        .filter((tag) => tag !== 'roster')
    expect(relayedTags()).toEqual(['join']) // control flowed, the data frame didn't cross the wire

    stub._onPeerBroadcastSubscribe(false) // the client's `subscribe()` signal
    await me.publish('after-subscribe')
    expect(relayedTags()).toEqual(['join', 'data'])

    stub._onPeerBroadcastUnsubscribe(false)
    await me.publish('after-unsubscribe')
    expect(relayedTags()).toEqual(['join', 'data'])
  })

  it('sub-text relays only the wanted members; a room-level subscription supersedes the set', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('member-text-stub')
    const alice = await serverRoom.join({ name: 'Alice' })
    const bob = await serverRoom.join({ name: 'Bob' })

    const relayedData = () =>
      peer
        .decoded()
        .filter((f) => f.tag === TAG.PUBLISH)
        .map((f) => JSON.parse(f.text) as { __r: string; data?: unknown })
        .filter((m) => m.__r === 'data')
        .map((m) => m.data)

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-text', members: [alice.id] }), 50)
    await alice.publish('from-alice')
    await bob.publish('from-bob') // not in the want set — never crosses the wire
    expect(relayedData()).toEqual(['from-alice'])

    stub._onPeerBroadcastSubscribe(false) // room-level subscription — everything flows
    await bob.publish('bob-now-flows')
    expect(relayedData()).toEqual(['from-alice', 'bob-now-flows'])

    stub._onPeerBroadcastUnsubscribe(false) // back to the member set
    await bob.publish('bob-dropped')
    await alice.publish('alice-still-flows')
    expect(relayedData()).toEqual(['from-alice', 'bob-now-flows', 'alice-still-flows'])
  })

  it('a stub member joined with selfDelivery=false gets no echo of its own publishes', async () => {
    const { stub, peer } = await createServedRoom('quiet-stub')
    stub._onPeerBroadcastSubscribe(false) // the client listens for data — the echo skip must still hold
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
  close: () => void
  published: unknown[]
  sent: Array<{ __r: string }>
  textSubscribed: () => boolean
  stub: ClientBroadcast
}

function createFakeStub(joinAck?: { id: string }): FakeStub {
  const textCbs: Array<(data: unknown, info: ChannelPublishInfo) => void> = []
  const binaryCbs: Array<(data: Uint8Array, info: ChannelPublishInfo) => void> = []
  const published: unknown[] = []
  const sent: Array<{ __r: string }> = []
  const closeCbs: Array<() => void> = []
  let wireTextSubscribed = false
  let seq = 0
  const info = () => ({ key: 'fake', seq: ++seq, timestamp: 1 })
  const stub = {
    _subscribeLocal: (cb: (data: unknown, info: ChannelPublishInfo) => void) => {
      textCbs.push(cb)
      return () => textCbs.splice(textCbs.indexOf(cb), 1)
    },
    _subscribeBinaryLocal: (cb: (data: Uint8Array, info: ChannelPublishInfo) => void) => {
      binaryCbs.push(cb)
      return () => binaryCbs.splice(binaryCbs.indexOf(cb), 1)
    },
    _setWireTextSubscribed: (on: boolean) => {
      wireTextSubscribed = on
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
    onClose: (cb: () => void) => closeCbs.push(cb),
  }
  return {
    emit: (envelope) => [...textCbs].forEach((cb) => cb(envelope, info())),
    emitBinary: (framed) => [...binaryCbs].forEach((cb) => cb(framed, info())),
    close: () => [...closeCbs].forEach((cb) => cb()),
    published,
    sent,
    textSubscribed: () => wireTextSubscribed,
    stub: stub as unknown as ClientBroadcast,
  }
}

function createSnapshot(roomId: string, partial?: Partial<RoomSnapshotMetadata>): RoomSnapshotMetadata {
  return {
    channelId: 'ch1',
    roomId,
    meta: {},
    size: null,
    isolated: false,
    closed: false,
    count: 0,
    stamp: { at: 0, by: '' },
    ...partial,
  }
}

describe('ClientRoom', () => {
  it('seeds the count from the snapshot; the streamed roster then makes the view authoritative', async () => {
    const fake = createFakeStub()
    const alice = crypto.randomUUID()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('snap', { count: 1 }))
    const joins: string[] = []
    clientRoom.onJoin((m) => joins.push(m.id))

    expect(clientRoom.count).toBe(1) // scalar seed — exact before the roster even arrives
    const membersPending = clientRoom.getParticipants() // parks until the roster streams in

    fake.emit({ __r: 'roster', members: [{ id: alice, meta: { name: 'Alice' }, joinedAt: 1 }] })
    expect((await membersPending).map((m) => m.id)).toEqual([alice])
    expect(joins).toEqual([]) // the roster seeds silently — it is not a join event

    fake.emit({ __r: 'join', id: alice, meta: { name: 'Alice' }, joinedAt: 1 }) // echo overlap
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
    me.onLeave((cause) => log.push(`me-left:${JSON.stringify(cause)}`))

    fake.emit({ __r: 'p-meta', id: me.id, meta: { mood: 'happy' }, prev: {}, seq: 1 })
    expect(me.meta).toEqual({ mood: 'happy' })

    fake.emit({ __r: 'update', meta: { topic: 'b' }, prev: { topic: 'a' }, size: null, at: 9, by: 'w1' })
    expect(clientRoom.size).toBe(Infinity)

    fake.emit({ __r: 'leave', id: me.id, cause: 'removed', reason: 'be nice' }) // kicked, told why
    fake.emit({ __r: 'closed' })

    expect(log).toEqual(['update:{"topic":"b"}', 'me-left:{"type":"removed","reason":"be nice"}', 'closed'])
    expect(clientRoom.isClosed).toBe(true)
    await expect(me.publish('x')).rejects.toThrow('Participant has left')
  })

  it("wire death surfaces as cause 'disconnected' on the client's own participants", async () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('wire-death'))
    const me = await clientRoom.join()
    const causes: unknown[] = []
    me.onLeave((cause) => causes.push(cause))

    fake.close() // the connection died — no `closed` event preceded it

    expect(causes).toEqual([{ type: 'disconnected' }])
    expect(clientRoom.isClosed).toBe(true)
  })

  it('declares the text want only while data listeners exist — presence stays wire-free of chatter', () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('lazy-text'))
    clientRoom.onJoin(() => {}) // presence listeners alone declare nothing
    expect(fake.textSubscribed()).toBe(false)

    const unsubscribe = clientRoom.subscribe(() => {})
    expect(fake.textSubscribed()).toBe(true) // declared synchronously — FIFO-safe with a publish right after

    unsubscribe()
    expect(fake.textSubscribed()).toBe(false)
  })

  it('binary delivery follows the declared wants; frames route to listeners', () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('bin'))
    expect(fake.sent.filter((m) => m.__r === 'sub-binary')).toEqual([])

    const bytes: number[][] = []
    const unsubscribe = clientRoom.subscribeBinary((data) => bytes.push([...data]))
    expect(fake.sent.filter((m) => m.__r === 'sub-binary').at(-1)).toEqual({
      __r: 'sub-binary',
      all: true,
      members: [],
    })

    const sender = crypto.randomUUID()
    fake.emit({ __r: 'join', id: sender, meta: {}, joinedAt: 1 })
    fake.emitBinary(frameWithMemberId(sender, new Uint8Array([9, 8])))
    expect(bytes).toEqual([[9, 8]])

    unsubscribe()
    expect(fake.sent.filter((m) => m.__r === 'sub-binary').at(-1)).toEqual({
      __r: 'sub-binary',
      all: false,
      members: [],
    })
  })

  it('declares its binary wants to the server — member-selective, sent synchronously, deduped', async () => {
    const cam1 = crypto.randomUUID()
    const cam2 = crypto.randomUUID()
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('wants', { count: 2 }))
    fake.emit({
      __r: 'roster',
      members: [
        { id: cam1, meta: {}, joinedAt: 1 },
        { id: cam2, meta: {}, joinedAt: 2 },
      ],
    })
    const subBinaryMsgs = () => fake.sent.filter((m) => m.__r === 'sub-binary')

    // Each widening is declared synchronously — a publish right after subscribing must be
    // preceded by its declaration on the wire (same-connection FIFO).
    const unsub1 = (await clientRoom.getParticipant(cam1))!.subscribeBinary(() => {})
    expect(subBinaryMsgs()).toEqual([{ __r: 'sub-binary', all: false, members: [cam1] }])
    ;(await clientRoom.getParticipant(cam2))!.subscribeBinary(() => {})
    expect(subBinaryMsgs().at(-1)).toEqual({ __r: 'sub-binary', all: false, members: [cam1, cam2] })

    // A room-level listener upgrades the declaration to `all`; listener changes that leave the
    // effective set unchanged send nothing.
    const unsubAll = clientRoom.subscribeBinary(() => {})
    expect(subBinaryMsgs().at(-1)).toEqual({ __r: 'sub-binary', all: true, members: [] })
    const sentCount = subBinaryMsgs().length
    const unsubDup = (await clientRoom.getParticipant(cam1))!.subscribeBinary(() => {})
    expect(subBinaryMsgs().length).toBe(sentCount)

    // Dropping back to one member narrows it again.
    unsubDup()
    unsubAll()
    unsub1()
    expect(subBinaryMsgs().at(-1)).toEqual({ __r: 'sub-binary', all: false, members: [cam2] })
  })

  it('declares member-scoped text wants — sub-text rides beside the broadcast subscription', async () => {
    const alice = crypto.randomUUID()
    const bob = crypto.randomUUID()
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('text-wants', { count: 2 }))
    fake.emit({
      __r: 'roster',
      members: [
        { id: alice, meta: {}, joinedAt: 1 },
        { id: bob, meta: {}, joinedAt: 2 },
      ],
    })
    const subTextMsgs = () => fake.sent.filter((m) => m.__r === 'sub-text')

    // Participant-scoped listeners declare a member set — no room-level broadcast subscription.
    const unsubAlice = (await clientRoom.getParticipant(alice))!.subscribe(() => {})
    expect(fake.textSubscribed()).toBe(false)
    expect(subTextMsgs()).toEqual([{ __r: 'sub-text', members: [alice] }])
    const unsubBob = (await clientRoom.getParticipant(bob))!.subscribe(() => {})
    expect(subTextMsgs().at(-1)).toEqual({ __r: 'sub-text', members: [alice, bob] })

    // A room-level subscribe() upgrades to the broadcast subscription and clears the member
    // set; listener changes that leave the effective want unchanged send nothing.
    const unsubAll = clientRoom.subscribe(() => {})
    expect(fake.textSubscribed()).toBe(true)
    expect(subTextMsgs().at(-1)).toEqual({ __r: 'sub-text', members: [] })
    const sentCount = subTextMsgs().length
    const unsubDup = (await clientRoom.getParticipant(alice))!.subscribe(() => {})
    expect(subTextMsgs().length).toBe(sentCount)

    // Narrowing back re-declares the member set.
    unsubDup()
    unsubAll()
    expect(fake.textSubscribed()).toBe(false)
    expect(subTextMsgs().at(-1)).toEqual({ __r: 'sub-text', members: [alice, bob] })
    unsubBob()
    expect(subTextMsgs().at(-1)).toEqual({ __r: 'sub-text', members: [alice] })
    unsubAlice()
    expect(subTextMsgs().at(-1)).toEqual({ __r: 'sub-text', members: [] })
  })

  it("a member leaving releases its listeners — the client's declaration narrows without an unsubscribe", async () => {
    const cam = crypto.randomUUID()
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('wants-release', { count: 1 }))
    fake.emit({ __r: 'roster', members: [{ id: cam, meta: {}, joinedAt: 1 }] })
    ;(await clientRoom.getParticipant(cam))!.subscribeBinary(() => {}) // never unsubscribed
    expect(fake.sent.filter((m) => m.__r === 'sub-binary').at(-1)).toEqual({
      __r: 'sub-binary',
      all: false,
      members: [cam],
    })

    fake.emit({ __r: 'leave', id: cam })

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
    const leaves: Array<[string, unknown]> = []
    observer.onLeave((m, cause) => leaves.push([m.id, cause]))
    await observer.getParticipants() // materialize the roster — leave events need the member view

    await backdate('crashed', me.id) // simulate: the owning node died 2 minutes ago

    const reader = await Room.get('crashed')
    expect(await reader.getParticipants()).toEqual([]) // reap-on-read: record deleted, leave announced
    expect(await Room.list()).toMatchObject([{ id: 'crashed', count: 0 }])
    expect(leaves).toEqual([[me.id, { type: 'disconnected' }]]) // the reaper knows it's a crash death
    expect(observer.count).toBe(0)
    expect(a.count).toBe(0) // the (supposed) owner learned via the reaper's event too
    await expect(me.publish('boo')).rejects.toThrow('Participant has left')
  })

  it("native KV expiry bounds a crashed node's leftovers even when nothing ever reads the room", async () => {
    vi.useFakeTimers()
    try {
      const a = await Room.create('abandoned')
      const me = await a.join({ name: 'Ghost' })
      const key = roomMemberKvKey('abandoned', me.id)
      const adapter = getBroadcastAdapter()
      expect(await adapter.get!(key)).not.toBe(null)

      // The owning node dies (no heartbeats), and no reader ever touches the room again.
      vi.setSystemTime(Date.now() + ROOM_MEMBER_KV_TTL_MS + 1)

      expect(await adapter.get!(key)).toBe(null) // the store expired it on its own
      expect(await adapter.keys!('telefunc:room:abandoned:m:')).toEqual([])
    } finally {
      vi.useRealTimers()
    }
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
