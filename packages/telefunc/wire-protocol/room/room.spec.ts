import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { Abort } from '../../shared/Abort.js'
import { Broadcast } from '../server/server-broadcast.js'
import {
  DefaultBroadcastAdapter,
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  type BroadcastAdapter,
} from '../server/broadcast.js'
import { IndexedPeer } from '../server/IndexedPeer.js'
import { ReplayBuffer } from '../replay-buffer.js'
import {
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_MEMBER_KV_TTL_MS,
  ROOM_MEMBER_TTL_MS,
  ROOM_ID_MAX_BYTES,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_TAIL_HOLD_BYTES_MAX,
  ROOM_TRACKS_PER_MEMBER_MAX,
} from '../constants.js'
import { ACK_STATUS, TAG, decode, encodePublishText, encodePublishBinary } from '../shared-ws.js'
import { Room, ServerRoom, type ServerLocalParticipant } from './server.js'
import { RoomStubChannel, bindParticipantStubChannel } from './stubs.js'
import { ClientRoom } from './client.js'
import { ServerChannel } from '../server/channel.js'
import { createStreamingReplacer } from '../server/response/registry.js'
import { createStreamingReviver } from '../client/response/registry.js'
import type { ClientReviverContext, ServerReplacerContext } from '../types.js'
import type { RemoteParticipant } from './types.js'
import {
  DEFAULT_TRACK,
  frameWithMemberId,
  roomCtrlKey,
  roomTextKey,
  roomConfigKvKey,
  roomRetainedBinaryPrefix,
  roomRetainedTextKey,
  roomMemberDataKey,
  roomMemberTrackKey,
  roomMemberKvKey,
  roomIdentityMemberKvKey,
  roomIdentityKvPrefix,
  unframeMemberId,
  sanitizeBinaryWants,
  pushBoundedTail,
  type RoomMemberRecord,
  type RoomSnapshotMetadata,
} from './protocol.js'
import { RoomState } from './state.js'
import type { ClientBroadcast } from '../client/channel.js'
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
    await Room.create('lobby', { meta: { topic: 'general' } })
    const lobby = await Room.get('lobby')
    expect(lobby.id).toBe('lobby')
    expect(lobby.meta).toEqual({ topic: 'general' })
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
      Room.getOrCreate('boot', { meta: { topic: 'first' } }),
      Room.getOrCreate('boot', { meta: { topic: 'first' } }),
    ])
    expect(a.meta).toEqual({ topic: 'first' })
    expect(b.meta).toEqual({ topic: 'first' })

    // Already exists: returned as-is, options don't overwrite.
    const again = await Room.getOrCreate('boot', { meta: { topic: 'second' } })
    expect(again.meta).toEqual({ topic: 'first' })
  })

  it('setMeta replaces the room meta wholesale and propagates to observers', async () => {
    await Room.create('cfg', { meta: { topic: 'general', pinned: true } })
    const observer = await Room.get('cfg')
    observer.onUpdate(() => {}) // observing — receives update events

    await Room.setMeta('cfg', { topic: 'renamed' }) // wholesale replace — `pinned` is dropped
    expect((await Room.get('cfg')).meta).toEqual({ topic: 'renamed' })
    expect(observer.meta).toEqual({ topic: 'renamed' }) // the update event carried the new meta
  })

  it('list({ prefix }) filters by room-ID prefix', async () => {
    await Room.create('chat:a')
    await Room.create('chat:b')
    await Room.create('voice:c')
    expect((await Room.list({ prefix: 'chat:' })).map((r) => r.id).sort()).toEqual(['chat:a', 'chat:b'])
    expect((await Room.list()).length).toBe(3)
  })

  it('Room.join() is a shorthand for get + join', async () => {
    await Room.create('shortcut')
    const observer = await Room.get('shortcut')

    const me = await Room.join('shortcut', { meta: { name: 'Bot' }, selfDelivery: false })

    expect(me.meta).toEqual({ name: 'Bot' })
    expect(me.selfDelivery).toBe(false)
    expect((await observer.getParticipants()).map((m) => m.meta)).toEqual([{ name: 'Bot' }])
    await expect(Room.join('nope')).rejects.toThrow('Room not found: nope')
  })

  it('Room.get() never reads member records up front — prefix scans for the count, roster lazy', async () => {
    await Room.create('scan')
    await Room.join('scan', { meta: { n: 1 } })
    await Room.join('scan', { meta: { n: 2 } })
    await settle()

    const reads = vi.spyOn(getBroadcastAdapter(), 'get')
    const scans = vi.spyOn(getBroadcastAdapter(), 'keys')
    const room = await Room.get('scan')

    expect(room.count).toBe(2) // exact, from the scans
    expect(scans.mock.calls.length).toBe(2) // two prefix scans — member keys, then hidden markers to exclude
    expect(reads.mock.calls.filter((c) => String(c[0]).includes(':m:'))).toEqual([]) // still no member reads
    expect((await room.getParticipants()).length).toBe(2) // the roster loads on first need
    reads.mockRestore()
    scans.mockRestore()
  })

  it('Room.join() pays not even the count scan — a pure joiner loads no roster, ever', async () => {
    await Room.create('scan2')
    const scans = vi.spyOn(getBroadcastAdapter(), 'keys')
    const reads = vi.spyOn(getBroadcastAdapter(), 'get')
    await Room.join('scan2', { meta: { n: 1 } })
    await settle()
    expect(scans.mock.calls.length).toBe(0) // roster loads are need-driven; a joiner has no need
    expect(reads.mock.calls.filter((c) => String(c[0]).includes(':m:'))).toEqual([])
    scans.mockRestore()
    reads.mockRestore()
  })

  it('list() reflects rooms and their live member counts', async () => {
    await Room.create('a')
    const b = await Room.create('b', { meta: { topic: 'x' } })
    await b.join()

    const rooms = (await Room.list()).sort((x, y) => x.id.localeCompare(y.id))
    expect(rooms).toEqual([
      { id: 'a', meta: {}, count: 0, isEmpty: true },
      { id: 'b', meta: { topic: 'x' }, count: 1, isEmpty: false },
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

  it('setMeta() replaces the room meta and fires onUpdate with the delta', async () => {
    const lobby = await Room.create('conf', { meta: { topic: 'a' } })
    const updates: Array<[unknown, unknown]> = []
    lobby.onUpdate((meta, prev) => updates.push([meta, prev]))

    await Room.setMeta('conf', { topic: 'b' })

    expect(updates).toEqual([[{ topic: 'b' }, { topic: 'a' }]])
    expect(lobby.meta).toEqual({ topic: 'b' })
    await expect(Room.setMeta('gone', {})).rejects.toThrow('Room not found: gone')
  })

  it('setAttributes() merges room meta per key and deletes on undefined — other keys survive', async () => {
    const lobby = await Room.create('room-attrs', { meta: { topic: 'a', pinned: 1 } })
    const updates: Array<[unknown, unknown]> = []
    lobby.onUpdate((meta, prev) => updates.push([meta, prev]))

    await Room.setAttributes('room-attrs', { topic: 'b' }) // only topic changes; pinned untouched

    expect(updates).toEqual([
      [
        { topic: 'b', pinned: 1 },
        { topic: 'a', pinned: 1 },
      ],
    ])
    expect(lobby.meta).toEqual({ topic: 'b', pinned: 1 })

    await Room.setAttributes('room-attrs', { pinned: undefined, topic: 'c' }) // delete a key while setting another
    expect(lobby.meta).toEqual({ topic: 'c' })
    await expect(Room.setAttributes('gone', {})).rejects.toThrow('Room not found: gone')
  })

  it('typed publish: a declared Pub types publish() and subscribe() end to end', async () => {
    type ChatMsg = { kind: 'chat'; text: string }
    const room = await Room.create<{ topic: string }, { name: string }, ChatMsg>('typed-pub', { meta: { topic: 't' } })
    const me = await room.join({ meta: { name: 'a' } })
    const received: ChatMsg[] = []
    // `data` carries no annotation — pushing it into ChatMsg[] only compiles if `Pub` threaded through.
    room.subscribe((data) => received.push(data))
    await me.publish({ kind: 'chat', text: 'hello' })
    await settle()
    expect(received).toEqual([{ kind: 'chat', text: 'hello' }])
  })

  it('concurrent updates converge to the same winner on every node, whatever the arrival order', () => {
    const view = (events: Array<{ at: number; by: string; topic: string }>) => {
      const state = new RoomState({
        roomId: 'lww',
        meta: { topic: 'seed' },
        seed: { members: [] },
        updateStamp: { at: 0, by: '' },
        onListenersChanged: () => {},
        onCallbackError: () => {},
      })
      for (const e of events) state.applyRoomUpdate({ topic: e.topic }, {}, e.at, e.by)
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
    await Room.setMeta('rapid', { v: 1 })
    await Room.setMeta('rapid', { v: 2 }) // same millisecond as v1 — must still win
    expect(lobby.meta).toEqual({ v: 2 })
  })

  it('a stale p-meta revision never overwrites a newer one', async () => {
    const lobby = await Room.create('rev')
    const me = await lobby.join({ meta: { v: 0 } })
    await me.setMeta({ v: 1 })
    await me.setMeta({ v: 2 })

    // A duplicate delivery of the older event (broker redelivery, echo) arrives late.
    await getBroadcastAdapter().publish(
      roomCtrlKey('rev'),
      stringify({ __r: 'p-meta', id: me.id, meta: { v: 1 }, prev: { v: 0 }, seq: 1 }),
    )

    expect((await lobby.getParticipant(me.id))!.meta).toEqual({ v: 2 })
  })

  it('a stale join echo never regresses meta that a later p-meta already advanced', async () => {
    const lobby = await Room.create('join-echo')
    const me = await lobby.join({ meta: { v: 0 } })
    await me.setMeta({ v: 1 }) // p-meta advances the meta past its join baseline

    // A late redelivery of the original join event (broker redelivery / echo) lands after the update.
    await getBroadcastAdapter().publish(
      roomCtrlKey('join-echo'),
      stringify({ __r: 'join', id: me.id, meta: { v: 0 }, joinedAt: 1 }),
    )
    await settle()

    expect((await lobby.getParticipant(me.id))!.meta).toEqual({ v: 1 }) // not regressed to the join meta
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
    const me = await lobby.join({ meta: { name: 'Alice' } })
    const observer = await Room.get('kick')
    const kicked: string[] = []
    observer.onLeave((m) => kicked.push(m.id))
    let meLeft = false
    me.onLeave(() => (meLeft = true))

    await Room.removeParticipant('kick', { id: me.id })

    expect(kicked).toEqual([me.id])
    expect(meLeft).toBe(true)
    expect(lobby.count).toBe(0)
    await expect(me.publish({ text: 'too late' })).rejects.toThrow('Participant has left')
    await expect(Room.removeParticipant('kick', { id: me.id })).rejects.toThrow('Participant not found')
  })

  it('every leave carries its cause — kick reasons travel with the removal, nothing to race', async () => {
    const lobby = await Room.create('causes')
    const observer = await Room.get('causes')
    const observed: Array<[string, unknown]> = []
    observer.onLeave((m, cause) => observed.push([String(m.meta.name), cause]))

    // Voluntary leave.
    const alice = await lobby.join({ meta: { name: 'Alice' } })
    const aliceCauses: unknown[] = []
    alice.onLeave((cause) => aliceCauses.push(cause))
    await alice.leave()
    expect(aliceCauses).toEqual([{ type: 'left' }])

    // Kick, with the reason riding the removal event itself.
    const bob = await lobby.join({ meta: { name: 'Bob' } })
    const bobCauses: unknown[] = []
    bob.onLeave((cause) => bobCauses.push(cause))
    await Room.removeParticipant('causes', { id: bob.id, reason: { rule: 'spam' } })
    expect(bobCauses).toEqual([{ type: 'removed', reason: { rule: 'spam' } }])

    expect(observed).toEqual([
      ['Alice', { type: 'left' }],
      ['Bob', { type: 'removed', reason: { rule: 'spam' } }],
    ])

    // Room closure.
    const carol = await lobby.join({ meta: { name: 'Carol' } })
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

    const me = await a.join({ meta: { name: 'Alice' } })

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

  it('getParticipants() on an unobserved instance resyncs from KV', async () => {
    const a = await Room.create('lazy')
    const me = await a.join({ meta: { name: 'Alice' } })

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

    const me = await a.join({ meta: { name: 'Alice', score: 0 } })
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

  it('setAttributes() merges per key and deletes on undefined — other keys survive', async () => {
    const a = await Room.create('attrs')
    const b = await Room.get('attrs')
    const me = await a.join({ meta: { name: 'Alice', score: 0, away: true } })

    await me.setAttributes({ score: 42 }) // only score changes; name & away untouched
    expect(me.meta).toEqual({ name: 'Alice', score: 42, away: true })
    expect((await b.getParticipant(me.id))!.meta).toEqual({ name: 'Alice', score: 42, away: true })

    await me.setAttributes({ away: undefined, score: 43 }) // delete a key while setting another
    expect(me.meta).toEqual({ name: 'Alice', score: 43 })
    expect((await b.getParticipant(me.id))!.meta).toEqual({ name: 'Alice', score: 43 })
  })

  it('onParticipantUpdate fires room-level for every member meta change, with the member and delta', async () => {
    const a = await Room.create('pmeta-delta')
    const b = await Room.get('pmeta-delta')
    const onA: Array<[string, unknown, unknown]> = []
    const onB: Array<[string, unknown, unknown]> = []
    a.onParticipantUpdate((m, meta, prev) => onA.push([m.id, meta, prev]))
    b.onParticipantUpdate((m, meta, prev) => onB.push([m.id, meta, prev])) // observe from another instance

    const me = await a.join({ meta: { name: 'A', v: 0 } })
    await me.setMeta({ name: 'A', v: 1 }) // full replace
    await me.setAttributes({ v: 2 }) // per-key merge
    await settle()

    const expected = [
      [me.id, { name: 'A', v: 1 }, { name: 'A', v: 0 }],
      [me.id, { name: 'A', v: 2 }, { name: 'A', v: 1 }],
    ]
    expect(onA).toEqual(expected) // the origin sees each change with its delta…
    expect(onB).toEqual(expected) // …and so does a remote observer, from the p-meta events
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Hidden participants — `join({ hidden: true })`: off-presence members (a server
// authority, a bot, a recorder), any number, addressable like any member.
// ───────────────────────────────────────────────────────────────────────────

describe('hidden participants', () => {
  it('are excluded from presence but reachable and addressable', async () => {
    const room = await Room.create('hidden')
    const joins: string[] = []
    room.onJoin((m) => joins.push(m.id))

    const authority = await room.join({ meta: { role: 'authority' }, hidden: true })
    const p1 = await room.join({ meta: { name: 'P1' } })
    const p2 = await room.join({ meta: { name: 'P2' } })

    // Excluded from presence: the two players are counted; the hidden one isn't.
    expect(room.count).toBe(2)
    expect((await room.getParticipants()).map((m) => m.id).sort()).toEqual([p1.id, p2.id].sort())
    expect(
      room
        .snapshot()
        .participants.map((p) => p.id)
        .sort(),
    ).toEqual([p1.id, p2.id].sort())
    expect(joins).toEqual([p1.id, p2.id]) // onJoin never fired for the hidden one

    // But it's a first-class, addressable member.
    expect((await room.getParticipants({ hidden: true })).map((p) => p.id)).toEqual([authority.id])
    expect((await room.getParticipant(authority.id))!.id).toBe(authority.id)
  })

  it('any number of hidden participants coexist', async () => {
    const room = await Room.create('hidden-many')
    const a = await room.join({ meta: { role: 'authority' }, hidden: true })
    const b = await room.join({ meta: { role: 'recorder' }, hidden: true })
    await room.join({ meta: { name: 'P1' } })

    expect(room.count).toBe(1) // only the player is present
    expect((await room.getParticipants({ hidden: true })).map((p) => p.id).sort()).toEqual([a.id, b.id].sort())
  })

  it('room.onEmpty fires when the last real player leaves — hidden participants linger', async () => {
    const room = await Room.create('hidden-empty')
    const events: string[] = []
    room.onLeave((m) => events.push(`leave:${m.id}`))
    room.onEmpty(() => events.push('empty'))

    await room.join({ hidden: true })
    const player = await room.join({ meta: { name: 'solo' } })
    expect(room.count).toBe(1) // the hidden one doesn't count

    await player.leave()
    expect(events).toEqual([`leave:${player.id}`, 'empty']) // onEmpty despite the hidden one lingering
    expect(room.count).toBe(0)
    expect((await room.getParticipants({ hidden: true })).length).toBe(1) // it outlives the players
  })

  it('is discovered cross-instance via the roster, still excluded from presence', async () => {
    const a = await Room.create('hidden-x')
    const authority = await a.join({ hidden: true })
    await a.join({ meta: { name: 'P1' } })

    const b = await Room.get('hidden-x')
    const participants = await b.getParticipants() // materializes the roster from KV
    expect(participants.map((m) => m.meta)).toEqual([{ name: 'P1' }]) // hidden excluded
    expect(b.count).toBe(1)
    expect((await b.getParticipants({ hidden: true })).map((p) => p.id)).toEqual([authority.id]) // reachable on the sibling
  })

  it('the lazy seed count (Room.get / Room.list) excludes hidden members before any roster loads', async () => {
    const a = await Room.create('hidden-seed')
    await a.join({ hidden: true })
    await a.join({ hidden: true })
    await a.join({ meta: { name: 'P1' } })

    // Room.get seeds `count` from KV with no per-member reads — the hidden-marker index keeps the
    // two off-presence members out of the seed, so the count is right the instant it resolves, not
    // only after a roster reconcile heals it.
    expect((await Room.get('hidden-seed')).count).toBe(1)

    // Room.list has no roster to heal it, so its count/isEmpty must be presence-only at the source.
    const info = (await Room.list()).find((r) => r.id === 'hidden-seed')!
    expect(info).toMatchObject({ count: 1, isEmpty: false })

    // A room holding only hidden members reads as empty.
    const botOnly = await Room.create('bot-only')
    await botOnly.join({ hidden: true })
    expect((await Room.list()).find((r) => r.id === 'bot-only')!).toMatchObject({ count: 0, isEmpty: true })
    expect((await Room.get('bot-only')).count).toBe(0)
  })

  it('an already-connected observer learns of a hidden join live (announced on the control lane)', async () => {
    const a = await Room.create('hidden-live')
    const b = await Room.get('hidden-live')
    let changes = 0
    const joins: string[] = []
    b.onChange(() => changes++)
    b.onJoin((m) => joins.push(m.id))
    await b.getParticipants() // b is a live observer with a loaded roster
    expect((await b.getParticipants({ hidden: true })).length).toBe(0) // none yet

    const hidden = await a.join({ meta: { role: 'authority' }, hidden: true }) // joined AFTER b connected

    expect((await b.getParticipants({ hidden: true })).map((p) => p.id)).toEqual([hidden.id]) // learned live
    expect(b.count).toBe(0) // still not counted
    expect(joins).toEqual([]) // not narrated as a participant join
    expect(changes).toBeGreaterThan(0) // but onChange fired (the roster changed)

    await hidden.leave()
    expect((await b.getParticipants({ hidden: true })).length).toBe(0) // symmetric: departure reaches observers
  })

  it('sends and receives DMs like any member', async () => {
    const room = await Room.create('hidden-dm')
    const authority = await room.join({ hidden: true })
    const player = await room.join({ meta: { name: 'P1' } })
    const toAuthority: unknown[] = []
    const toPlayer: unknown[] = []
    authority.listen((data) => toAuthority.push(data))
    player.listen((data) => toPlayer.push(data))

    await player.send(authority.id, { cmd: 'move' }) // address it by id
    await authority.send(player.id, { ack: true })

    expect(toAuthority).toEqual([{ cmd: 'move' }])
    expect(toPlayer).toEqual([{ ack: true }])
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

    const me = await a.join({ meta: { name: 'Alice' } })
    const ack = await me.publish({ text: 'hello' })

    expect(received).toEqual([{ data: { text: 'hello' }, from: { name: 'Alice' }, key: 'chat', seq: ack.seq }])
    expect(ack.key).toBe('chat')
    expect(typeof ack.timestamp).toBe('number')
  })

  it("per-member subscribe receives only that member's messages", async () => {
    const lobby = await Room.create('duo')
    const alice = await lobby.join({ meta: { name: 'Alice' } })
    const bob = await lobby.join({ meta: { name: 'Bob' } })

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

    const chatty = await a.join({ meta: { name: 'chatty' } })
    const muted = await a.join({ meta: { name: 'muted' }, selfDelivery: false })
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

    const me = await room.join({ meta: { name: 'Alice' } })
    await me.publish('hello')
    await Room.announce('lanes', 'notice')

    expect(ctrlTraffic).toEqual(['join', 'announce'])
    expect(textTraffic).toEqual(['data'])
  })

  it('binary rides per-publisher keys in shared mode too — upstream subscriptions are member-selective', async () => {
    const a = await Room.create('per-pub')
    const cam1 = await a.join({ meta: { name: 'cam1' } })
    const cam2 = await a.join({ meta: { name: 'cam2' } })
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
      stringify({ __r: 'data', from: ghost, fromMeta: { name: 'Zoe' }, data: 'first!', ord: { seq: 0, timestamp: 1 } }),
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
    const ghostly = await a.join({ meta: { name: 'Casper' } })
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
  it("subscribes only the wanted members' binary upstream keys", async () => {
    const a = await Room.create('sel')
    const cam1 = await a.join({ meta: { name: 'cam1' } })
    const cam2 = await a.join({ meta: { name: 'cam2' } })
    const b = await Room.get('sel')
    await b.getParticipants() // materialize the lazy roster

    const frames: string[] = []
    ;(await b.getParticipant(cam1.id))!.subscribeBinary((data) => frames.push(`cam1:${data[0]}`))
    await cam1.publishBinary(new Uint8Array([1]))
    await cam2.publishBinary(new Uint8Array([2])) // nobody wants cam2 — b never subscribed its key

    expect(frames).toEqual(['cam1:1'])
  })

  it('named tracks multiplex one member lane — filters, per-frame meta, zero-cost default', async () => {
    const a = await Room.create('media')
    const cam = await a.join({ meta: { name: 'Cam' } })
    const b = await Room.get('media')
    await b.getParticipants() // materialize the lazy roster

    const all: Array<[string | null, Record<string, unknown> | null, number]> = []
    const mics: number[] = []
    const remote = (await b.getParticipant(cam.id))!
    remote.subscribeBinary((data, info) => all.push([info.track, info.meta, data[0]!]))
    remote.subscribeBinary((data) => mics.push(data[0]!), { track: 'mic' })

    await cam.publishBinary(new Uint8Array([1])) // default track
    await cam.publishBinary(new Uint8Array([2]), { track: 'mic' })
    await cam.publishBinary(new Uint8Array([3]), { track: 'camera', meta: { key: true } })

    expect(all).toEqual([
      [null, null, 1],
      ['mic', null, 2],
      ['camera', { key: true }, 3],
    ])
    expect(mics).toEqual([2]) // the filter saw only its track

    // Room-level subscription filters the same way, with the verified sender.
    const cams: Array<[number, string]> = []
    b.subscribeBinary((data, _info, from) => cams.push([data[0]!, String(from.meta.name)]), { track: 'camera' })
    await cam.publishBinary(new Uint8Array([9]), { track: 'camera' })
    expect(cams).toEqual([[9, 'Cam']])
  })

  it('track framing round-trips exactly — default publishes cost one flag byte', () => {
    const id = crypto.randomUUID()
    const plain = frameWithMemberId(id, new Uint8Array([7, 8]))
    expect(plain.byteLength).toBe(16 + 1 + 2) // member + flags + payload
    expect(unframeMemberId(plain)).toMatchObject({ from: id, track: null, meta: null })
    expect([...unframeMemberId(plain)!.payload]).toEqual([7, 8])

    const tracked = frameWithMemberId(id, new Uint8Array([9]), { track: 'screen', meta: { key: true, ts: 42 } })
    const out = unframeMemberId(tracked)!
    expect(out.track).toBe('screen')
    expect(out.meta).toEqual({ key: true, ts: 42 })
    expect([...out.payload]).toEqual([9])
  })

  it('rejects a hostile binary frame: unknown flag bits and non-object meta', () => {
    const id = crypto.randomUUID()
    // An unknown flag bit → reject the whole frame (malformed or forward-incompatible).
    const badFlags = frameWithMemberId(id, new Uint8Array([1, 2, 3]))
    badFlags[16] = 0x80
    expect(unframeMemberId(badFlags)).toBeNull()

    // A scalar (non-object) meta is rejected — the framer never emits one, so it can only be hand-crafted.
    const uuidBytes = frameWithMemberId(id, new Uint8Array([9])).subarray(0, 16)
    const scalarMeta = new TextEncoder().encode('5')
    const hostile = new Uint8Array(16 + 1 + 2 + scalarMeta.length + 1)
    hostile.set(uuidBytes, 0)
    hostile[16] = 0x01 // FRAME_FLAG_META
    hostile[18] = scalarMeta.length // 2-byte length (high byte 0)
    hostile.set(scalarMeta, 19)
    hostile[19 + scalarMeta.length] = 9 // payload
    expect(unframeMemberId(hostile)).toBeNull()
  })

  it('bounds a binary want track by UTF-8 bytes, not JS chars', () => {
    const wide = '中'.repeat(30) // 30 UTF-16 units, 90 UTF-8 bytes — passes a char count, fails the byte cap
    expect(wide.length).toBeLessThanOrEqual(64)
    expect(sanitizeBinaryWants({ everyMember: { all: false, tracks: [wide] }, members: {} })).toBeNull()
    expect(sanitizeBinaryWants({ everyMember: { all: false, tracks: ['camera'] }, members: {} })).not.toBeNull()
  })

  it('bounds the tail hold by total size, not just count — and drops a single oversize entry', () => {
    const hold: Array<{ serialized: string; ord: { seq: number; timestamp: number }; from: string }> = []
    const chunk = 'x'.repeat(300_000) // the 1 MiB size budget binds well before the 256 count would
    for (let i = 0; i < 20; i++) pushBoundedTail(hold, { serialized: chunk, ord: { seq: i, timestamp: 1 }, from: 'a' })
    expect(hold.reduce((n, e) => n + e.serialized.length, 0)).toBeLessThanOrEqual(ROOM_TAIL_HOLD_BYTES_MAX)
    expect(hold.length).toBeLessThan(20) // the size cap evicted before the count cap could

    const len = hold.length
    pushBoundedTail(hold, {
      serialized: 'y'.repeat(ROOM_TAIL_HOLD_BYTES_MAX + 1),
      ord: { seq: 99, timestamp: 2 },
      from: 'a',
    })
    expect(hold.some((e) => e.serialized[0] === 'y')).toBe(false) // a single over-budget entry is never held
    expect(hold.length).toBeLessThanOrEqual(len)
  })

  it('encodes room IDs so a delimiter in one ID cannot collide with or sweep another room', () => {
    // Raw interpolation aliased roomCtrlKey('a:t') onto roomTextKey('a'), and let a `:rb:` close-sweep for
    // room 'A' reach a room named 'A:rb:X'. IDs may legitimately contain ':' (the docs' 'team:red'), so
    // encoding makes every ID one opaque, delimiter-free segment.
    expect(roomCtrlKey('alpha:t')).not.toBe(roomTextKey('alpha'))
    expect(roomConfigKvKey('A:rb:X').startsWith(roomRetainedBinaryPrefix('A'))).toBe(false)
    expect(roomCtrlKey('team:red')).not.toBe(roomCtrlKey('team')) // the documented nested ID stays distinct
  })

  it('bounds the room ID length', async () => {
    await expect(Room.create('x'.repeat(ROOM_ID_MAX_BYTES + 1))).rejects.toThrow(/at most/)
    await expect(Room.create('x'.repeat(ROOM_ID_MAX_BYTES))).resolves.toBeDefined() // the cap itself is allowed
  })

  it('a member-scoped listener still brings up the room text lane', async () => {
    const a = await Room.create('sel-shared')
    const alice = await a.join({ meta: { name: 'alice' } })
    const bob = await a.join({ meta: { name: 'bob' } })
    const b = await Room.get('sel-shared')
    await b.getParticipants() // materialize the lazy roster

    const heard: unknown[] = []
    ;(await b.getParticipant(alice.id))!.subscribe((data) => heard.push(data))
    await alice.publish('a1')
    await bob.publish('b1') // one shared key — it reaches the node; the view filters it out
    expect(heard).toEqual(['a1'])
  })

  it("releases a departed member's listeners — the decoder pattern must not pin subscriptions", async () => {
    const a = await Room.create('release')
    const cam = await a.join({ meta: { name: 'cam' } })
    const b = await Room.get('release')
    await b.getParticipants() // materialize the lazy roster

    // The documented pattern: subscribe on join, close the decoder on leave — no unsubscribe.
    ;(await b.getParticipant(cam.id))!.subscribeBinary(() => {})
    expect(await b.getParticipant(cam.id)).not.toBe(null)

    await cam.leave()

    const bState = (b as ServerRoom)._state
    expect(bState.binaryWants()).toEqual({ everyMember: { all: false, tracks: [] }, members: {} })
    expect(bState.binaryListenerCount).toBe(0)
  })

  it("the screen-share flow: unsubscribing a track stops its bytes at the source — the publisher's ack says so", async () => {
    const a = await Room.create('share')
    const sharer = await a.join({ meta: { name: 'Sharer' }, selfDelivery: false })
    const viewer = await Room.get('share')
    await viewer.getParticipants()

    // Nobody watches yet: the frame reaches no subscription anywhere — pause-the-encoder signal.
    expect((await sharer.publishBinary(new Uint8Array([1]), { track: 'screen' })).receivers).toBe(0)

    // A viewer starts watching (named subscribers attach eagerly — no frame is ever missed) …
    const seen: number[] = []
    const stopWatching = viewer.subscribeBinary((data) => seen.push(data[0]!), { track: 'screen' })
    expect((await sharer.publishBinary(new Uint8Array([2]), { track: 'screen' })).receivers).toBe(1)
    expect(seen).toEqual([2])

    // … keeps the call audio while dropping the screen: mic flows, screen bytes stop upstream.
    const mics: number[] = []
    viewer.subscribeBinary((data) => mics.push(data[0]!), { track: 'mic' })
    stopWatching()
    expect((await sharer.publishBinary(new Uint8Array([3]), { track: 'screen' })).receivers).toBe(0)
    expect((await sharer.publishBinary(new Uint8Array([4]), { track: 'mic' })).receivers).toBe(1)
    expect(seen).toEqual([2])
    expect(mics).toEqual([4])
  })

  it('named tracks ride per-(member, track) keys; the default lane stays on the member key', async () => {
    const a = await Room.create('track-keys')
    const cam = await a.join({ meta: { name: 'Cam' } })
    const b = await Room.get('track-keys')
    b.subscribeBinary(() => {}) // all tracks — forces the upstream keys up as tracks appear

    const published = vi.spyOn(getBroadcastAdapter(), 'publishBinary')
    await cam.publishBinary(new Uint8Array([1]))
    await cam.publishBinary(new Uint8Array([2]), { track: 'camera' })
    expect(published.mock.calls.map(([key]) => key)).toEqual([
      roomMemberDataKey('track-keys', cam.id),
      roomMemberTrackKey('track-keys', cam.id, 'camera'),
    ])

    // The KV record now names the track — late observers can subscribe streams they can't name.
    const record = parse((await getBroadcastAdapter().get!(roomMemberKvKey('track-keys', cam.id)))!)
    expect((record as RoomMemberRecord).tracks).toEqual(['camera'])
  })

  it('an all-track observer arriving after the track exists discovers it from the roster', async () => {
    const a = await Room.create('discover')
    const cam = await a.join({ meta: { name: 'Cam' } })
    await cam.publishBinary(new Uint8Array([1]), { track: 'screen' }) // track born before any observer

    const late = await Room.get('discover')
    const frames: Array<[string | null, number]> = []
    late.subscribeBinary((data, info) => frames.push([info.track, data[0]!]))
    await settle() // roster load (KV) brings the track key subscription up

    await cam.publishBinary(new Uint8Array([2]), { track: 'screen' })
    expect(frames).toEqual([['screen', 2]])
  })

  it('`track: null` selects the default lane only — named tracks never reach it', async () => {
    const a = await Room.create('default-only')
    const cam = await a.join({ meta: { name: 'Cam' } })
    const b = await Room.get('default-only')
    await b.getParticipants()

    const defaults: number[] = []
    b.subscribeBinary((data) => defaults.push(data[0]!), { track: null })
    await cam.publishBinary(new Uint8Array([1]))
    await cam.publishBinary(new Uint8Array([2]), { track: 'screen' })
    expect(defaults).toEqual([1])
    // And the narrowing holds upstream: with only the default lane wanted, the named frame
    // found no subscriber at all.
    expect((await cam.publishBinary(new Uint8Array([3]), { track: 'screen' })).receivers).toBe(0)
    expect((await cam.publishBinary(new Uint8Array([4]))).receivers).toBe(1)
  })

  it('caps the named tracks one participant can announce, sparing the default lane', async () => {
    const room = await Room.create('track-cap')
    const cam = await room.join({ meta: { name: 'Cam' } })
    for (let i = 0; i < ROOM_TRACKS_PER_MEMBER_MAX; i++) {
      await cam.publishBinary(new Uint8Array([i]), { track: `t${i}` })
    }
    // A new distinct track past the cap is rejected...
    await expect(cam.publishBinary(new Uint8Array([1]), { track: 'overflow' })).rejects.toThrow(/at most .*tracks/)
    // ...while an already-announced track and the unnamed default lane keep working.
    await expect(cam.publishBinary(new Uint8Array([1]), { track: 't0' })).resolves.toBeDefined()
    await expect(cam.publishBinary(new Uint8Array([1]))).resolves.toBeDefined()
  })

  it('unframeMemberId rejects a malformed binary frame with null instead of throwing', () => {
    const id = crypto.randomUUID()
    expect(
      unframeMemberId(frameWithMemberId(id, new Uint8Array([1, 2]), { track: 'cam', meta: { k: 1 } })),
    ).toMatchObject({ from: id, track: 'cam', meta: { k: 1 } })
    expect(unframeMemberId(new Uint8Array(10))).toBeNull() // shorter than the fixed sender+flags prefix

    // Layout: [16-byte id][1-byte flags][?1-byte track length + track][?2-byte meta length + meta][payload].
    const badTrack = frameWithMemberId(id, new Uint8Array([1]), { track: 'cam' })
    badTrack[17] = 200 // the track-length byte — 200 > TRACK_MAX_BYTES, so the frame is over its bound
    expect(unframeMemberId(badTrack)).toBeNull()

    const badMeta = frameWithMemberId(id, new Uint8Array([1]), { meta: { k: 1 } })
    badMeta[19] = 0x78 // clobber the opening `{` of the meta JSON (offset 16+1+2) so parse would throw
    expect(unframeMemberId(badMeta)).toBeNull()
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
    const alice = await a.join({ meta: { name: 'Alice' } })
    const bob = await b.join({ meta: { name: 'Bob' } })
    const bobInbox: unknown[] = []
    const aliceInbox: unknown[] = []
    const roomStream: unknown[] = []
    bob.listen((data, from) => bobInbox.push([data, from?.id, from?.meta]))
    alice.listen((data) => aliceInbox.push(data))
    a.subscribe((data) => roomStream.push(data))
    b.subscribe((data) => roomStream.push(data))

    const receipt = await alice.send((await a.getParticipant(bob.id))!, 'psst') // target as object — or pass the ID

    expect(bobInbox).toEqual([['psst', alice.id, { name: 'Alice' }]]) // live RemoteParticipant sender
    expect(aliceInbox).toEqual([]) // not echoed to the sender
    expect(roomStream).toEqual([]) // never on the room stream
    // send() resolves with the delivery receipt — the sequenced hand-off, not a fire-and-forget.
    expect(typeof receipt.seq).toBe('number')
    expect(typeof receipt.timestamp).toBe('number')
  })

  it('rejects unknown targets and departed senders', async () => {
    const lobby = await Room.create('dm-err')
    const alice = await lobby.join()
    await expect(alice.send(crypto.randomUUID(), 'x')).rejects.toThrow('Participant not found')

    const bob = await lobby.join()
    await alice.leave()
    await expect(alice.send(bob.id, 'x')).rejects.toThrow('Participant has left')
  })

  it("a reactive DM beating the target's listen() is held and flushed — never dropped", async () => {
    const room = await Room.create('reactive')
    const greeter = await room.join({ meta: { name: 'Bot' } })
    // The bot greets on join — before the joiner had any chance to attach listen().
    room.onJoin((member) => void greeter.send(member.id, 'welcome!').catch(() => {}))

    const newcomer = await (await Room.get('reactive')).join({ meta: { name: 'New' } })
    await settle() // let the greet round-trip land — nobody is listening yet

    const inbox: unknown[] = []
    newcomer.listen((data, from) => inbox.push([data, from?.meta.name]))
    expect(inbox).toEqual([['welcome!', 'Bot']]) // flushed on attach
  })

  it('the pre-listen hold is bounded (drop-oldest) and released on leave', async () => {
    const room = await Room.create('hold-cap')
    const sender = await room.join({ meta: { name: 'S' } })
    const target = await (await Room.get('hold-cap')).join({ meta: { name: 'T' } })
    for (let i = 0; i < 70; i++) await sender.send(target.id, i)

    const inbox: number[] = []
    target.listen((data) => inbox.push(data as number))
    expect(inbox.length).toBe(64) // capped
    expect(inbox[0]).toBe(6) // oldest dropped first
    expect(inbox[63]).toBe(69)

    // Held messages die with the membership.
    const gone = await (await Room.get('hold-cap')).join({ meta: { name: 'G' } })
    await sender.send(gone.id, 'never-read')
    await gone.leave()
    const late: unknown[] = []
    gone.listen((data) => late.push(data))
    expect(late).toEqual([])
  })

  it('identity is stamped at server-side join and travels everywhere, spoof-proof', async () => {
    const a = await Room.create('who')
    const alice = await a.join({ meta: { name: 'Alice' }, identity: 'user-1' })
    expect(alice.identity).toBe('user-1')

    // Roster: another instance sees the identity.
    const b = await Room.get('who')
    expect((await b.getParticipant(alice.id))?.identity).toBe('user-1')

    // Messages: the envelope carries the server-stamped identity — even to a view that
    // doesn't know the sender yet (snapshot sender).
    const c = await Room.get('who')
    const senders: Array<[string | null, string | null]> = []
    c.subscribe((_data, _info, from) => senders.push([String(from.meta.name), from.identity]))
    await alice.publish('hi')
    expect(senders).toEqual([['Alice', 'user-1']])

    // DMs: the inbox sender carries it too.
    const bob = await a.join({ meta: { name: 'Bob' } })
    expect(bob.identity).toBe(null) // identity is opt-in
    const inbox: Array<string | null> = []
    bob.listen((_data, from) => inbox.push(from?.identity ?? null))
    await alice.send(bob.id, 'psst')
    expect(inbox).toEqual(['user-1'])

    // Guards see it — bans by identity are one comparison.
    const guarded = await Room.get('who')
    const guardSaw: Array<string | null> = []
    Room.guard(guarded, { onBeforeJoin: (member) => void guardSaw.push(member.identity) })
    await guarded.join({ meta: { name: 'Tab2' }, identity: 'user-1' })
    expect(guardSaw).toEqual(['user-1'])
  })

  it('a client-side join cannot claim an identity — it is assigned where trust lives', async () => {
    await Room.create('trusted')
    const served = (await Room.get('trusted')) as ServerRoom
    const stub = new RoomStubChannel(served)
    stub._registerChannel()
    served._attachStub(stub)
    // The wire request has no identity field at all; a crafted one is simply never read.
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: {}, identity: 'admin' }), 1)
    const memberId = [...stub._stubMembers.keys()][0]!
    expect((await served.getParticipant(memberId))?.identity).toBe(null)
  })

  it('removeParticipant({ identity }) sweeps every membership of that identity, with the reason', async () => {
    const room = await Room.create('sweep')
    const tab1 = await room.join({ meta: { name: 'T1' }, identity: 'user-9' })
    const tab2 = await room.join({ meta: { name: 'T2' }, identity: 'user-9' })
    const other = await room.join({ meta: { name: 'Other' }, identity: 'user-8' })
    const causes: unknown[] = []
    tab1.onLeave((cause) => causes.push(cause))
    tab2.onLeave((cause) => causes.push(cause))

    await Room.removeParticipant('sweep', { identity: 'user-9', reason: 'banned' })

    expect(causes).toEqual([
      { type: 'removed', reason: 'banned' },
      { type: 'removed', reason: 'banned' },
    ])
    expect(room.count).toBe(1)
    expect((await room.getParticipant(other.id))?.identity).toBe('user-8')

    // Idempotent: sweeping an identity with no memberships is a no-op, not an error.
    await Room.removeParticipant('sweep', { identity: 'user-9' })
  })

  it('Room.send({ identity }) fans a server DM to every membership of that identity; none is a no-op', async () => {
    const room = await Room.create('id-send')
    const tab1 = await room.join({ meta: { name: 'T1' }, identity: 'user-7' })
    const tab2 = await room.join({ meta: { name: 'T2' }, identity: 'user-7' })
    const other = await room.join({ meta: { name: 'Other' }, identity: 'user-6' })
    const got1: unknown[] = []
    const got2: unknown[] = []
    const gotOther: unknown[] = []
    tab1.listen((data, from) => got1.push([data, from]))
    tab2.listen((data, from) => got2.push([data, from]))
    other.listen((data, from) => gotOther.push([data, from]))

    await Room.send('id-send', { identity: 'user-7' }, { ping: 1 })
    await settle()

    expect(got1).toEqual([[{ ping: 1 }, null]]) // server-authored → from: null
    expect(got2).toEqual([[{ ping: 1 }, null]]) // both of the identity's tabs
    expect(gotOther).toEqual([]) // a different identity is untouched

    // Sending to an identity with no live membership is a no-op, not an error (e.g. a signed-out user).
    await Room.send('id-send', { identity: 'nobody' }, { ping: 2 })
  })

  it('the identity index is a hint: a stale marker resolves to nothing and is pruned on read', async () => {
    const kv = getBroadcastAdapter()
    const room = await Room.create('id-heal')
    const tab1 = await room.join({ meta: { name: 'T1' }, identity: 'user-5' })
    const ghost = await room.join({ meta: { name: 'Ghost' }, identity: 'user-5' })

    // Simulate the ghost's record vanishing (a reap or a crash mid-leave) with its marker lingering.
    await kv.delete(roomMemberKvKey('id-heal', ghost.id))
    const got1: unknown[] = []
    const gotGhost: unknown[] = []
    tab1.listen((data) => got1.push(data))
    ghost.listen((data) => gotGhost.push(data))

    await Room.send('id-heal', { identity: 'user-5' }, 'hi')
    await settle()

    expect(got1).toEqual(['hi']) // the live membership still receives
    expect(gotGhost).toEqual([]) // the ghost has no record — filtered out, no phantom delivery
    // …and resolving pruned the ghost's stale marker, leaving only the live one.
    expect(await kv.keys(roomIdentityKvPrefix('id-heal', 'user-5'))).toEqual([
      roomIdentityMemberKvKey('id-heal', 'user-5', tab1.id),
    ])
  })

  it("Room.getParticipants({ identity }) reads one identity's memberships as snapshots, without loading the roster", async () => {
    const room = await Room.create('id-read')
    const tab1 = await room.join({ meta: { name: 'T1', dnd: true }, identity: 'user-4' })
    const tab2 = await room.join({ meta: { name: 'T2', dnd: true }, identity: 'user-4' })
    await room.join({ meta: { name: 'Other' }, identity: 'user-3' })

    const mine = await Room.getParticipants('id-read', { identity: 'user-4' })
    expect(mine.map((p) => p.id).sort()).toEqual([tab1.id, tab2.id].sort()) // both of the identity's tabs
    const t1 = mine.find((p) => p.id === tab1.id)!
    expect(t1.identity).toBe('user-4')
    expect(t1.meta).toEqual({ name: 'T1', dnd: true }) // a DND/presence check reads meta directly
    expect(typeof t1.joinedAt).toBe('number')
    expect(Object.keys(t1).sort()).toEqual(['id', 'identity', 'joinedAt', 'meta']) // a plain snapshot, not a live handle

    expect((await Room.getParticipants('id-read')).length).toBe(3) // whole roster (no target)
    expect(await Room.getParticipants('id-read', { identity: 'nobody' })).toEqual([]) // signed-out user → [], not an error
  })

  it('Room.guard({ onBeforeSend }) guards sends: rejections reach the sender, the guard sees rich identities', async () => {
    await Room.create('guarded')
    const lobby = await Room.get('guarded')
    Room.guard(lobby, {
      onBeforeSend: (from, to, data) => {
        if (data === 'blocked') throw new Error('not friends')
        expect(from.meta).toEqual({ name: 'Alice' }) // resolved sender, meta included
        expect(to.meta).toEqual({ name: 'Bob' }) // resolved target, meta included
      },
    })
    const bob = await lobby.join({ meta: { name: 'Bob' } })
    const inbox: unknown[] = []
    bob.listen((data, from) => inbox.push([data, from?.id, from?.meta]))
    const alice = await lobby.join({ meta: { name: 'Alice' } })

    await expect(alice.send(bob.id, 'blocked')).rejects.toThrow('not friends')
    await alice.send(bob.id, 'hi')

    expect(inbox).toEqual([['hi', alice.id, { name: 'Alice' }]])
  })

  it('Room.guard({ onBeforeSend }) guards client-side joins made through that instance', async () => {
    await Room.create('gated')
    const served = (await Room.get('gated')) as ServerRoom
    Room.guard(served, {
      onBeforeSend: (from, _to, data) => {
        if (data === 'blocked') throw new Error(`no messages from ${from.meta.name}`)
      },
    })
    const stub = new RoomStubChannel(served)
    stub._registerChannel()
    served._attachStub(stub)
    const target = await served.join({ meta: { name: 'T' } })
    const inbox: unknown[] = []
    target.listen((data) => inbox.push(data))
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: { name: 'C' } }), 1)

    const memberId = [...stub._stubMembers.keys()][0]!
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-dm', id: memberId, to: target.id, data: 'blocked' }), 2)
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-dm', id: memberId, to: target.id, data: 'hi' }), 3)

    expect(inbox).toEqual(['hi']) // the guarded send never delivered
  })

  it('Room.guard({ onBeforePublish }) gates room-wide messages — text and binary, server and client joins', async () => {
    await Room.create('moderated')
    const served = (await Room.get('moderated')) as ServerRoom
    Room.guard(served, {
      onBeforePublish: (from, data) => {
        if (data === 'slur' || (data instanceof Uint8Array && data[0] === 0xff)) {
          throw new Error(`blocked: ${from.meta.name}`)
        }
      },
    })
    const observer = await Room.get('moderated')
    const seen: unknown[] = []
    observer.subscribe((data) => seen.push(data))
    const me = await served.join({ meta: { name: 'Mallory' } })

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

  it('Room.guard({ onBeforeJoin }) gates admission — server and client joins, a rejected join writes nothing', async () => {
    await Room.create('door')
    const served = (await Room.get('door')) as ServerRoom
    const seen: { id: string; meta: Record<string, unknown> }[] = []
    Room.guard(served, {
      onBeforeJoin: (member) => {
        seen.push(member)
        if (member.meta.name === 'Banned') throw new Error(`no entry for ${member.meta.name}`)
      },
    })

    await expect(served.join({ meta: { name: 'Banned' } })).rejects.toThrow('no entry for Banned')
    // The guard runs before any state is written — a rejected join leaves no trace.
    expect(served.count).toBe(0)
    expect(await (await Room.get('door')).getParticipants()).toEqual([])

    const alice = await served.join({ meta: { name: 'Alice' } })
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
      onBeforeJoin: () => {
        throw new Error('nobody enters')
      },
    })
    await expect(granted.join()).rejects.toThrow('nobody enters')
    const me = await Room.join('velvet', { meta: { name: 'Direct' } }) // no grant involved — like Room.announce() vs onBeforePublish
    expect(me.meta).toEqual({ name: 'Direct' })
  })

  it('Room.guard() is one-shot and validates its arguments', async () => {
    await Room.create('strict')
    const room = await Room.get('strict')
    Room.guard(room, { onBeforeSend: () => {} })
    expect(() => Room.guard(room, { onBeforeSend: () => {} })).toThrow('already called')
    // @ts-expect-error — runtime validation
    expect(() => Room.guard(room, { onBeforePublish: 'nope' })).toThrow('should be a function')
    // @ts-expect-error — runtime validation
    expect(() => Room.guard(room, { onBeforeJoin: 'nope' })).toThrow('should be a function')
    expect(() => Room.guard({} as never, {})).toThrow('expects a room')
  })

  it('Room.guard() after-hooks fire post-commit with the authoritative receipt', async () => {
    await Room.create('after')
    const room = await Room.get('after')
    const joins: Array<{ id: string; name: unknown; joinedAt: unknown }> = []
    const published: Array<{ data: unknown; seq: number; timestamp: unknown; receivers: unknown }> = []
    const sent: Array<{ to: string; data: unknown; seq: unknown }> = []
    Room.guard(room, {
      onAfterJoin: (member, info) =>
        void joins.push({ id: member.id, name: member.meta.name, joinedAt: info.joinedAt }),
      onAfterPublish: (_from, data, info) =>
        void published.push({ data, seq: info.seq, timestamp: info.timestamp, receivers: info.receivers }),
      onAfterSend: (_from, to, data, info) => void sent.push({ to: to.id, data, seq: info.seq }),
    })

    const alice = await room.join({ meta: { name: 'Alice' } })
    expect(joins).toHaveLength(1)
    expect(joins[0]).toMatchObject({ id: alice.id, name: 'Alice' })
    expect(typeof joins[0]!.joinedAt).toBe('number')

    const bob = await room.join({ meta: { name: 'Bob' } })
    await alice.publish({ text: 'one' })
    await alice.publish({ text: 'two' })
    expect(published.map((p) => p.data)).toEqual([{ text: 'one' }, { text: 'two' }])
    // The receipt carries the room's logical clock: the (timestamp, seq) pair strictly increases per
    // publish — the order you persist for history — even as seq resets when wall time advances.
    const [p0, p1] = [published[0]!, published[1]!]
    expect(p1.timestamp > p0.timestamp || (p1.timestamp === p0.timestamp && p1.seq > p0.seq)).toBe(true)
    expect(typeof p0.timestamp).toBe('number')
    expect(typeof p0.receivers).toBe('number')

    await alice.send(bob.id, 'hi bob')
    expect(sent).toHaveLength(1)
    expect(sent[0]).toMatchObject({ to: bob.id, data: 'hi bob' })
    expect(typeof sent[0]!.seq).toBe('number')
  })

  it('a throwing after-hook rejects the caller, but the message is already delivered', async () => {
    await Room.create('after-throw')
    const room = await Room.get('after-throw')
    Room.guard(room, {
      onAfterPublish: (_from, data) => {
        if (data === 'boom') throw new Error('persist failed')
      },
    })
    const observer = await Room.get('after-throw')
    const seen: unknown[] = []
    observer.subscribe((data) => seen.push(data))
    const me = await room.join({ meta: { name: 'A' } })

    await expect(me.publish('boom')).rejects.toThrow('persist failed')
    // onAfterPublish runs post-commit: the message was already broadcast before the hook threw.
    expect(seen).toEqual(['boom'])
  })

  it("delivers to a member the sender's stale local view doesn't know yet (KV fallback)", async () => {
    const a = await Room.create('dm-lag')
    const alice = await a.join({ meta: { name: 'Alice' } })
    const b = await Room.get('dm-lag') // snapshot: alice only
    const bob = await a.join({ meta: { name: 'Bob' } }) // b is unobserved — it missed this join
    const bobInbox: unknown[] = []
    bob.listen((data, from) => bobInbox.push([data, from?.id]))

    // b's local view lags; the KV member record is authoritative.
    await (b as ServerRoom)._sendDm(alice.id, bob.id, 'catch-up')

    expect(bobInbox).toEqual([['catch-up', alice.id]])
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Direct message acks — `send(to, data, { ack: true })` waits for the recipient
// to handle the message and resolves with its reply, rejecting on the handler's
// throw or the recipient's departure. Mirrors the channel `send({ ack: true })`.
// ───────────────────────────────────────────────────────────────────────────

describe('direct message acks', () => {
  it('resolves with the recipient handler’s reply (the request/response twin of channel ack)', async () => {
    const room = await Room.create('dm-ack')
    const authority = await room.join({ meta: { role: 'authority' } })
    const player = await room.join({ meta: { name: 'p' } })
    authority.listen((cmd) => `applied:${cmd}`)

    // A superset of the plain-send receipt: the reply plus the message's own seq/timestamp.
    const receipt = await player.send(authority.id, 'move e4', { ack: true })
    expect(receipt.response).toBe('applied:move e4')
    expect(typeof receipt.seq).toBe('number')
    expect(typeof receipt.timestamp).toBe('number')
  })

  it('carries a recipient handler’s Abort to the sender — value and identity intact', async () => {
    const room = await Room.create('dm-ack-abort')
    const authority = await room.join()
    const player = await room.join()
    authority.listen(() => {
      throw Abort({ code: 'ILLEGAL_MOVE', detail: 'e5' })
    })

    const err = await player.send(authority.id, 'x', { ack: true }).then(
      () => null,
      (e) => e,
    )
    expect(err instanceof Abort).toBe(true) // an Abort, exactly like a rejected telefunction
    expect(err.abortValue).toEqual({ code: 'ILLEGAL_MOVE', detail: 'e5' })
  })

  it('hides a recipient handler’s plain error (a bug) behind the generic message', async () => {
    const room = await Room.create('dm-ack-throw')
    const authority = await room.join()
    const player = await room.join()
    authority.listen(() => {
      throw new Error('secret server detail')
    })

    // Matches telefunc: a non-Abort throw is a bug — hidden from the caller, logged on the server.
    const err = await player.send(authority.id, 'x', { ack: true }).then(
      () => null,
      (e) => e,
    )
    expect(err instanceof Abort).toBe(false)
    expect(err.message).toBe('Internal Server Error — see server logs')
    expect(err.message).not.toContain('secret server detail')
  })

  it('replies with the last listener’s return, like a channel', async () => {
    const room = await Room.create('dm-ack-last')
    const a = await room.join()
    const b = await room.join()
    b.listen(() => 'first')
    b.listen(() => 'second')

    expect((await a.send(b.id, 'x', { ack: true })).response).toBe('second')
  })

  it('waits for a recipient that listens after the fact — no spurious failure on the pre-listen race', async () => {
    const room = await Room.create('dm-ack-hold')
    const authority = await room.join()
    const player = await room.join()

    const pending = player.send(authority.id, 'ping', { ack: true }) // no listener yet — held
    await settle()
    authority.listen((cmd) => `pong:${cmd}`) // attaches now → the held DM is handled

    expect((await pending).response).toBe('pong:ping')
  })

  it('rejects if the recipient leaves before handling', async () => {
    const room = await Room.create('dm-ack-leave')
    const authority = await room.join()
    const player = await room.join()

    const pending = player.send(authority.id, 'x', { ack: true }) // no listener → held
    await settle()
    await authority.leave()

    await expect(pending).rejects.toThrow(/left the room/)
  })

  it('rejects after a timeout when the recipient joined but never handles it (and never leaves)', async () => {
    vi.useFakeTimers()
    try {
      const room = await Room.create('dm-ack-timeout')
      const authority = await room.join()
      const player = await room.join()

      const pending = player.send(authority.id, 'x', { ack: true }) // authority never listens, never leaves
      const outcome = pending.then(
        () => 'resolved',
        (err: unknown) => (err as Error).message,
      )
      await vi.advanceTimersByTimeAsync(600_000) // well past the ack safety timeout

      expect(await outcome).toMatch(/timed out/)
    } finally {
      vi.useRealTimers()
    }
  })

  it('a plain send() (no ack) still resolves with a delivery receipt, ignoring any handler return', async () => {
    const room = await Room.create('dm-noack')
    const a = await room.join()
    const b = await room.join()
    b.listen(() => 'ignored')

    const receipt = await a.send(b.id, 'x')
    expect(typeof receipt.seq).toBe('number')
    expect(typeof receipt.timestamp).toBe('number')
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
    await a.join({ meta: { name: 'Alice' } })
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
    const alice = await lobby.join({ meta: { name: 'Alice' } })
    const bob = await lobby.join({ meta: { name: 'Bob' } })
    const aliceInbox: unknown[] = []
    const bobInbox: unknown[] = []
    alice.listen((data, from) => aliceInbox.push([data, from]))
    bob.listen((data) => bobInbox.push(data))

    await Room.send('automod', { id: alice.id }, { warning: 'watch the language' })

    expect(aliceInbox).toEqual([[{ warning: 'watch the language' }, null]]) // null = room-authored
    expect(bobInbox).toEqual([])
    await expect(Room.send('automod', { id: crypto.randomUUID() }, 'x')).rejects.toThrow('Participant not found')
  })
})

// ───────────────────────────────────────────────────────────────────────────
// One ordered semantic lane — participant text and Room.announce() draw one
// persisted, clamped (timestamp, seq) clock, so a room's semantic messages
// totally order. Bug class targeted: text and announce on separate per-key seq
// domains (uncomparable), and a clock that rewinds under skew or a recreation.
// ───────────────────────────────────────────────────────────────────────────

describe('one ordered semantic lane', () => {
  /** Lexicographic (timestamp, seq): is x strictly before y in the room's semantic order? */
  const before = (x: { timestamp: number; seq: number }, y: { timestamp: number; seq: number }) =>
    x.timestamp < y.timestamp || (x.timestamp === y.timestamp && x.seq < y.seq)

  it('participant text and Room.announce() draw one strictly-increasing, unique order', async () => {
    const room = await Room.create('order:shared')
    const alice = await room.join({ meta: { name: 'Alice' } })
    const a = await alice.publish('one')
    const b = await Room.announce('order:shared', 'notice') // an announcement, ordered against text
    const c = await alice.publish('two')
    expect(before(a, b)).toBe(true)
    expect(before(b, c)).toBe(true)
  })

  it('the clock resets seq on a time advance and clamps+increments on a repeat or regress', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    try {
      vi.setSystemTime(1000)
      const room = await Room.create('order:clock')
      const alice = await room.join({ meta: {} })
      const pair = async () => {
        const { timestamp, seq } = await alice.publish('x')
        return { timestamp, seq }
      }
      expect(await pair()).toEqual({ timestamp: 1000, seq: 1 }) // first: time advanced past 0 → reset to 1
      expect(await pair()).toEqual({ timestamp: 1000, seq: 2 }) // same ms → clamp, increment
      vi.setSystemTime(2000)
      expect(await pair()).toEqual({ timestamp: 2000, seq: 1 }) // advance → reset to 1
      vi.setSystemTime(1500) // wall clock jumps backward
      expect(await pair()).toEqual({ timestamp: 2000, seq: 2 }) // clamp to the watermark, increment
    } finally {
      vi.useRealTimers()
    }
  })

  it('a recreation resumes past the previous watermark even if the wall clock regressed', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    try {
      vi.setSystemTime(5000)
      const first = await Room.create('order:recreate')
      const old = await (await first.join({ meta: {} })).publish('old')
      expect(old).toMatchObject({ timestamp: 5000, seq: 1 })
      await Room.close('order:recreate')
      vi.setSystemTime(3000) // the clock jumps backward before the room is recreated
      const second = await Room.create('order:recreate')
      const fresh = await (await second.join({ meta: {} })).publish('new')
      // The watermark outlives the close, so the new incarnation clamps past it — never rewinding to
      // the regressed wall clock, so no consumer sees a newer message stamped older than an old one.
      expect(fresh).toMatchObject({ timestamp: 5000, seq: 2 })
    } finally {
      vi.useRealTimers()
    }
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
    // The join success rides an OK ack carrying the raw identity — no `{ ok: true }` envelope.
    return JSON.parse(ack.text) as { id: string; joinedAt: number }
  }

  it('req-join creates a member and acks its identity', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('served')
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-join', meta: { name: 'Remote' } }), 1)
    const ack = peer.decoded().find((f) => f.tag === TAG.ACK_RES && f.ackedSeq === 1)

    expect(ack.status).toBe(ACK_STATUS.OK)
    const { id } = JSON.parse(ack.text) as { id: string; joinedAt: number }
    expect(typeof id).toBe('string')
    expect(serverRoom.count).toBe(1)
    expect((await serverRoom.getParticipant(id))!.meta).toEqual({ name: 'Remote' })
  })

  it('room events are relayed to the client as PUBLISH frames, behind the streamed roster', async () => {
    const { serverRoom, peer } = await createServedRoom('relay')
    await settle() // the roster streams once the peer attaches
    await serverRoom.join({ meta: { name: 'Alice' } })

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

  it("a member's malformed binary frame is rejected with a clean error ack, not a crash", async () => {
    const { serverRoom, stub, peer } = await createServedRoom('bad-binary')
    const { id } = await joinViaStub(stub, peer, 1)

    // The member's own valid ID (so it passes the impersonation check), but corrupt meta JSON — the
    // validating unframe rejects it rather than throwing into the relay.
    const framed = frameWithMemberId(id, new Uint8Array([1]), { meta: { k: 1 } })
    framed[19] = 0x78 // clobber the opening `{` of the meta JSON (see `unframeMemberId`)
    await stub._onPeerPublishBinaryAckReqMessage(framed, 2)
    await settle()

    const ack = peer.decoded().find((f) => f.tag === TAG.ACK_RES && f.ackedSeq === 2)
    expect(ack.status).toBe(ACK_STATUS.ERROR)
    expect(ack.text).toContain('Malformed binary frame')
    expect(serverRoom.count).toBe(1) // the room is unharmed
  })

  it("shields a client publish against the room's declared message type", async () => {
    const { stub, peer } = await createServedRoom('shield')
    // The verifier the build transform generates from `Room<…, { text: string }>`, installed on the
    // publish-ingress slot exactly as `roomReplacer` does (see `RoomShield` and the room replacer).
    stub._publishShield = (v: unknown) =>
      typeof (v as { text?: unknown }).text === 'string' ? true : '`data.text` should be `string`'
    const { id } = await joinViaStub(stub, peer, 1)

    await stub._onPeerPublishAckReqMessage(stringify({ __r: 'data', from: id, data: { text: 42 } }), 2)
    await stub._onPeerPublishAckReqMessage(stringify({ __r: 'data', from: id, data: { text: 'hi' } }), 3)
    await settle()

    const acks = peer.decoded().filter((f) => f.tag === TAG.ACK_RES)
    const bad = acks.find((f) => f.ackedSeq === 2)
    expect(bad.status).toBe(ACK_STATUS.SHIELD_ERROR) // the client rebuilds a ShieldValidationError from this
    expect(bad.text).toContain('should be `string`')
    expect(acks.find((f) => f.ackedSeq === 3).status).toBe(ACK_STATUS.OK) // the valid payload publishes
  })

  it('leaves server-side publishes unshielded — only client input crosses the shield', async () => {
    const { serverRoom, stub } = await createServedRoom('shield-server')
    stub._publishShield = () => 'reject everything' // even a total-reject shield...
    const alice = await serverRoom.join({ meta: { name: 'A' } })
    // ...never runs on a trusted server-side publish: it doesn't pass through the client ingress.
    await expect(alice.publish({ anything: true })).resolves.toBeDefined()
  })

  it("shields a standalone participant's client publish on its own channel (req-publish)", async () => {
    const serverRoom = (await Room.create('shield-participant')) as ServerRoom
    const me = await serverRoom.join({ meta: { name: 'A' } })

    // Serialize the standalone participant through the real participant replacer, with the room's `data`
    // shield present in `context.validators` exactly as auto-generation supplies it — the replacer hands
    // it to `bindParticipantStubChannel`, which runs it at the `req-publish` ingress (its own channel).
    let channel: ServerChannel | undefined
    const context = {
      createChannel: () => (channel = new ServerChannel()),
      registerChannel: () => {},
      sendStream: () => {
        throw new Error('unused')
      },
      validators: new Map([
        [
          'data',
          (v: unknown) =>
            typeof (v as { text?: unknown }).text === 'string' ? true : '`data.text` should be `string`',
        ],
      ]),
      passScope: new Map(),
    } as unknown as ServerReplacerContext
    stringify(me, {
      replacer: createStreamingReplacer(
        () => context,
        () => {},
        [],
      ),
    })

    const frames: Uint8Array[] = []
    channel!._attachPeer(
      new IndexedPeer({ send: (f) => frames.push(f) }, 7, new ReplayBuffer(1024 * 1024, 60_000, 1024 * 1024)),
    )
    const ackFor = (seq: number) =>
      frames.map((f) => decode(f as Uint8Array<ArrayBuffer>)).find((f) => f.tag === TAG.ACK_RES && f.ackedSeq === seq)

    await channel!._onPeerAckReqMessage(JSON.stringify({ __r: 'req-publish', data: { text: 42 } }), 1)
    await channel!._onPeerAckReqMessage(JSON.stringify({ __r: 'req-publish', data: { text: 'hi' } }), 2)
    await settle()

    expect(ackFor(1)!.status).toBe(ACK_STATUS.SHIELD_ERROR) // the malformed payload is rejected at the ingress
    expect(ackFor(1)!.text).toContain('should be `string`')
    expect(ackFor(2)!.status).toBe(ACK_STATUS.OK) // the valid payload publishes
  })

  it('refuses to bind one LocalParticipant to a second client', async () => {
    const serverRoom = (await Room.create('bind-once')) as ServerRoom
    const me = await serverRoom.join({ meta: { name: 'A' } })
    // A participant is a single member's seat: its inbox forwards to one holder and that holder's
    // close ends the membership. A second bind would silently overwrite the first's forwarder and let
    // either close drop the shared seat — so it's rejected instead.
    bindParticipantStubChannel(new ServerChannel(), me)
    expect(() => bindParticipantStubChannel(new ServerChannel(), me)).toThrow(/already bound to a client/)
  })

  it("the room replacer installs the room's auto-generated data shield onto the stub", async () => {
    const serverRoom = (await Room.create('shield-wiring')) as ServerRoom
    // The one wiring line under test: `roomReplacer` must lift `context.validators`' `data` verifier — the
    // shield the transform auto-generates from `Pub` — onto the stub's `_publishShield`, and nowhere else.
    let stub: RoomStubChannel | undefined
    const context = {
      createChannel: () => {
        throw new Error('unused')
      },
      registerChannel: (ch: unknown) => {
        if (ch instanceof RoomStubChannel) stub = ch
      },
      sendStream: () => {
        throw new Error('unused')
      },
      validators: new Map([['data', (v: unknown) => (v === 'ok' ? true : 'only `ok` allowed')]]),
      passScope: new Map(),
    } as unknown as ServerReplacerContext
    stringify(serverRoom, {
      replacer: createStreamingReplacer(
        () => context,
        () => {},
        [],
      ),
    })

    expect(stub?._publishShield).toBeDefined()
    expect(stub!._publishShield!('nope')).toBe('only `ok` allowed') // the verifier the replacer lifted across
    expect(stub!._publishShield!('ok')).toBe(true)
    expect(stub!._validators.get('data')).toBeUndefined() // never the request-validation map (no request shielding)
  })

  it('the error contract matches telefunc across the wire: bug hidden, RoomError shown, Abort carried', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('wire-errors')
    Room.guard(serverRoom, {
      onBeforeJoin: (m) => {
        if (m.meta.secret === 'bug') throw new Error('internal policy-table detail')
        if (m.meta.secret === 'abort') throw Abort({ code: 'BANNED', until: 2030 })
      },
    })
    // Every failure rides the channel's own ack status — no `{ ok: false }` envelope. The client
    // channel rebuilds an `AbortError` from ABORT and a plain `Error` from ERROR (see `roomAckError`).
    const reply = async (req: object, seq: number): Promise<{ status: number; text: string }> => {
      await stub._onPeerAckReqMessage(JSON.stringify(req), seq)
      await settle()
      const ack = peer.decoded().find((f) => f.tag === TAG.ACK_RES && f.ackedSeq === seq)
      return { status: ack.status, text: ack.text }
    }

    // A plain guard throw is a bug: an ERROR ack with the generic message — the real detail never sent.
    const bug = await reply({ __r: 'req-join', meta: { secret: 'bug' } }, 1)
    expect(bug.status).toBe(ACK_STATUS.ERROR)
    expect(bug.text).toBe('Internal Server Error — see server logs')
    expect(bug.text).not.toContain('policy-table')
    // An Abort guard throw is carried — an ABORT ack with the value serialized, so the client rebuilds
    // an `AbortError` whose `abortValue` is intact.
    const aborted = await reply({ __r: 'req-join', meta: { secret: 'abort' } }, 2)
    expect(aborted.status).toBe(ACK_STATUS.ABORT)
    expect(parse(aborted.text)).toEqual({ code: 'BANNED', until: 2030 })
    // A framework `RoomError` (a DM to a non-member) is an ERROR ack showing its operational message.
    const { id } = await joinViaStub(stub, peer, 3)
    const dm = await reply({ __r: 'req-dm', id, to: crypto.randomUUID(), data: 'x', ack: true }, 4)
    expect(dm.status).toBe(ACK_STATUS.ERROR)
    expect(dm.text).toContain('Participant not found')

    expect(serverRoom.count).toBe(1) // both rejected joins admitted nothing
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

  it("binary frames are relayed selectively, per the client's declared (member, track) wants", async () => {
    const { serverRoom, stub, peer } = await createServedRoom('lazy-bin')
    const wanted = await serverRoom.join({ meta: { name: 'wanted' } })
    const unwanted = await serverRoom.join({ meta: { name: 'unwanted' } })
    const subBinary = (wants: unknown, seq: number) =>
      stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', wants }), seq)
    const relayed = () => peer.decoded().filter((f: any) => f.tag === TAG.PUBLISH_BINARY)

    await wanted.publishBinary(new Uint8Array([1]))
    expect(relayed()).toEqual([]) // nothing declared yet

    subBinary({ everyMember: { all: false, tracks: [] }, members: { [wanted.id]: { all: true, tracks: [] } } }, 60)
    await wanted.publishBinary(new Uint8Array([2]))
    await unwanted.publishBinary(new Uint8Array([3]))
    expect(relayed().length).toBe(1) // only the wanted member's frame crossed the wire
    expect(unframeMemberId(relayed()[0]!.data)!.from).toBe(wanted.id)

    // Track-selective: only 'cam' of every member — the default lane and other tracks stay put.
    subBinary({ everyMember: { all: false, tracks: ['cam'] }, members: {} }, 61)
    await wanted.publishBinary(new Uint8Array([4]))
    await wanted.publishBinary(new Uint8Array([5]), { track: 'cam' })
    await unwanted.publishBinary(new Uint8Array([6]), { track: 'mic' })
    expect(relayed().length).toBe(2)
    expect(unframeMemberId(relayed()[1]!.data)!.track).toBe('cam')

    subBinary({ everyMember: { all: true, tracks: [] }, members: {} }, 62)
    await unwanted.publishBinary(new Uint8Array([7]))
    expect(relayed().length).toBe(3)

    // A malformed declaration is rejected — the previous wants stay in force.
    subBinary({ everyMember: { all: 'yes' }, members: {} }, 63)
    await settle()
    expect(stub._binaryWants.everyMember.all).toBe(true)
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

    const sender = await serverRoom.join({ meta: { name: 'Srv' } })
    await sender.send(id, 'psst')

    const dmFramesOf = (frames: any[]) =>
      frames.filter((f) => f.tag === TAG.PUBLISH && (JSON.parse(f.text) as { __r: string }).__r === 'dm')
    expect(dmFramesOf(peer.decoded()).length).toBe(1) // the owner's stub got it
    expect(dmFramesOf(bystanderPeer.decoded())).toEqual([]) // nobody else did
  })

  it("routes a stub member's DM to a server-held participant, validating the sender", async () => {
    const { serverRoom, stub, peer } = await createServedRoom('dm-stub-send')
    const target = await serverRoom.join({ meta: { name: 'Srv' } })
    const inbox: unknown[] = []
    target.listen((data, from) => inbox.push([data, from?.id]))
    const { id } = await joinViaStub(stub, peer, 1)

    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-dm', id, to: target.id, data: 'hi' }), 2)
    // Impersonation: a sender ID not joined through this stub is rejected.
    await stub._onPeerAckReqMessage(JSON.stringify({ __r: 'req-dm', id: target.id, to: target.id, data: 'spoof' }), 3)

    expect(inbox).toEqual([['hi', id]])
    const acks = peer.decoded().filter((f) => f.tag === TAG.ACK_RES)
    expect(acks.find((f) => f.ackedSeq === 2).status).toBe(ACK_STATUS.OK) // valid send
    expect(acks.find((f) => f.ackedSeq === 3).status).toBe(ACK_STATUS.ERROR) // impersonation rejected
  })

  it('text is relayed only after the client subscribes; control always flows', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('lazy-text-stub')
    const me = await serverRoom.join({ meta: { name: 'Alice' } })

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
    const alice = await serverRoom.join({ meta: { name: 'Alice' } })
    const bob = await serverRoom.join({ meta: { name: 'Bob' } })

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

  it('tail mode holds text server-side from Room.get until the first subscribe, then flushes it in order', async () => {
    const serverRoom = (await Room.create('tail-relay')) as ServerRoom
    const alice = await serverRoom.join({ meta: { name: 'A' } })
    serverRoom._startTail() // Room.get(id, { tail: true }): ingestion opens now, before any stub exists

    await alice.publish({ text: 'early' }) // published before the stub attaches — held on the room

    const stub = new RoomStubChannel(serverRoom)
    stub._registerChannel()
    serverRoom._attachStub(stub) // the hold moves onto the stub; nothing is relayed yet
    const peer = attachPeer(stub)

    await alice.publish({ text: 'held' }) // still no subscribe — held on the stub, not relayed

    const relayedData = () =>
      peer
        .decoded()
        .filter((f) => f.tag === TAG.PUBLISH)
        .map((f) => JSON.parse(f.text) as { __r: string; data?: unknown })
        .filter((m) => m.__r === 'data')
        .map((m) => m.data)
    expect(relayedData()).toEqual([]) // the client asked for nothing yet, so nothing crossed the wire

    stub._onPeerBroadcastSubscribe(false) // the client's first subscribe() — flush the held tail, in order
    await settle()
    await alice.publish({ text: 'live' }) // relayed live from here

    expect(relayedData()).toEqual([{ text: 'early' }, { text: 'held' }, { text: 'live' }])
  })

  it('tail flush is member-selective: a scoped subscribe replays only the wanted member, dropping the rest', async () => {
    const serverRoom = (await Room.create('tail-selective')) as ServerRoom
    const alice = await serverRoom.join({ meta: { name: 'A' } })
    const bob = await serverRoom.join({ meta: { name: 'B' } })
    serverRoom._startTail()
    await alice.publish({ text: 'a1' })
    await bob.publish({ text: 'b1' })

    const stub = new RoomStubChannel(serverRoom)
    stub._registerChannel()
    serverRoom._attachStub(stub)
    const peer = attachPeer(stub)

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-text', members: [alice.id] }), 40) // wants Alice only
    await settle()

    const relayed = peer
      .decoded()
      .filter((f) => f.tag === TAG.PUBLISH)
      .map((f) => JSON.parse(f.text) as { __r: string; data?: unknown })
      .filter((m) => m.__r === 'data')
      .map((m) => m.data)
    expect(relayed).toEqual([{ text: 'a1' }]) // Bob's held tail is dropped — the client never wanted it
  })

  it('tail safety timer drops the held tail and releases ingestion when the client never subscribes', async () => {
    vi.useFakeTimers()
    try {
      const serverRoom = (await Room.create('tail-timeout')) as ServerRoom
      const alice = await serverRoom.join({ meta: { name: 'A' } })
      serverRoom._startTail()
      await alice.publish({ text: 'early' })

      const stub = new RoomStubChannel(serverRoom)
      stub._registerChannel()
      serverRoom._attachStub(stub)
      const peer = attachPeer(stub)
      expect(stub._tailPending).not.toBeNull() // holding, awaiting the client's first subscribe

      await vi.advanceTimersByTimeAsync(ROOM_TAIL_ATTACH_TIMEOUT_MS + 1) // ...which never comes
      expect(stub._tailPending).toBeNull() // the safety timer dropped it

      stub._onPeerBroadcastSubscribe(false) // a late subscribe gets no backfill — the tail is gone
      await vi.advanceTimersByTimeAsync(0)
      const relayedData = peer
        .decoded()
        .filter((f) => f.tag === TAG.PUBLISH)
        .map((f) => JSON.parse(f.text) as { __r: string })
        .filter((m) => m.__r === 'data')
      expect(relayedData).toEqual([]) // roster still streams; only the held tail is gone
    } finally {
      vi.useRealTimers()
    }
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

  it('a co-returned selfDelivery=false participant is bound onto its room stub at the source, in either serialization order', async () => {
    const serverRoom = (await Room.create('self-src-order')) as ServerRoom

    // Whether the app returns { room, me } or { me, room }, the serializer converges on one drop-set:
    // the room's stub adopts the pass's set, the participant adds its id — order-independent by identity.
    // A fresh member per pass: each response serializes its own seat (a participant binds to one client).
    for (const order of ['room-first', 'me-first'] as const) {
      const me = await serverRoom.join({ meta: { name: 'me' }, selfDelivery: false })
      const value = order === 'room-first' ? { room: serverRoom, me } : { me, room: serverRoom }
      const registered: unknown[] = []
      const context = {
        createChannel: () => new ServerChannel(),
        registerChannel: (ch: unknown) => registered.push(ch),
        sendStream: () => {
          throw new Error('unused')
        },
        validators: new Map(),
        passScope: new Map(),
      } as unknown as ServerReplacerContext
      const replacer = createStreamingReplacer(
        () => context,
        () => {},
        [],
      )
      stringify(value, { replacer })
      const stub = registered.find((c): c is RoomStubChannel => c instanceof RoomStubChannel)!
      expect(stub._selfSuppressed.has(me.id)).toBe(true)
    }
  })

  it('a source-bound member is suppressed on its own client stub yet still relayed to independent observers', async () => {
    const serverRoom = (await Room.create('self-src-relay')) as ServerRoom
    const me = await serverRoom.join({ meta: { name: 'me' }, selfDelivery: false })

    // Own stub: the serializer bound `me` into its drop-set (see the order-independence test above).
    const own = new RoomStubChannel(serverRoom)
    own._registerChannel()
    own._adoptSelfSuppressed(new Set([me.id]))
    serverRoom._attachStub(own)
    const ownPeer = attachPeer(own)
    own._onPeerBroadcastSubscribe(false)

    // Observer stub: an independent view of the same room, with no binding.
    const observer = new RoomStubChannel(serverRoom)
    observer._registerChannel()
    serverRoom._attachStub(observer)
    const observerPeer = attachPeer(observer)
    observer._onPeerBroadcastSubscribe(false)

    await settle() // rosters stream once the peers attach
    await me.publish({ text: 'own' })
    await settle()

    const dataOf = (p: { decoded: () => any[] }) =>
      p
        .decoded()
        .filter((f) => f.tag === TAG.PUBLISH && (JSON.parse(f.text) as { __r: string }).__r === 'data')
        .map((f) => (JSON.parse(f.text) as { data: unknown }).data)
    expect(dataOf(ownPeer)).toEqual([]) // suppressed at the source — never written to its own client's wire
    expect(dataOf(observerPeer)).toEqual([{ text: 'own' }]) // an independent observer still receives it
  })

  // ── Retained messages (`{ retain: true }`) — the last message per lane, replayed to a client that
  //    subscribes after the fact. Text and binary behave identically; only the lane's scope differs.
  const allBinary = { everyMember: { all: true, tracks: [] }, members: {} }
  const dataFramesOf = (p: { decoded: () => any[] }) =>
    p
      .decoded()
      .filter((f) => f.tag === TAG.PUBLISH)
      .map((f) => JSON.parse(f.text) as { __r: string; data?: unknown })
      .filter((m) => m.__r === 'data')
      .map((m) => m.data)
  const binaryFramesOf = (p: { decoded: () => any[] }) =>
    p
      .decoded()
      .filter((f) => f.tag === TAG.PUBLISH_BINARY)
      .map((f) => unframeMemberId(f.data)!)

  it('replays the last { retain: true } text message to a late subscriber — non-retained and superseded ones are not kept', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-text')
    const alice = await serverRoom.join({ meta: { name: 'Alice' } })
    await alice.publish('transient') // never retained
    await alice.publish('pinned', { retain: true })
    await alice.publish('newer', { retain: true }) // supersedes — last write wins, one slot per room

    stub._onPeerBroadcastSubscribe(false) // a client's subscribe() arriving after the publishes
    await settle()

    expect(dataFramesOf(peer)).toEqual(['newer'])
  })

  it('replays the last retained frame of every (member, track) a client starts watching', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-binary')
    const cam = await serverRoom.join({ meta: { name: 'cam' } })
    await cam.publishBinary(new Uint8Array([1])) // never retained
    await cam.publishBinary(new Uint8Array([2]), { retain: true }) // retained on the default lane
    await cam.publishBinary(new Uint8Array([9]), { track: 'screen', retain: true }) // retained on a named track

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', wants: allBinary }), 40)
    await settle()

    const got = binaryFramesOf(peer)
    expect(got.map((r) => r.payload[0]).sort()).toEqual([2, 9]) // the retained frames, not [1]
    expect(got.every((r) => r.from === cam.id)).toBe(true)
  })

  it('a leaving member takes their retained frames with them — a later subscriber gets nothing for that lane', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-leave')
    const cam = await serverRoom.join({ meta: { name: 'cam' } })
    await cam.publishBinary(new Uint8Array([7]), { retain: true })
    await cam.leave()

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', wants: allBinary }), 41)
    await settle()

    expect(binaryFramesOf(peer)).toEqual([]) // reaped on leave — the stream is over
  })

  it('kicking a member drops their retained frames too — including ones this node never stored', async () => {
    const owner = (await Room.create('retain-kick')) as ServerRoom
    const cam = await owner.join({ meta: { name: 'cam' } })
    await cam.publishBinary(new Uint8Array([2]), { retain: true }) // default lane
    await cam.publishBinary(new Uint8Array([9]), { track: 'screen', retain: true }) // named track
    const adapter = getBroadcastAdapter()
    const prefix = `telefunc:room:retain-kick:rb:${cam.id}:`
    expect((await adapter.keys!(prefix)).length).toBe(2) // both lanes retained

    // A kick goes through evictMember, which has no local track state for the member — cleanup must
    // scan the member's retained keys, not rely on knowing its tracks.
    await Room.removeParticipant('retain-kick', { id: cam.id })

    expect(await adapter.keys!(prefix)).toEqual([]) // swept — no retained frame left behind
  })

  it('replays a retained lane once — growing the want set never resends an already-covered lane', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-once')
    const a = await serverRoom.join({ meta: { name: 'a' } })
    const b = await serverRoom.join({ meta: { name: 'b' } })
    await a.publishBinary(new Uint8Array([1]), { retain: true })
    await b.publishBinary(new Uint8Array([2]), { retain: true })

    stub._onPeerMessage(
      JSON.stringify({
        __r: 'sub-binary',
        wants: { everyMember: { all: false, tracks: [] }, members: { [a.id]: { all: true, tracks: [] } } },
      }),
      42,
    )
    await settle()
    stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', wants: allBinary }), 43) // grow to everyone
    await settle()

    expect(binaryFramesOf(peer).map((r) => r.payload[0])).toEqual([1, 2]) // a once, then b once — never [1, 1, 2]
  })

  it('replays retained text to a member-scoped subscriber only once it wants the sender — the binary rule, for text', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-text-scoped')
    const alice = await serverRoom.join({ meta: { name: 'Alice' } })
    const bob = await serverRoom.join({ meta: { name: 'Bob' } })
    await bob.publish('bob-pinned', { retain: true })

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-text', members: [alice.id] }), 44) // wants Alice, not Bob
    await settle()
    expect(dataFramesOf(peer)).toEqual([]) // Bob's retained message isn't for this subscriber yet

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-text', members: [alice.id, bob.id] }), 45) // now wants Bob
    await settle()
    expect(dataFramesOf(peer)).toEqual(['bob-pinned'])
  })

  // ── Retained replay is causal and metadata-preserving. A subscribe races the publisher's live
  //    stream: the retained back-fill and the live frame can carry the same message, in either order.
  //    Replay must reconcile against the live frames a stub has been handed, so a message lands exactly
  //    once and never rewinds the stream — and carry the frame's real order, not a fresh stamp.
  const textWire = (from: string, data: unknown, ord: { seq: number; timestamp: number }) =>
    encodePublishText(stringify({ __r: 'data', from, data, ord }), ord)

  it('replays a retained binary frame with its real publish receipt — not a fresh seq:0/timestamp stamp', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-binary-receipt')
    const cam = await serverRoom.join({ meta: { name: 'cam' } })
    await cam.publishBinary(new Uint8Array([1])) // lane seq 1 — not retained
    const receipt = await cam.publishBinary(new Uint8Array([2]), { retain: true }) // lane seq 2, retained

    stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', wants: allBinary }), 40)
    await settle()

    const frames = peer.decoded().filter((f) => f.tag === TAG.PUBLISH_BINARY)
    expect(frames).toHaveLength(1)
    expect(receipt.seq).toBeGreaterThan(1) // guards the assertion: a stale seq:0 would be visibly wrong order
    expect(frames[0].info).toEqual({ seq: receipt.seq, timestamp: receipt.timestamp }) // the lane's real order
  })

  it('delivers a retained text message and its own late live echo exactly once', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-echo')
    const alice = await serverRoom.join({ meta: { name: 'Alice' } })
    await alice.publish('hello', { retain: true })

    stub._onPeerBroadcastSubscribe(false) // subscribe after the publish — the retained slot back-fills
    await settle()
    expect(dataFramesOf(peer)).toEqual(['hello']) // replayed once

    // The publish's own live broadcast reaches this node after the retained back-fill: pub/sub fan-out
    // lags the read-your-writes retained store. Re-inject the exact stored frame on the text key — the
    // stub must recognize it as the echo of what it just replayed and drop it, not deliver a duplicate.
    const stored = getBroadcastAdapter().get(roomRetainedTextKey('retain-echo')) as string
    await getBroadcastAdapter().publish(roomTextKey('retain-echo'), stored)
    await settle()
    expect(dataFramesOf(peer)).toEqual(['hello']) // still once — the echo was dropped
  })

  it('skips a retained text frame once a newer live frame from the sender has reached the client', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-stale')
    const alice = await serverRoom.join({ meta: { name: 'Alice' } })
    stub._onPeerBroadcastSubscribe(false)
    await settle()

    const older = { seq: 1, timestamp: 1000 }
    const newer = { seq: 1, timestamp: 2000 }
    stub._relayTextLive(textWire(alice.id, 'live-new', newer), alice.id, newer) // client already saw newer text
    stub._emitRetainedText(textWire(alice.id, 'retained-old', older), alice.id, older) // stale retained, late

    expect(dataFramesOf(peer)).toEqual(['live-new']) // the stale retained is dropped — the stream never rewinds
  })

  it('never drops a live text frame for arriving out of order — the causal gate guards retained replay only', async () => {
    const { serverRoom, stub, peer } = await createServedRoom('reorder-live')
    const alice = await serverRoom.join({ meta: { name: 'Alice' } })
    stub._onPeerBroadcastSubscribe(false)
    await settle()

    const first = { seq: 2, timestamp: 2000 }
    const second = { seq: 1, timestamp: 1000 } // older, arrives second
    stub._relayTextLive(textWire(alice.id, 'a', first), alice.id, first)
    stub._relayTextLive(textWire(alice.id, 'b', second), alice.id, second)

    expect(dataFramesOf(peer)).toEqual(['a', 'b']) // text is a stream: no live frame is withheld for being "old"
  })

  it("drops a retained binary frame's own live echo per (member, track) lane — the binary twin", async () => {
    const { serverRoom, stub, peer } = await createServedRoom('retain-binary-echo')
    const cam = await serverRoom.join({ meta: { name: 'cam' } })
    stub._onPeerMessage(JSON.stringify({ __r: 'sub-binary', wants: allBinary }), 40)
    await settle()

    const framed = frameWithMemberId(cam.id, new Uint8Array([7]))
    const info = { seq: 3, timestamp: 3000 }
    stub._emitRetainedBinary(encodePublishBinary(framed, info), cam.id, DEFAULT_TRACK, info) // retained back-fill
    stub._relayBinaryLive(encodePublishBinary(framed, info), cam.id, DEFAULT_TRACK, info) // its own late live echo

    expect(binaryFramesOf(peer).map((r) => r.payload[0])).toEqual([7]) // delivered once — the echo dropped
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
    send: async (msg: { __r: string; ack?: boolean }) => {
      sent.push(msg)
      // The server returns raw values on an OK ack (no `{ ok: true }` envelope); a failure would
      // reject the request via the channel ack instead.
      if (msg.__r === 'req-join') return { id: joinAck?.id ?? crypto.randomUUID(), joinedAt: 1 }
      if (msg.__r === 'req-dm')
        return msg.ack ? { seq: ++seq, timestamp: 1, response: 'fake-reply' } : { seq: ++seq, timestamp: 1 }
      return undefined
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

  it('rejects a client-side hidden join; a hidden member never rides the roster to a client', async () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('hidden', { count: 1 }))
    await expect(clientRoom.join({ hidden: true })).rejects.toThrow('server-side only')

    const hiddenId = crypto.randomUUID()
    const player = crypto.randomUUID()
    const joins: string[] = []
    clientRoom.onJoin((m) => joins.push(m.id))
    // Hidden members are server-only; even if one leaks into a streamed roster the client drops it.
    fake.emit({
      __r: 'roster',
      members: [
        { id: hiddenId, meta: { role: 'authority' }, joinedAt: 1, hidden: true },
        { id: player, meta: { name: 'P1' }, joinedAt: 2 },
      ],
    })

    expect((await clientRoom.getParticipants({ hidden: true })).map((p) => p.id)).toEqual([]) // never surfaced
    expect(clientRoom.count).toBe(1) // the real player only
    expect((await clientRoom.getParticipants()).map((m) => m.id)).toEqual([player])
    expect(joins).toEqual([])
  })

  it('a directly-granted hidden handle survives roster drift — it is grant-managed, not roster-managed', async () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('grant'))
    const authorityId = crypto.randomUUID()
    // A telefunction returning the hidden authority's handle revives it off-presence.
    clientRoom._reviveRemote({ id: authorityId, meta: { role: 'authority' }, joinedAt: 1, metaSeq: 0, hidden: true })

    // The presence-only roster (no hidden members) is the drift that must not reap the granted handle.
    const player = crypto.randomUUID()
    fake.emit({ __r: 'roster', members: [{ id: player, meta: { name: 'P1' }, joinedAt: 1 }] })
    expect((await clientRoom.getParticipants({ hidden: true })).map((p) => p.id)).toEqual([authorityId]) // survived
    expect((await clientRoom.getParticipants()).map((m) => m.id)).toEqual([player])
  })

  it('join() + publish() wrap the wire protocol; relayed data comes back with sender identity', async () => {
    const memberId = crypto.randomUUID()
    const fake = createFakeStub({ id: memberId })
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('flow'))
    const received: unknown[] = []
    clientRoom.subscribe((data, info, from) => received.push([data, info.key, from.id]))

    const me = await clientRoom.join({ meta: { name: 'Me' } })
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
    const me = await clientRoom.join({ selfDelivery: false })
    const inbox: unknown[] = []
    me.listen((data, from) => inbox.push([data, from]))

    const peer = crypto.randomUUID()
    await me.send(peer, 'psst')
    expect(fake.sent).toContainEqual({ __r: 'req-dm', id: memberId, to: peer, data: 'psst' })
    expect(fake.sent).toContainEqual({ __r: 'req-join', meta: {}, selfDelivery: false })

    fake.emit({ __r: 'dm', to: memberId, from: peer, fromMeta: { name: 'Peer' }, data: 'reply' })
    fake.emit({ __r: 'dm', to: crypto.randomUUID(), from: peer, data: 'not-mine' })
    expect(inbox).toEqual([['reply', { id: peer, meta: { name: 'Peer' }, identity: null }]]) // snapshot sender — peer isn't in the local view
  })

  it('replies to an ack DM with its handler’s return, routed back up the stub', async () => {
    const memberId = crypto.randomUUID()
    const fake = createFakeStub({ id: memberId })
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('dm-ack'))
    const me = await clientRoom.join()
    me.listen((data) => `handled:${data}`) // a client recipient replies exactly like a server-side one

    fake.emit({ __r: 'dm', to: memberId, from: crypto.randomUUID(), data: 'ping', ackId: 'ack-1' })
    await settle()

    expect(fake.sent).toContainEqual({
      __r: 'dm-reply',
      id: memberId,
      ackId: 'ack-1',
      ok: true,
      result: 'handled:ping',
    })
  })

  it('applies leave/p-meta/update/closed events to state and local participants', async () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('events', { meta: { topic: 'a' } }))
    const me = await clientRoom.join()
    const log: string[] = []
    clientRoom.onUpdate((meta) => log.push(`update:${JSON.stringify(meta)}`))
    clientRoom.onClose(() => log.push('closed'))
    me.onLeave((cause) => log.push(`me-left:${JSON.stringify(cause)}`))

    fake.emit({ __r: 'p-meta', id: me.id, meta: { mood: 'happy' }, prev: {}, seq: 1 })
    expect(me.meta).toEqual({ mood: 'happy' })

    fake.emit({ __r: 'update', meta: { topic: 'b' }, prev: { topic: 'a' }, at: 9, by: 'w1' })
    expect(clientRoom.meta).toEqual({ topic: 'b' })

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
      wants: { everyMember: { all: true, tracks: [] }, members: {} },
    })

    const sender = crypto.randomUUID()
    fake.emit({ __r: 'join', id: sender, meta: {}, joinedAt: 1 })
    fake.emitBinary(frameWithMemberId(sender, new Uint8Array([9, 8])))
    expect(bytes).toEqual([[9, 8]])

    unsubscribe()
    expect(fake.sent.filter((m) => m.__r === 'sub-binary').at(-1)).toEqual({
      __r: 'sub-binary',
      wants: { everyMember: { all: false, tracks: [] }, members: {} },
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

    const none = { all: false, tracks: [] }
    const every = { all: true, tracks: [] }

    // Each widening is declared synchronously — a publish right after subscribing must be
    // preceded by its declaration on the wire (same-connection FIFO).
    const unsub1 = (await clientRoom.getParticipant(cam1))!.subscribeBinary(() => {})
    expect(subBinaryMsgs()).toEqual([{ __r: 'sub-binary', wants: { everyMember: none, members: { [cam1]: every } } }])
    ;(await clientRoom.getParticipant(cam2))!.subscribeBinary(() => {})
    expect(subBinaryMsgs().at(-1)).toEqual({
      __r: 'sub-binary',
      wants: { everyMember: none, members: { [cam1]: every, [cam2]: every } },
    })

    // A room-level listener adds the every-member want; listener changes that leave the
    // effective want unchanged send nothing.
    const unsubAll = clientRoom.subscribeBinary(() => {})
    expect(subBinaryMsgs().at(-1)).toEqual({
      __r: 'sub-binary',
      wants: { everyMember: every, members: { [cam1]: every, [cam2]: every } },
    })
    const sentCount = subBinaryMsgs().length
    const unsubDup = (await clientRoom.getParticipant(cam1))!.subscribeBinary(() => {})
    expect(subBinaryMsgs().length).toBe(sentCount)

    // Dropping back to one member narrows it again.
    unsubDup()
    unsubAll()
    unsub1()
    expect(subBinaryMsgs().at(-1)).toEqual({
      __r: 'sub-binary',
      wants: { everyMember: none, members: { [cam2]: every } },
    })

    // Track filters declare exactly the wanted tracks — `null` selects the default lane.
    const unsubScreen = clientRoom.subscribeBinary(() => {}, { track: 'screen' })
    const unsubDefault = (await clientRoom.getParticipant(cam1))!.subscribeBinary(() => {}, { track: null })
    expect(subBinaryMsgs().at(-1)).toEqual({
      __r: 'sub-binary',
      wants: {
        everyMember: { all: false, tracks: ['screen'] },
        members: { [cam2]: every, [cam1]: { all: false, tracks: [''] } },
      },
    })
    unsubScreen()
    unsubDefault()
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

  it('publish({ coalesce }) conflates a burst to one in-flight send plus the latest pending value', async () => {
    const published: unknown[] = []
    const gates: Array<() => void> = []
    let seq = 0
    const info = () => ({ key: 'fake', seq: ++seq, timestamp: 1 })
    const stub = {
      _subscribeLocal: () => () => {},
      _subscribeBinaryLocal: () => () => {},
      _setWireTextSubscribed: () => {},
      send: async (msg: { __r: string }) =>
        msg.__r === 'req-join' ? { id: crypto.randomUUID(), joinedAt: 1 } : undefined,
      publish: (envelope: { data: unknown }) => {
        published.push(envelope.data)
        return new Promise((resolve) => gates.push(() => resolve(info())))
      },
      publishBinary: async () => info(),
      onClose: () => {},
    }
    const clientRoom = new ClientRoom(stub as unknown as ClientBroadcast, createSnapshot('coalesce'))
    const me = await clientRoom.join({ meta: { name: 'cursor' } })

    const pA = me.publish({ x: 1 }, { coalesce: 'pos' }) // sent immediately, now in flight (gated)
    const pB = me.publish({ x: 2 }, { coalesce: 'pos' }) // conflates while A is in flight…
    const pC = me.publish({ x: 3 }, { coalesce: 'pos' }) // …superseded by the newest value
    await Promise.resolve()
    expect(published).toEqual([{ x: 1 }]) // only the first send actually left

    gates.shift()!() // A's send completes → the pending latest value drains next
    await pA
    await Promise.resolve()
    await Promise.resolve()
    expect(published).toEqual([{ x: 1 }, { x: 3 }]) // { x: 2 } was dropped by conflation

    gates.shift()!() // release the second send; B and C resolve with its receipt
    await Promise.all([pB, pC])
    expect(published).toEqual([{ x: 1 }, { x: 3 }])
  })

  it('onDemand fires when demand for a member track turns on, then off', async () => {
    const room = await Room.create('demand')
    const alice = await room.join({ meta: { name: 'A' } })
    const demand: Array<[string | null, number]> = []
    alice.onDemand((track, count) => demand.push([track, count]))

    // A second view of the room subscribes to alice's screen track — demand goes 0 → 1.
    const observer = await Room.get('demand')
    await observer.getParticipants() // materialize the roster so alice is known
    const unsub = (await observer.getParticipant(alice.id))!.subscribeBinary(() => {}, { track: 'screen' })
    await new Promise((r) => setTimeout(r, 30)) // the want gossips over the control lane
    expect(demand).toContainEqual(['screen', 1])

    unsub() // nobody watching the screen track — demand goes 1 → 0 (pause the encoder)
    await new Promise((r) => setTimeout(r, 30))
    expect(demand).toContainEqual(['screen', 0])
  })

  it("a member leaving releases its listeners — the client's declaration narrows without an unsubscribe", async () => {
    const cam = crypto.randomUUID()
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('wants-release', { count: 1 }))
    fake.emit({ __r: 'roster', members: [{ id: cam, meta: {}, joinedAt: 1 }] })
    ;(await clientRoom.getParticipant(cam))!.subscribeBinary(() => {}) // never unsubscribed
    expect(fake.sent.filter((m) => m.__r === 'sub-binary').at(-1)).toEqual({
      __r: 'sub-binary',
      wants: { everyMember: { all: false, tracks: [] }, members: { [cam]: { all: true, tracks: [] } } },
    })

    fake.emit({ __r: 'leave', id: cam })

    expect(fake.sent.filter((m) => m.__r === 'sub-binary').at(-1)).toEqual({
      __r: 'sub-binary',
      wants: { everyMember: { all: false, tracks: [] }, members: {} },
    })
  })

  it('a DM relayed before the join ack resolves is held and delivered — the reactive-send race', async () => {
    const memberId = crypto.randomUUID()
    const fake = createFakeStub({ id: memberId })
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('dm-race'))
    const peer = crypto.randomUUID()

    // The DM arrives on the stub before join() has resolved (different request, same connection).
    fake.emit({ __r: 'dm', to: memberId, from: peer, fromMeta: { name: 'Bot' }, data: 'welcome!' })

    const me = await clientRoom.join()
    const inbox: unknown[] = []
    me.listen((data, from) => inbox.push([data, from?.meta.name]))
    expect(inbox).toEqual([['welcome!', 'Bot']])
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
    const me = await a.join({ meta: { name: 'Ghost' } })
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
      const me = await a.join({ meta: { name: 'Ghost' } })
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

  it("the heartbeat refreshes a member's identity and hidden markers — neither outlives its record", async () => {
    vi.useFakeTimers()
    try {
      const a = await Room.create('hb-markers')
      const tab = await a.join({ meta: { name: 'T1' }, identity: 'user-9' })
      await a.join({ hidden: true })
      const left: unknown[] = []
      tab.onLeave((cause) => left.push(cause))

      // Heartbeat past a full KV TTL window. The record is refreshed each interval; before this fix
      // the sibling index keys still carried the join-time TTL and silently expired right here.
      await vi.advanceTimersByTimeAsync(ROOM_MEMBER_KV_TTL_MS + ROOM_HEARTBEAT_INTERVAL_MS)

      // The hidden marker survived — the lazy seed still excludes the off-presence member.
      expect((await Room.get('hb-markers')).count).toBe(1)

      // The identity index survived — a by-identity kick still resolves the long-lived membership.
      await Room.removeParticipant('hb-markers', { identity: 'user-9', reason: 'banned' })
      expect(left).toEqual([{ type: 'removed', reason: 'banned' }])
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

// ───────────────────────────────────────────────────────────────────────────
// Typed metadata — compile-time contract (tsc runs over this spec): the type
// parameters flow from the statics through every surface.
// ───────────────────────────────────────────────────────────────────────────

describe('typed metadata', () => {
  it('meta type parameters flow end-to-end', async () => {
    type ChatMeta = { topic: string }
    type MemberMeta = { name: string }
    const room = await Room.create<ChatMeta, MemberMeta>('typed', { meta: { topic: 'general' } })
    const topic: string = room.meta.topic

    const me = await room.join({ meta: { name: 'Alice' } })
    const myName: string = me.meta.name
    room.subscribe((_data, _info, from) => {
      const _senderName: string = from.meta.name
    })
    Room.guard(room, {
      onBeforeJoin: (member) => {
        const _joinerName: string = member.meta.name
      },
    })
    const member = await room.getParticipant(me.id)
    if (member) {
      const _memberName: string = member.meta.name
    }
    const snap = room.snapshot()
    const first = snap.participants[0]
    if (first) {
      const _snapName: string = first.meta.name
    }
    const direct = await Room.join<MemberMeta>('typed', { meta: { name: 'Bob' } })
    const _directName: string = direct.meta.name

    expect(topic).toBe('general')
    expect(myName).toBe('Alice')
    expect(_directName).toBe('Bob')
  })
})

// ───────────────────────────────────────────────────────────────────────────
// snapshot()/onChange — the UI-store contract: reference-stable immutable
// views, one change signal for everything observable.
// ───────────────────────────────────────────────────────────────────────────

describe('snapshot() and onChange()', () => {
  it('the reference is stable until something changes, then every change class invalidates it', async () => {
    const room = await Room.create('viewstore', { meta: { topic: 'a' } })
    const changes: number[] = []
    room.onChange(() => changes.push(1))

    const s0 = room.snapshot()
    expect(room.snapshot()).toBe(s0) // cached — uSES contract
    expect(s0).toMatchObject({ id: 'viewstore', meta: { topic: 'a' }, count: 0, isClosed: false })
    expect(Object.isFrozen(s0)).toBe(true)

    const me = await room.join({ meta: { name: 'Alice' }, identity: 'u1' })
    const s1 = room.snapshot()
    expect(s1).not.toBe(s0)
    expect(s1.participants).toMatchObject([{ id: me.id, identity: 'u1', meta: { name: 'Alice' } }])

    await me.setMeta({ name: 'Alicia' })
    const s2 = room.snapshot()
    expect(s2).not.toBe(s1)
    expect(s2.participants[0]!.meta).toEqual({ name: 'Alicia' })
    expect(s1.participants[0]!.meta).toEqual({ name: 'Alice' }) // old snapshots stay immutable

    await Room.setMeta('viewstore', { topic: 'b' })
    const s3 = room.snapshot()
    expect(s3.meta).toEqual({ topic: 'b' })

    await me.leave()
    const s4 = room.snapshot()
    expect(s4.participants).toEqual([])

    await Room.close('viewstore')
    expect(room.snapshot().isClosed).toBe(true)
    expect(changes.length).toBeGreaterThanOrEqual(5) // join, meta, update, leave, close all signaled
  })

  it('a client view starts from the count seed and completes when the roster streams in', () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('viewlazy', { count: 2 }))
    const changes: number[] = []
    clientRoom.onChange(() => changes.push(1))

    const before = clientRoom.snapshot()
    expect(before.count).toBe(2) // seeded — capacity gates correct before the roster lands
    expect(before.participants).toEqual([])

    const alice = crypto.randomUUID()
    const bob = crypto.randomUUID()
    fake.emit({
      __r: 'roster',
      members: [
        { id: alice, meta: { name: 'A' }, joinedAt: 1, metaSeq: 0, identity: 'u1' },
        { id: bob, meta: { name: 'B' }, joinedAt: 2, metaSeq: 0, identity: null },
      ],
    })
    expect(changes.length).toBeGreaterThanOrEqual(1)
    const after = clientRoom.snapshot()
    expect(after).not.toBe(before)
    expect(after.participants.map((p) => [p.id, p.identity])).toEqual([
      [alice, 'u1'],
      [bob, null],
    ])
  })

  it('an echo roster reconcile that changes nothing fires no onChange', () => {
    // Every already-applied membership event triggers a roster refresh; when the streamed roster
    // matches what the live path already applied, the reconcile is a pure echo — it must not bump the
    // state version, or `useSyncExternalStore` re-renders on every echo.
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('echo-reconcile', { count: 0 }))
    const alice = crypto.randomUUID()
    const roster = {
      __r: 'roster' as const,
      members: [{ id: alice, meta: { name: 'A' }, joinedAt: 1, metaSeq: 0, identity: 'u1' }],
    }

    fake.emit(roster) // first load
    const loaded = clientRoom.snapshot()
    const changes: number[] = []
    clientRoom.onChange(() => changes.push(1))

    fake.emit(roster) // identical roster — a pure echo
    expect(changes).toEqual([]) // no spurious onChange
    expect(clientRoom.snapshot()).toBe(loaded) // the snapshot reference stays stable
  })

  it('a drift reconcile fires one onChange per discovered change, with no redundant trailing bump', () => {
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('drift-reconcile', { count: 0 }))
    const alice = crypto.randomUUID()
    const bob = crypto.randomUUID()
    fake.emit({ __r: 'roster', members: [{ id: alice, meta: { name: 'A' }, joinedAt: 1, metaSeq: 0, identity: 'u1' }] })
    const changes: number[] = []
    clientRoom.onChange(() => changes.push(1))

    // A later roster that adds Bob — drift, replayed through applyJoin (which bumps once). The reconcile
    // must not add a second, redundant bump on top of the applier's.
    fake.emit({
      __r: 'roster',
      members: [
        { id: alice, meta: { name: 'A' }, joinedAt: 1, metaSeq: 0, identity: 'u1' },
        { id: bob, meta: { name: 'B' }, joinedAt: 2, metaSeq: 0, identity: null },
      ],
    })
    expect(changes).toEqual([1]) // exactly one — applyJoin's bump alone
  })

  it('an onChange subscriber that reads snapshot() on first roster load sees the roster, not an empty cache', () => {
    // Regression for the documented `useSyncExternalStore(room.onChange, room.snapshot)` pairing:
    // the first reconcile must bump the state version *after* creating entries. A pre-bump let the
    // synchronous snapshot() read cache an empty roster under the new version, and the silent entry
    // creation that followed never re-invalidated it — a populated room whose snapshot() stayed
    // empty forever (while getParticipants() returned everyone).
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('snap-poison-load', { count: 1 }))
    const sawCounts: number[] = []
    clientRoom.onChange(() => sawCounts.push(clientRoom.snapshot().participants.length))

    const alice = crypto.randomUUID()
    fake.emit({
      __r: 'roster',
      members: [{ id: alice, meta: { name: 'A' }, joinedAt: 1, metaSeq: 0, identity: 'u1' }],
    })

    expect(clientRoom.snapshot().participants.map((p) => p.id)).toEqual([alice])
    expect(sawCounts[sawCounts.length - 1]).toBe(1) // the onChange that fired already saw the roster
  })

  it('exposes onChange/snapshot detached — the documented useSyncExternalStore(room.onChange, room.snapshot)', () => {
    // React stores the two callbacks and invokes them with NO receiver. As plain prototype methods they
    // deref `this === undefined` and throw on first render; bound arrow properties survive the detachment.
    const fake = createFakeStub()
    const clientRoom = new ClientRoom(fake.stub, createSnapshot('snap-detached', { count: 0 }))
    const getSnapshot = clientRoom.snapshot // extracted exactly as useSyncExternalStore stores it
    const subscribe = clientRoom.onChange
    expect(() => getSnapshot()).not.toThrow()
    let unsubscribe: () => void = () => {}
    expect(() => {
      unsubscribe = subscribe(() => {})
    }).not.toThrow()
    expect(typeof unsubscribe).toBe('function')
    unsubscribe()
  })

  it('snapshot() hands out frozen metadata — a consumer cannot mutate it into live state', async () => {
    const room = await Room.create('snap-frozen', { meta: { topic: 'x' } })
    await room.join({ meta: { name: 'A' }, identity: 'u1' })
    const snap = room.snapshot()
    expect(snap.participants.length).toBe(1)
    expect(Object.isFrozen(snap.meta)).toBe(true)
    expect(Object.isFrozen(snap.participants[0]!.meta)).toBe(true)
    expect(() => {
      ;(snap.meta as { topic?: string }).topic = 'HACKED' // strict-mode write to a frozen object throws
    }).toThrow()
    expect(room.meta.topic).toBe('x') // the live room meta is untouched
  })

  it('close fires exactly one onChange, and it sees the emptied, closed room', async () => {
    // applyClosed applies the whole transition (leave callbacks, roster clear) before its single bump,
    // so a subscriber reading snapshot() during close sees only the final closed-and-empty room — never
    // a stale roster, and never a transient closed-but-still-populated one from a first-of-two bumps.
    const room = await Room.create('snap-poison-close', { meta: { topic: 'x' } })
    await room.join({ meta: { name: 'Alice' }, identity: 'u1' })
    const seen: { closed: boolean; count: number }[] = []
    room.onChange(() => {
      const s = room.snapshot()
      seen.push({ closed: s.isClosed, count: s.participants.length })
    })

    await Room.close('snap-poison-close')

    const final = room.snapshot()
    expect(final.isClosed).toBe(true)
    expect(final.participants).toEqual([]) // not the stale roster
    expect(seen).toEqual([{ closed: true, count: 0 }]) // one notification, the final state — no transient
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Returnable RemoteParticipant — a view crosses the wire as (room, snapshot);
// ref-identity dedup binds it to the co-returned room's live view.
// ───────────────────────────────────────────────────────────────────────────

describe('returnable RemoteParticipant', () => {
  function makeRoundTrip() {
    const registeredChannels: unknown[] = []
    const serverCtx = {
      createChannel: () => {
        throw new Error('unused')
      },
      registerChannel: (ch: unknown) => registeredChannels.push(ch),
      sendStream: () => {
        throw new Error('unused')
      },
      validators: new Map(),
      passScope: new Map(),
    } as unknown as ServerReplacerContext
    const replacer = createStreamingReplacer(
      () => serverCtx,
      () => {},
      [],
    )
    const serialize = (v: unknown) => stringify(v, { replacer })

    const fakes: FakeStub[] = []
    const clientCtx = {
      createBroadcast: () => {
        const f = createFakeStub()
        fakes.push(f)
        return f.stub
      },
      createChannel: () => {
        throw new Error('unused')
      },
      receiveStream: () => {
        throw new Error('unused')
      },
      waitFor: () => {},
    } as unknown as ClientReviverContext
    const reviver = createStreamingReviver(clientCtx, () => {}, [])
    const parseBody = (b: string) => parse(b, { reviver })
    return { serialize, parseBody, fakes, registeredChannels }
  }

  it('returns alongside its room — one stub, duplicates ===, bound to the live view', async () => {
    const server = await Room.create('viewable')
    const alice = await server.join({ meta: { name: 'Alice' } })
    const member = (await server.getParticipant(alice.id))!

    const { serialize, parseBody, fakes, registeredChannels } = makeRoundTrip()
    const body = serialize({ room: server, member, memberDupe: member })
    expect(registeredChannels.length).toBe(1) // one room stub — the view rides it

    const out = parseBody(body) as { room: ClientRoom; member: RemoteParticipant; memberDupe: RemoteParticipant }
    expect(out.memberDupe).toBe(out.member)
    expect(out.member.id).toBe(alice.id)
    expect(out.member.meta).toEqual({ name: 'Alice' })

    // Bound to the same live view: the room's own lookup returns the very same object.
    fakes[0]!.emit({
      __r: 'roster',
      members: [{ id: alice.id, meta: { name: 'Alice' }, joinedAt: 1, metaSeq: 0 }],
    })
    expect(await out.room.getParticipant(alice.id)).toBe(out.member)
  })

  it('returns on its own — the backing room rides along and keeps the view live', async () => {
    const server = await Room.create('solo-view')
    const bob = await server.join({ meta: { name: 'Bob' }, identity: 'user:bob' })
    const member = (await server.getParticipant(bob.id))!

    const { serialize, parseBody, fakes } = makeRoundTrip()
    const out = parseBody(serialize({ solo: member })) as { solo: RemoteParticipant }
    expect(fakes.length).toBe(1) // the embedded room minted its (one) client stub
    expect(out.solo.id).toBe(bob.id)
    expect(out.solo.meta).toEqual({ name: 'Bob' })
    // App identity rides the snapshot — a directly-returned view reports it immediately, not
    // `null`-until-roster (there is no roster here to heal it).
    expect(out.solo.identity).toBe('user:bob')

    // Live: the member's events flow through the embedded room's stub into the view.
    fakes[0]!.emit({ __r: 'p-meta', id: bob.id, meta: { name: 'Bobby' }, prev: { name: 'Bob' }, seq: 1 })
    expect(out.solo.meta).toEqual({ name: 'Bobby' })
    const causes: unknown[] = []
    out.solo.onLeave((cause) => causes.push(cause))
    fakes[0]!.emit({ __r: 'leave', id: bob.id, cause: 'removed', reason: 'bye' })
    expect(causes).toEqual([{ type: 'removed', reason: 'bye' }])
  })
})
