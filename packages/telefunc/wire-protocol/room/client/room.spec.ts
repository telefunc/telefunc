import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ChannelClosedError } from '../../channel/channelErrors.js'
import { parse } from '@brillout/json-serializer/parse'
import { CHANNEL_TRANSPORT } from '../../constants.js'
import { ACK_STATUS, TAG } from '../../wire/connectionFrame.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { ROOM_TRACK_WANTS_MAX } from '../constants.js'
import { DEFAULT_TRACK } from '../binary.js'
import { ClientRoom } from './room.js'
import { ClientBroadcast } from '../../channel/client/channel.js'
import { Room } from '../server/statics.js'
import { ServerRoom } from '../server/room.js'
import { config as clientConfig } from '../../../client/clientConfig.js'
import { roomRemoteReviver } from '../response-client.js'
import type { InternalClientReviverContext } from '../../values/types.js'
import {
  attachPeer,
  disposeRoomBackend,
  fakeClient,
  forceRoomGc,
  gcFixture,
  installRoomBackend,
  joinThrough,
  pendingClientJoin,
  register,
  retainOnlyCallbackRemote,
  retainOnlyDepartedParticipant,
  retainOnlyJoinedParticipant,
  retainOnlyListedRemote,
  retainOnlyRevivedRemote,
  rosterEnded,
  snapshot,
  standaloneParticipant,
  trackNames,
  type Peer,
} from '../../test/room.js'

beforeEach(() => installRoomBackend())
afterEach(disposeRoomBackend)
describe('client Room lifecycle', () => {
  it('refuses a subscribeBinary naming more than ROOM_TRACK_WANTS_MAX tracks of one room handle, declaring nothing for it', () => {
    const declared: unknown[] = []
    const { client } = fakeClient('client-track-wants', { send: async (notice) => void declared.push(notice) })
    const offs = trackNames(ROOM_TRACK_WANTS_MAX).map((track) => client.subscribeBinary(() => {}, { track }))
    expect(() => client.subscribeBinary(() => {}, { track: 'past' })).toThrow(
      `Too many binary tracks subscribed on one room (${ROOM_TRACK_WANTS_MAX} max)`,
    )
    // A track already named, and every track, take nothing more.
    client.subscribeBinary(() => {}, { track: 't0' })
    client.subscribeBinary(() => {})
    offs[1]!()
    client.subscribeBinary(() => {}, { track: 'past' })
    expect(declared).toHaveLength(ROOM_TRACK_WANTS_MAX + 3)
    expect(declared.at(-1)).toEqual({ __r: 'sub-binary', member: null, track: 'past', on: true })
  })
  it("keeps a client-held participant's meta in accepted revision order", async () => {
    const acks: Array<(accepted: unknown) => void> = []
    const { participant, notify } = standaloneParticipant({ v: 0 }, () => new Promise((resolve) => acks.push(resolve)))
    const first = participant.setMeta({ v: 'A' })
    const second = participant.setMeta({ v: 'B' })
    notify({ __r: 'p-meta', meta: { v: 'B' }, seq: 2 })
    acks[1]!({ meta: { v: 'B' }, seq: 2 })
    acks[0]!({ meta: { v: 'A' }, seq: 1 }) // the older write's ack arrives last
    await Promise.all([first, second])
    expect(participant.meta).toEqual({ v: 'B' })
  })
  it("holds DMs before a client-held participant's first listen() within the bufferLimit its server sent", () => {
    const dm = (data: number) => ({ __r: 'dm', from: '', fromMeta: null, data })
    const roomy = standaloneParticipant({})
    for (let n = 0; n < 200; n++) roomy.notify(dm(n))
    const received: unknown[] = []
    roomy.participant.listen((data) => void received.push(data))
    expect(received).toEqual(Array.from({ length: 200 }, (_, n) => n))
    // Their serialized bytes alone fit 20,000; with each one's overhead they don't.
    const tight = standaloneParticipant({}, undefined, 20_000)
    for (let n = 0; n < 200; n++) tight.notify(dm(n))
    const kept: number[] = []
    tight.participant.listen((data) => void kept.push(data as number))
    expect(kept.length).toBeGreaterThan(0)
    expect(kept.length).toBeLessThan(200)
    expect(kept).toEqual(Array.from({ length: kept.length }, (_, i) => 200 - kept.length + i))
  })
  it("applies a handed-out participant's whole demand set as the changes from what it had", () => {
    const { participant, notify } = standaloneParticipant({})
    notify({ __r: 'demand', track: 'screen', wanted: true })
    const demand: unknown[] = []
    participant.onDemand((track, wanted) => demand.push([track, wanted]))
    demand.length = 0
    notify({ __r: 'demand-state', tracks: [DEFAULT_TRACK] })
    expect(demand).toEqual([
      ['screen', false],
      [null, true],
    ])
  })
  describe('Room-derived handle ownership (real GC)', () => {
    it.each([
      ['a roster participant', 'gc-list-owner', false, retainOnlyListedRemote],
      ['a participant revived with its Room', 'gc-revived-owner', false, retainOnlyRevivedRemote],
      ['a joined participant', 'gc-join-owner', true, retainOnlyJoinedParticipant],
      ['a callback participant', 'gc-callback-owner', false, retainOnlyCallbackRemote],
      ['a departed participant', 'gc-departed-owner', true, retainOnlyDepartedParticipant],
    ] as const)('does not make %s the owner of its Room wrapper', async (_handle, roomId, join, retainOnly) => {
      const gc = gcFixture(roomId, join)
      const retained = await retainOnly(gc)
      await forceRoomGc()
      expect(retained.room.deref()).toBeUndefined()
      expect(retained.member.id).toBe(gc.memberId)
      expect(gc.closed()).toBe(1)
    })
  })
  it('counts a member of the seed that leaves before the first roster out at once, and a hidden one not at all', () => {
    const { client, emit } = fakeClient('pre-roster-leave', undefined, { count: 2 })
    emit({ __r: 'leave', id: crypto.randomUUID(), hidden: true })
    expect(client.count).toBe(2)
    emit({ __r: 'leave', id: crypto.randomUUID() }, 2)
    expect(client.count).toBe(1)
  })
  it('does not emit onEmpty from a partial pre-roster member map', () => {
    const { client, emit } = fakeClient('pre-roster-empty', undefined, { count: 2 })
    const memberId = crypto.randomUUID()
    let empty = 0
    client.onEmpty(() => empty++)
    emit({ __r: 'join', id: memberId, meta: {}, joinedAt: 1 })
    emit({ __r: 'leave', id: memberId }, 2)
    expect(client.count).toBe(2)
    expect(empty).toBe(0)
  })
  it('keeps a client participant active so a rejected leave request can be retried', async () => {
    let leaveAttempts = 0
    const { client } = fakeClient('retry-client-leave', {
      send: async (message) => {
        const request = message as { __r?: string }
        if (request.__r === 'req-join') return { id: crypto.randomUUID(), joinedAt: Date.now() }
        if (request.__r === 'req-leave' && leaveAttempts++ === 0) throw new Error('transient leave rejection')
        return undefined
      },
    })
    const member = await client.join()
    const causes: unknown[] = []
    client.onLeave((_, cause) => causes.push(cause))
    await expect(member.leave()).rejects.toThrow('transient leave rejection')
    await expect(member.publish('still-active')).resolves.toMatchObject({ seq: 1 })
    await expect(member.leave()).resolves.toBeUndefined()
    expect(leaveAttempts).toBe(2)
    expect(causes).toEqual([{ type: 'left' }])
  })
  it('settles a client join across a pre-ack closed event', async () => {
    const { id, ack, emit, joining } = await pendingClientJoin('pre-ack-closed')
    emit({ __r: 'closed' })
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    await expect(participant.publish('zombie-check')).rejects.toThrow(/left/i)
    const causes: unknown[] = []
    participant.onLeave((cause) => causes.push(cause))
    expect(causes).toEqual([{ type: 'closed' }])
  })
  it('drops an ack DM reply that settles after the client stub closed', async () => {
    const { id, ack, fake, emit, joining } = await pendingClientJoin('dm-reply-after-close')
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const answer = createDeferred<string>()
    participant.listen(() => answer.promise)
    emit(
      { __r: 'dm', to: id, from: crypto.randomUUID(), fromMeta: {}, fromMetaSeq: 0, data: 'hi', ackId: 'ack-late' },
      1,
    )
    Object.defineProperty(fake.stub, 'isClosed', { value: true })
    const send = vi.spyOn(fake.stub, 'send').mockImplementation(() => {
      throw new ChannelClosedError()
    })
    answer.resolve('late')
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(send).not.toHaveBeenCalled()
  })
  it("applies a member's whole demand set as the changes from what it had", async () => {
    const { id, ack, emit, joining } = await pendingClientJoin('demand-state')
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 1)
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    emit({ __r: 'demand', member: id, track: 'screen', wanted: true }, 2)
    emit({ __r: 'demand', member: id, track: 'mic', wanted: true }, 3)
    const demand: unknown[] = []
    participant.onDemand((track, wanted) => demand.push([track, wanted]))
    demand.length = 0
    emit({ __r: 'demand-state', member: id, tracks: ['mic', DEFAULT_TRACK] }, 4)
    expect(demand).toEqual([
      ['screen', false],
      [null, true],
    ])
  })
  it('hands listen() an unstamped DM from a member of a roster still streaming in as that member', async () => {
    const { id, ack, emit, joining } = await pendingClientJoin('dm-pending-sender')
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const senders: unknown[] = []
    participant.listen((_data, from) => void senders.push(from))
    const from = crypto.randomUUID()
    emit(
      {
        __r: 'roster',
        members: [{ id: from, meta: { name: 'Ada' }, joinedAt: 1, metaSeq: 1, identity: 'ada' }],
        part: 0,
        more: true,
      },
      1,
    )
    emit({ __r: 'dm', to: id, from, data: 'hi' }, 2)
    expect(senders).toEqual([{ id: from, meta: { name: 'Ada' }, identity: 'ada' }])
  })
  it("delivers member-addressed events that arrive before the participant's join ack", async () => {
    const replies: unknown[] = []
    const { id, ack, emit, joining, client } = await pendingClientJoin('pre-ack-events', (message) => {
      if ((message as { __r?: string }).__r === 'dm-reply') replies.push(message)
    })
    emit(
      {
        __r: 'dm',
        to: id,
        from: crypto.randomUUID(),
        fromMeta: {},
        fromMetaSeq: 0,
        data: 'welcome',
        ackId: 'ack-welcome',
      },
      1,
    )
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 2)
    emit(
      { __r: 'dm', to: id, from: crypto.randomUUID(), fromMeta: {}, fromMetaSeq: 0, data: 'again', ackId: 'ack-again' },
      3,
    )
    emit({ __r: 'demand', member: id, track: 'screen', wanted: true }, 4)
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const demand: unknown[] = []
    participant.onDemand((track, wanted) => demand.push([track, wanted]))
    participant.listen((data) => `got ${String(data)}`)
    await vi.waitFor(() =>
      expect(replies).toEqual([
        expect.objectContaining({ ackId: 'ack-welcome', reply: { ok: true, result: 'got welcome' } }),
        expect.objectContaining({ ackId: 'ack-again', reply: { ok: true, result: 'got again' } }),
      ]),
    )
    expect(demand).toEqual([['screen', true]])
    expect(client.count).toBe(1)
  })
  it('leaves, without a ghost, a participant whose leave beat its join ack', async () => {
    const { id, ack, emit, joining, client } = await pendingClientJoin('pre-ack-leave')
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 1)
    emit({ __r: 'leave', id, cause: 'removed' }, 2)
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const causes: unknown[] = []
    participant.onLeave((cause) => causes.push(cause.type))
    expect(causes).toEqual(['removed'])
    expect(client.count).toBe(0)
  })
  it("reports another member's leave with its cause to a page that reconnects while its own join is pending", async () => {
    const room = (await Room.create('reattach-during-join')) as ServerRoom
    const other = await room.join()
    const stub = register(room)
    const first = attachPeer(stub)
    await vi.waitFor(() => expect(rosterEnded(first)).toBe(true))
    const ack = createDeferred<{ id: string; joinedAt: number }>()
    const { client, emit } = fakeClient(room.id, {
      send: async (message) => ((message as { __r?: string }).__r === 'req-join' ? await ack.promise : undefined),
    })
    const feed = (frames: ReturnType<Peer['decoded']>) => {
      for (const frame of frames) if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
    }
    feed(first.decoded())
    const causes: unknown[] = []
    client.onLeave((member, cause) => causes.push([member.id === other.id, cause.type]))
    const lastSeq = Math.max(...first.decoded().map((frame) => ('seq' in frame ? frame.seq : 0)))
    stub._onPeerDisconnect(first.peer, 1_000)
    const joining = client.join()
    await other.leave()
    // Back online: the replayed leave, then a fresh roster without `other`, all before the join's answer.
    const second = attachPeer(stub, lastSeq)
    await vi.waitFor(() => expect(rosterEnded(second)).toBe(true))
    const fed = second.decoded().length
    feed(second.decoded())
    const id = await joinThrough(stub)
    feed(second.decoded().slice(fed))
    ack.resolve({ id, joinedAt: Date.now() })
    await joining
    expect(causes).toEqual([[true, 'left']])
  })
  it('sends DMs from a client participant, and rejects an option a client call does not have', async () => {
    const requests: unknown[] = []
    const { id, ack, emit, joining, client } = await pendingClientJoin('client-unknown-options', (message) => {
      requests.push(message)
      return { seq: requests.length, timestamp: 1 }
    })
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 1)
    ack.resolve({ id, joinedAt: 1 })
    const me = await joining
    const calls: Array<[() => unknown, string]> = [
      [() => client.getParticipants({ all: true } as never), 'Unknown getParticipants() option: all'],
      [() => me.publish('x', { persist: true } as never), 'Unknown publish() option: persist'],
      [() => me.send(id, 'x', { confirm: true } as never), 'Unknown send() option: confirm'],
      [() => me.publish('x', { retain: 1 } as never), 'publish() options.retain should be a boolean'],
      [() => me.publish('x', { coalesce: 1 } as never), 'publish() options.coalesce should be a string'],
      [() => me.send(id, 'x', { ack: 'yes' } as never), 'send() options.ack should be a boolean'],
      [
        () => me.publishBinary(new Uint8Array([1]), { retain: 1 } as never),
        'publishBinary() options.retain should be a boolean',
      ],
    ]
    for (const [call, message] of calls) await expect(Promise.resolve().then(call)).rejects.toThrow(message)
    await me.send(id, 'fire and forget')
    await me.send(id, 'confirmed', { ack: true })
    expect(requests).toEqual([
      expect.objectContaining({ __r: 'req-dm', data: 'fire and forget' }),
      expect.objectContaining({ __r: 'req-dm', data: 'confirmed', ack: true }),
    ])
    expect(requests[0]).not.toHaveProperty('ack')
  })
  it('ends a local participant that a roster no longer lists', async () => {
    const { id, ack, emit, joining, client } = await pendingClientJoin('roster-drops-local')
    emit({ __r: 'join', id, meta: {}, joinedAt: 1 }, 1)
    ack.resolve({ id, joinedAt: 1 })
    const participant = await joining
    const causes: unknown[] = []
    participant.onLeave((cause) => causes.push(cause.type))
    client.onLeave((_, cause) => causes.push(cause?.type))
    emit({ __r: 'roster', members: [], part: 0 }, 2)
    expect(causes).toEqual(['removed', 'removed'])
  })
  it('gives an unstamped message from a member of a roster still streaming in the meta changes relayed after its part', async () => {
    const { client, emit } = fakeClient('pending-sender-meta')
    const member = crypto.randomUUID()
    const senderMetas: unknown[] = []
    client.subscribe((_data, _info, from) => senderMetas.push(from.meta))
    emit({ __r: 'roster', members: [{ id: member, meta: { v: 0 }, joinedAt: 1, metaSeq: 0 }], part: 0, more: true }, 1)
    emit({ __r: 'p-meta', id: member, meta: { v: 1 }, seq: 1 }, 2)
    emit({ __r: 'data', from: member, data: 'hi' }, 3)
    emit({ __r: 'roster', members: [], part: 1 }, 4)
    expect(senderMetas).toEqual([{ v: 1 }])
    expect((await client.getParticipant(member))?.meta).toEqual({ v: 1 })
  })
  it("derives participant-update prev from the receiver's own applied state", async () => {
    const { client, emit } = fakeClient('receiver-local-prev')
    const memberId = crypto.randomUUID()
    emit({ __r: 'roster', members: [{ id: memberId, meta: { step: 0 }, joinedAt: 1, metaSeq: 0 }], part: 0 })
    const member = await client.getParticipant(memberId)
    const updates: Array<[unknown, unknown]> = []
    member!.onUpdate((meta, prev) => updates.push([meta, prev]))
    // This receiver skipped the writer's step:1 revision; prev is the value it actually transitioned away from.
    emit({ __r: 'p-meta', id: memberId, meta: { step: 2 }, seq: 2 }, 2)
    expect(updates).toEqual([[{ step: 2 }, { step: 0 }]])
  })
  it('makes the closed-and-empty state visible before participant leave callbacks run', async () => {
    const { client, emit } = fakeClient('atomic-close-state')
    const memberId = crypto.randomUUID()
    emit({ __r: 'roster', members: [{ id: memberId, meta: {}, joinedAt: 1, metaSeq: 0 }], part: 0 })
    const member = await client.getParticipant(memberId)
    const before = client.snapshot()
    let observed: unknown
    member!.onLeave(() => {
      const snap = client.snapshot()
      observed = {
        closed: client.isClosed,
        count: client.count,
        snapshotChanged: snap !== before,
        snapshotClosed: snap.isClosed,
        snapshotCount: snap.count,
        participants: snap.participants.length,
      }
    })
    emit({ __r: 'closed' }, 2)
    expect(observed).toEqual({
      closed: true,
      count: 0,
      snapshotChanged: true,
      snapshotClosed: true,
      snapshotCount: 0,
      participants: 0,
    })
  })
  it('gives a participant revived with its Room no lifecycle of its own', async () => {
    const { client } = fakeClient('revived-no-lifecycle')
    const context = { shareLifecycle: () => {} } as unknown as InternalClientReviverContext
    const metadata = { room: client, id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0, identity: null }
    const revived = roomRemoteReviver.revive(metadata, context)
    await revived.close()
    revived.abort(new Error('aborted') as never)
    expect(client.isClosed).toBe(false)
    expect(client._getRemote(metadata.id)).toBe(revived.value)
  })
  it('fires onClose for a listener registered after the room closed', () => {
    const { client } = fakeClient('closed-before-listener', undefined, { closed: true, count: 0 })
    let closes = 0
    client.onClose(() => closes++)
    expect(closes).toBe(1)
  })
  it('hands out a participant revived into a closed room as having left with it', () => {
    const { client } = fakeClient('revived-into-closed', undefined, { closed: true, count: 0 })
    const member = client._reviveRemote({ id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0, identity: null })
    const causes: unknown[] = []
    member.onLeave((cause) => causes.push(cause))
    expect(causes).toEqual([{ type: 'closed' }])
    expect(client.snapshot().participants).toEqual([])
  })
  it('fires onLeave on a directly held hidden member when its leave is relayed', () => {
    const { client, emit } = fakeClient('client-hidden-leave')
    emit({ __r: 'roster', members: [], part: 0 })
    const hidden = client._reviveRemote({
      id: crypto.randomUUID(),
      meta: {},
      joinedAt: 1,
      metaSeq: 0,
      identity: null,
      hidden: true,
    })
    const causes: unknown[] = []
    hidden.onLeave((cause) => causes.push(cause?.type))
    emit({ __r: 'leave', id: hidden.id, cause: 'removed', hidden: true }, 2)
    expect(causes).toEqual(['removed'])
  })
  it('keeps a directly held hidden member while its roster carries it, and rejects client enumeration', async () => {
    const { client, emit } = fakeClient('client-hidden-roster')
    emit({ __r: 'roster', members: [], part: 0 })
    const member = { id: crypto.randomUUID(), meta: { role: 'moderator' }, joinedAt: 1, metaSeq: 0, identity: null }
    const hidden = client._reviveRemote({ ...member, hidden: true })
    let left = 0
    hidden.onLeave(() => left++)
    emit({ __r: 'roster', members: [{ ...member, hidden: true }], part: 0 }, 2)
    expect(client._getRemote(hidden.id)).toBe(hidden)
    await expect(client.getParticipants({ hidden: true })).rejects.toThrow(
      'Hidden participants can only be enumerated on the server',
    )
    // A roster without it: the member left before this client's stub could hear its leave.
    emit({ __r: 'roster', members: [], part: 0 }, 3)
    expect(client._getRemote(hidden.id)).toBeNull()
    expect(left).toBe(1)
  })
  it("rejects a member's publish the server refused as expected, without reporting a client bug", async () => {
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    // A real ClientBroadcast whose wire never opens: the test answers its requests.
    clientConfig.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
    const stub = new ClientBroadcast({
      channelId: crypto.randomUUID(),
      key: 'publish-refused',
      transports: [CHANNEL_TRANSPORT.SSE],
      telefuncUrl: 'http://publish-refused.test/_telefunc',
      connectionKey: crypto.randomUUID(),
    })
    vi.spyOn(stub, 'send').mockImplementation((async (message: { __r?: string }) =>
      message.__r === 'req-join' ? { id: crypto.randomUUID(), joinedAt: 1 } : undefined) as never)
    try {
      const me = await new ClientRoom(stub, snapshot('publish-refused')).join()
      const publishing = me.publish('hi')
      const text = 'Participant not found (left?)'
      stub._dispatchFrame({ tag: TAG.ACK_RES, index: 0, seq: 1, bytes: 0, ackedSeq: 1, status: ACK_STATUS.ERROR, text })
      await expect(publishing).rejects.toThrow(text)
      expect(report).not.toHaveBeenCalled()
    } finally {
      stub.abort()
      delete clientConfig.fetch
    }
  })
  it('takes a coalesced publish that waits behind its key as it was at the call', async () => {
    const memberId = crypto.randomUUID()
    const { fake, client } = fakeClient('client-coalesce-owned', {
      send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined),
    })
    const me = await client.join()
    const sent: string[] = []
    let releaseFirst!: () => void
    vi.spyOn(fake.stub as any, '_publishUnreported').mockImplementation(async (...args: unknown[]) => {
      sent.push(JSON.stringify(args))
      if (sent.length === 1) await new Promise<void>((resolve) => (releaseFirst = resolve))
      return { key: 'fake', seq: sent.length, timestamp: 1 }
    })
    void me.publish({ draft: 'first' }, { coalesce: 'draft' })
    const draft = { draft: 'hello' }
    const queued = me.publish(draft, { coalesce: 'draft' }) // waits behind the first
    draft.draft = '' // the page clears its draft once submitted
    releaseFirst()
    await queued
    expect(sent[1]).toContain('hello')
  })
  it("leaves no unhandled rejection behind a client participant's un-awaited publish, binary publish or DM that fails", async () => {
    const memberId = crypto.randomUUID()
    const { client } = fakeClient('client-fire-and-forget', {
      send: async (message: any) => (message.__r === 'req-join' ? { id: memberId, joinedAt: 1 } : undefined),
    })
    const me = await client.join()
    await me.leave()
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown) => void unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    try {
      me.publish({ cursor: 1 })
      me.publish({ cursor: 2 }, { coalesce: 'cursor' })
      me.publishBinary(new Uint8Array([1]))
      me.send(memberId, 'dm')
      await new Promise((resolve) => setTimeout(resolve, 10))
      expect(unhandled).toEqual([])
      await expect(me.publish({ cursor: 3 })).rejects.toThrow('Participant left the room')
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
  })
  it('declares its member subscriptions while a room-wide one covers them, so the server never wants none when it stops', () => {
    const log: unknown[] = []
    const { client, emit } = fakeClient('text-want-order', {
      wireDeclarations: log as boolean[],
      send: async (message: any) => {
        if (message.__r === 'sub-text') log.push(message.members)
      },
    })
    const member = { id: crypto.randomUUID(), meta: {}, joinedAt: 1, metaSeq: 0, identity: null }
    emit({ __r: 'roster', members: [member], part: 0 })
    const stopRoomWide = client.subscribe(() => {})
    log.length = 0
    client._getRemote(member.id)!.subscribe(() => {})
    // Declared at once, so a stream stopped live or in a reattach's RECONCILE finds the member set in place.
    expect(log).toEqual([{ [member.id]: true }])
    stopRoomWide()
    expect(log).toEqual([{ [member.id]: true }, false])
  })
  it('declares nothing while its stub is closing, so an unsubscribe during the close returns normally', () => {
    let closing = false
    const { client, fake } = fakeClient('declare-while-closing', {
      // Like ClientChannel.send on a closed channel: it throws synchronously.
      send: () => {
        if (closing) throw new ChannelClosedError()
        return Promise.resolve(undefined)
      },
    })
    Object.defineProperty(fake.stub, 'isClosed', { get: () => closing })
    const unsubscribe = client.onAnnounce(() => {})
    closing = true
    expect(() => unsubscribe()).not.toThrow()
  })
  it('declares a room-level default binary track without an earlier all-track listener', () => {
    const sent: unknown[] = []
    const { client } = fakeClient('default-track-declaration', {
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    client.subscribeBinary(() => {}, { track: null })
    expect(sent).toContainEqual({ __r: 'sub-binary', member: null, track: DEFAULT_TRACK, on: true })
  })
  it('declares only the binary want that changed, and the text wants of the member whose listeners changed', () => {
    const sent: unknown[] = []
    const { client, emit } = fakeClient('declared-member-changes', {
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    const members = [0, 1, 2].map(() => ({
      id: crypto.randomUUID(),
      meta: {},
      joinedAt: 1,
      metaSeq: 0,
      identity: null,
    }))
    emit({ __r: 'roster', members, part: 0 })
    for (const { id } of members) client._getRemote(id)!.subscribeBinary(() => {})
    client._getRemote(members[1]!.id)!.subscribe(() => {})()
    expect(sent).toEqual([
      ...members.map(({ id }) => ({ __r: 'sub-binary', member: id, track: null, on: true })),
      { __r: 'sub-text', announce: false, members: { [members[1]!.id]: true } },
      { __r: 'sub-text', announce: false, members: { [members[1]!.id]: false } },
    ])
  })
  it("withdraws the wants a member's listeners declared once that member leaves", () => {
    const sent: unknown[] = []
    const { client, emit } = fakeClient('wants-left-member', {
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    const id = crypto.randomUUID()
    emit({ __r: 'roster', members: [{ id, meta: {}, joinedAt: 1, metaSeq: 0, identity: null }], part: 0 })
    client._getRemote(id)!.subscribe(() => {})
    client._getRemote(id)!.subscribeBinary(() => {})
    sent.length = 0
    emit({ __r: 'leave', id })
    expect(sent).toEqual([
      { __r: 'sub-text', announce: false, members: { [id]: false } },
      { __r: 'sub-binary', member: id, track: null, on: false },
    ])
  })
  it('declares wants only when they change, so listener churn sends nothing', () => {
    const sent: unknown[] = []
    const wireDeclarations: boolean[] = []
    const { client } = fakeClient('wants-on-change', {
      wireDeclarations,
      send: async (message) => {
        sent.push(message)
        return undefined
      },
    })
    for (let i = 0; i < 3; i++) client.onChange(() => {})()
    expect(sent).toEqual([])
    const stop = client.onAnnounce(() => {})
    client.onChange(() => {})()
    stop()
    expect(sent).toEqual([
      { __r: 'sub-text', members: {}, announce: true },
      { __r: 'sub-text', members: {}, announce: false },
    ])
    expect(wireDeclarations).toEqual([])
  })
})
