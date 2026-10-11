import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { IndexedPeer } from '../server/IndexedPeer.js'
import { TAG, decode } from '../shared-ws.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import { ClientRoom } from './client.js'
import { ClientBroadcast } from '../client/channel.js'
import type { ChannelPublishInfo } from '../channel.js'
import { Room } from './server/statics.js'
import type { ServerRoom } from './server/room.js'
import { ResponseRoomGrants, RoomStubChannel } from './server/stub.js'
import { disposeBackend, installBackend } from '../backend/install.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import { ROOM_TRACK_WANTS_MAX } from './constants.js'

// Each spec here times a stall at 100,000 members, so it runs in a worker of its own: room.spec's other specs leave
// rooms reachable (vitest keeps every spy, and a stub no client connects to waits out its TTL), and collecting them
// lands in the timed turns.
beforeEach(async () => {
  await disposeBackend()
  const driver = new MemoryBackend({ state: new MemoryBackendState() })
  installBackend(() => driver)
})
afterEach(async () => {
  await disposeBackend()
})
describe('Room at 100,000 members', () => {
  it('keeps the event loop turning through a heartbeat tick renewing and reading 100,000 owned members', async () => {
    const room = (await Room.create('heartbeat-turns')) as ServerRoom
    await Promise.all(Array.from({ length: 100_000 }, () => room.join()))
    const longestGap = async () => {
      let last = performance.now()
      let longest = 0
      const timer = setInterval(() => {
        const now = performance.now()
        longest = Math.max(longest, now - last)
        last = now
      }, 1)
      await subsOf(room)._heartbeatTick()
      clearInterval(timer)
      return Math.max(longest, performance.now() - last)
    }
    // The shorter of two ticks, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await longestGap(), await longestGap())).toBeLessThanOrEqual(50)
  }, 60_000)
  it('keeps the event loop turning through a replan of 100,000 members', async () => {
    const room = (await Room.create('replan-turns')) as ServerRoom
    await Promise.all(Array.from({ length: 100_000 }, () => room.join()))
    const longestGap = async () => {
      let last = performance.now()
      let longest = 0
      const timer = setInterval(() => {
        const now = performance.now()
        longest = Math.max(longest, now - last)
        last = now
      }, 1)
      await subsOf(room).replan()
      clearInterval(timer)
      return Math.max(longest, performance.now() - last)
    }
    // The shorter of two passes, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await longestGap(), await longestGap())).toBeLessThanOrEqual(50)
  }, 60_000)
  it('streams a roster of 100,000 members to a client a part at a time, with the join and leave landing between two parts each once', async () => {
    const authority = await Room.create('roster-parts')
    const members = await Promise.all(Array.from({ length: 100_000 }, () => authority.join()))
    const observer = (await Room.get(authority.id)) as ServerRoom
    await observer.getParticipants()
    const stub = register(observer)
    const joined = crypto.randomUUID()
    const [sent, unsent] = [members[0]!.id, members[99_999]!.id]
    const sendPublish = stub._sendPublish.bind(stub)
    let parts = 0
    // Not a vi.spyOn(): vitest keeps every spy, and so this room, until the file ends.
    stub._sendPublish = (wireText, resentOnAttach) => {
      sendPublish(wireText, resentOnAttach)
      // As the control lane delivers them, between two parts: a join, and the leaves of a member already sent and of
      // one not sent yet.
      if (wireText.includes('\n{"__r":"roster",') && ++parts === 50) {
        const ctrl = (event: object) => observer._onCtrlMessage(stringify(event), { seq: 0, timestamp: 1 })
        ctrl({ __r: 'join', id: joined, meta: {}, joinedAt: 1 })
        ctrl({ __r: 'leave', id: sent })
        ctrl({ __r: 'leave', id: unsent })
      }
    }
    const rosterEnds = (frames: ReturnType<Peer['decoded']>) =>
      frames.some(
        (frame) =>
          frame.tag === TAG.PUBLISH &&
          /^\{"__r":"roster","members":/.test(frame.text) &&
          !/"more":true\}$/.test(frame.text),
      )
    const serverGap = await longestGapWhile(async () => {
      const peer = attachPeer(stub)
      await vi.waitFor(() => expect(rosterEnds(peer.decoded())).toBe(true), { timeout: 30_000 })
      return peer
    })
    const { client, emit } = fakeClient(authority.id)
    const narrated: string[] = []
    client.onJoin((member) => narrated.push(`join ${member.id}`))
    client.onLeave((member) => narrated.push(`leave ${member.id}`))
    const clientGap = await longestGapWhile(async () => {
      // A frame per turn, as a transport delivers them.
      for (const frame of serverGap.value.decoded()) {
        if (frame.tag !== TAG.PUBLISH) continue
        emit(parse(frame.text), frame.info.seq)
        await new Promise((resolve) => setImmediate(resolve))
      }
      await client.getParticipant(joined)
    })
    expect(serverGap.gap).toBeLessThanOrEqual(50)
    expect(clientGap.gap).toBeLessThanOrEqual(50)
    // Before its first roster the client knows neither leaver, so only the join is narrated, once, though a later part
    // lists the member too.
    expect(narrated).toEqual([`join ${joined}`])
    const ids = (await client.getParticipants()).map(({ id }) => id)
    expect(new Set(ids).size).toBe(ids.length)
    expect(ids.sort()).toEqual((await observer.getParticipants()).map(({ id }) => id).sort())
    expect(ids).toHaveLength(99_999)
  }, 60_000)
  it("drops the 409,600 track wants of 100 clients whose reconnect windows end in one timer turn with the event loop turning, the publisher's lanes released", async () => {
    const room = await Room.create('mass-expiry')
    const publisher = await room.join()
    const observer = (await Room.get(room.id)) as ServerRoom
    await observer.getParticipant(publisher.id)
    const held = Array.from({ length: 100 }, () => {
      const stub = register(observer)
      const { peer } = attachPeer(stub)
      for (let i = 0; i < ROOM_TRACK_WANTS_MAX; i++)
        stub._onPeerMessage(stringify({ __r: 'sub-binary', member: publisher.id, track: `t${i}`, on: true }), 0)
      return { stub, peer }
    })
    await subsOf(observer).binaryReady()
    expect(binaryLanesOf(observer)).toBeGreaterThan(0)
    // Each declaration replays the retained frames of its track, so the specs' declarations in one turn leave work
    // behind them: wait it out.
    for (let turn = performance.now(); ; turn = performance.now()) {
      await new Promise((resolve) => setTimeout(resolve, 20))
      if (performance.now() - turn < 30) break
    }
    const { gap } = await longestGapWhile(async () => {
      const closed = held.map(({ stub }) => new Promise<void>((resolve) => stub.onClose(() => resolve())))
      for (const { stub, peer } of held) stub._onPeerDisconnect(peer, 100)
      await Promise.all(closed)
      await subsOf(observer).binaryReady()
    })
    expect(gap).toBeLessThanOrEqual(50)
    expect(binaryLanesOf(observer)).toBe(0)
  }, 60_000)
  it('merges one key into a room meta of 100,000 keys with both event loops turning, a view and a client applying it', async () => {
    const room = await Room.create('meta-merge-turns')
    for (let from = 0; from < 100_000; from += 10_000)
      await Room.setAttributes(
        room.id,
        Object.fromEntries(Array.from({ length: 10_000 }, (_, i) => [`k${from + i}`, i])),
      )
    const observer = (await Room.get(room.id)) as ServerRoom
    observer.onUpdate(() => {})
    const peer = attachPeer(register(observer))
    await vi.waitFor(() => expect(peer.decoded().length).toBeGreaterThan(0))
    const { client, emit } = fakeClient(room.id)
    client.onUpdate(() => {})
    let fed = 0
    const feed = () => {
      for (const frame of peer.decoded().slice(fed))
        if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
      fed = peer.decoded().length
    }
    feed()
    expect(Object.keys(client.meta)).toHaveLength(100_000)
    const merge = async (value: number) => {
      const server = await longestGapWhile(async () => {
        await Room.setAttributes(room.id, { k0: value })
        await vi.waitFor(() => expect(observer.meta.k0).toBe(value))
      })
      const { gap } = await longestGapWhile(async () => feed())
      expect(client.meta.k0).toBe(value)
      return Math.max(server.gap, gap)
    }
    await merge(-1)
    // The shorter of two merges, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await merge(1), await merge(2))).toBeLessThanOrEqual(50)
  }, 60_000)
})
/** How many binary lanes the room holds: per track and every-track. */
function binaryLanesOf(room: ServerRoom): number {
  const subs = (room as unknown as { _subs: { _binary: Map<string, unknown>; _everyTrack: Map<string, unknown> } })
    ._subs
  return subs._binary.size + subs._everyTrack.size
}
function subsOf(room: ServerRoom): {
  _heartbeatTick(): Promise<void>
  replan(): Promise<void>
  binaryReady(): Promise<void>
} {
  return (room as unknown as { _subs: ReturnType<typeof subsOf> })._subs
}
/** The longest event-loop gap while `run` runs, and what it returned. */
async function longestGapWhile<T>(run: () => Promise<T>): Promise<{ gap: number; value: T }> {
  let last = performance.now()
  let gap = 0
  const timer = setInterval(() => {
    const now = performance.now()
    gap = Math.max(gap, now - last)
    last = now
  }, 1)
  try {
    const value = await run()
    return { gap: Math.max(gap, performance.now() - last), value }
  } finally {
    clearInterval(timer)
  }
}
function register(room: ServerRoom): RoomStubChannel {
  const stub = new RoomStubChannel(room, { grants: new ResponseRoomGrants() })
  stub._registerChannel()
  room._attachStub(stub)
  return stub
}
function attachPeer(stub: RoomStubChannel) {
  const frames: Uint8Array[] = []
  const peer = new IndexedPeer(
    {
      send: (frame, onCommit) => {
        frames.push(frame)
        onCommit?.()
      },
      bufferedAmount: () => 0,
    },
    7,
    stub._replayBuffer!,
  )
  stub._attachPeer(peer)
  return { peer, decoded: () => frames.map((frame) => decode(frame as Uint8Array<ArrayBuffer>)) }
}
type Peer = ReturnType<typeof attachPeer>
/** A client Room whose stub the test feeds: `emit` delivers a text frame. */
function fakeClient(roomId: string) {
  const text: Array<(data: unknown, info: ChannelPublishInfo) => void> = []
  const stub = {
    _wire: { text: false, binary: false },
    _isClosed: false,
    _connection: { sendBroadcastSubscribe: () => {}, sendBroadcastUnsubscribe: () => {} },
    _subscribeLocal: (kind: 'text' | 'binary', callback: never) => {
      if (kind === 'text') text.push(callback)
      return () => {}
    },
    _setWireSubscribed: ClientBroadcast.prototype._setWireSubscribed,
    send: async () => undefined,
    onClose: () => {},
  } as unknown as ClientBroadcast
  const snapshot = {
    channelId: 'channel',
    roomId,
    meta: [],
    closed: false,
    count: 0,
    metaSeq: 0,
    bufferLimit: getServerConfig().channel.bufferLimit,
  }
  return {
    client: new ClientRoom(stub, snapshot),
    emit: (data: unknown, seq = 1) => text.forEach((callback) => callback(data, { key: roomId, seq, timestamp: seq })),
  }
}
