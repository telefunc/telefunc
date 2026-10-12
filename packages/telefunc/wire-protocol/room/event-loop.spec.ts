import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { getHeapStatistics, setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
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
import type { PresenceEvent } from './server/replica.js'
import { ResponseRoomGrants, RoomStubChannel } from './server/stub.js'
import { disposeBackend, getRoomBackend, installBackend } from '../backend/install.js'
import { MemoryBackend, MemoryBackendState } from '../backend/memory/backend.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_TRACK_WANTS_MAX } from './constants.js'

// Each spec here times a stall at 100,000 members, so it runs in a worker of its own: room.spec's other specs leave
// rooms reachable (vitest keeps every spy, and a stub no client connects to waits out its TTL), and collecting them
// lands in the timed turns.
beforeEach(async () => {
  await disposeBackend()
  const driver = new MemoryBackend({ state: new MemoryBackendState() })
  installBackend(() => driver)
})
// A spec's stubs and rooms end after it, and what ending them sets off finishes before the next spec: a closed room's
// planning pass releases its lanes a slice at a time, and a stub holds its room until it ends.
afterEach(async () => {
  for (const stub of opened.stubs.splice(0)) stub.abort()
  const rooms = opened.rooms.splice(0)
  for (const id of new Set(rooms.map((room) => room.id))) await Room.close(id)
  // A view no lane tells of the close learns it as its next heartbeat would.
  await Promise.all(rooms.map((room) => syncOf(room).reconcileAuthority()))
  await Promise.all(rooms.map((room) => subsOf(room).binaryReady()))
  await settled()
  await disposeBackend()
})
describe('Room at 100,000 members', () => {
  it('keeps the event loop turning through a heartbeat tick renewing and reading 100,000 owned members', async () => {
    const room = await create('heartbeat-turns')
    await Promise.all(Array.from({ length: 100_000 }, () => room.join()))
    const longestGap = async () => {
      let last = performance.now()
      let longest = 0
      const timer = setInterval(() => {
        const now = performance.now()
        longest = Math.max(longest, now - last)
        last = now
      }, 1)
      await syncOf(room)._heartbeatTick()
      clearInterval(timer)
      return Math.max(longest, performance.now() - last)
    }
    // The shorter of two ticks, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await longestGap(), await longestGap())).toBeLessThanOrEqual(50)
  }, 60_000)
  it('keeps the event loop turning through a replan of 100,000 members', async () => {
    const room = await create('replan-turns')
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
  it('replans 100,000 members no holder wants binary of, allocating at most 700 bytes each', async () => {
    const room = await create('replan-allocation')
    await Promise.all(Array.from({ length: 100_000 }, () => room.join()))
    await subsOf(room).replan()
    const before = allocatedBytes()
    await subsOf(room).replan()
    // About 450 measured, the inbox renewal included; 1,400 when every member's plan walked its empty want lists.
    expect((allocatedBytes() - before) / 100_000).toBeLessThanOrEqual(700)
  }, 60_000)
  it('streams a roster of 100,000 members to a client a part at a time, with the join and leave landing between two parts each once', async () => {
    const authority = await create('roster-parts')
    const members = await Promise.all(Array.from({ length: 100_000 }, () => authority.join()))
    const observer = await view(authority.id)
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
        const ctrl = (event: object) =>
          observer._onCtrlEvent(event as PresenceEvent, stringify(event), { seq: 0, timestamp: 1 })
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
  it('streams an unchanged roster of 100,000 members to 200 clients attaching one after another, each reusing the text one built', async () => {
    const room = await create('serial-rosters')
    await Promise.all(Array.from({ length: 100_000 }, () => room.join()))
    const attach = async () => {
      const before = allocatedBytes()
      const stub = register(room)
      attachPeer(stub, { keep: false })
      await vi.waitFor(() => expect(stub._rosterSent).toBe(true), { timeout: 30_000, interval: 1 })
      return allocatedBytes() - before
    }
    const built = await attach()
    const allocated: number[] = []
    for (let i = 0; i < 200; i++) allocated.push(await attach())
    const median = (bytes: number[]) => bytes.toSorted((a, b) => a - b)[bytes.length >> 1]!
    // About 330 MB built the text, most of it the serializer's; about 3 MB each after.
    expect(median(allocated)).toBeLessThan(built / 10)
    expect(median(allocated.slice(100))).toBeLessThanOrEqual(median(allocated.slice(0, 100)) * 1.25)
  }, 120_000)
  it('holds at most 5,500 bytes per owned member, measured over 20,000 joins after a full collection', async () => {
    const room = await create('member-footprint')
    await room.join()
    const live = async () => {
      for (let i = 0; i < 4; i++) {
        collectGarbage()
        await new Promise((resolve) => setTimeout(resolve, 10))
      }
      return process.memoryUsage().heapUsed
    }
    const before = await live()
    const members = await Promise.all(Array.from({ length: 20_000 }, () => room.join()))
    const perMember = ((await live()) - before) / members.length
    // About 4,300 measured, most of it the member's inbox lane subscription; 7,700 before its listener lists and
    // participant view were made on demand.
    expect(perMember).toBeLessThanOrEqual(5_500)
    expect(members).toHaveLength(20_000)
  }, 60_000)
  it('reads no page of a room meta of 100,000 keys in a heartbeat tick while the view holds every write', async () => {
    const room = await create('meta-steady-tick')
    for (let from = 0; from < 100_000; from += 10_000)
      await Room.setAttributes(
        room.id,
        Object.fromEntries(Array.from({ length: 10_000 }, (_, i) => [`k${from + i}`, i])),
      )
    const observer = await view(room.id)
    observer.onUpdate(() => {})
    await syncOf(observer)._heartbeatTick()
    expect(Object.keys(observer.meta)).toHaveLength(100_000)
    const backend = getRoomBackend()
    const { readCells, readCellPages } = backend
    const prefixes: string[] = []
    backend.readCells = (roomId, inc, selector) => {
      if ('prefix' in selector) prefixes.push(selector.prefix)
      return readCells.call(backend, roomId, inc, selector)
    }
    backend.readCellPages = (roomId, inc, prefix) => {
      prefixes.push(prefix)
      return readCellPages.call(backend, roomId, inc, prefix)
    }
    try {
      await syncOf(observer)._heartbeatTick()
    } finally {
      Object.assign(backend, { readCells, readCellPages })
    }
    // A replace's drop marks are read each tick; they hold no key.
    expect(prefixes.filter((prefix) => prefix.startsWith('meta:') && !prefix.startsWith('meta:drop:'))).toEqual([])
  }, 60_000)
  it("drops the 409,600 track wants of 100 clients whose reconnect windows end in one timer turn with the event loop turning, the publisher's lanes released", async () => {
    const room = await create('mass-expiry')
    const publisher = await room.join()
    const observer = await view(room.id)
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
    await settled()
    const { gap } = await longestGapWhile(async () => {
      const closed = held.map(({ stub }) => new Promise<void>((resolve) => stub.onClose(() => resolve())))
      for (const { stub, peer } of held) stub._onPeerDisconnect(peer, 100)
      await Promise.all(closed)
      await subsOf(observer).binaryReady()
    })
    expect(gap).toBeLessThanOrEqual(50)
    expect(binaryLanesOf(observer)).toBe(0)
  }, 60_000)
  it('drops the text and every-track wants of 100,000 members a client held as it leaves, with the event loop turning', async () => {
    const room = await create('holder-leaves')
    const ids = (await Promise.all(Array.from({ length: 100_000 }, () => room.join()))).map(({ id }) => id)
    const observer = await view(room.id)
    await observer.getParticipants()
    const stub = register(observer)
    const { peer } = attachPeer(stub, { keep: false })
    // One member per message, as the client sends them.
    for (const id of ids) {
      stub._onPeerMessage(stringify({ __r: 'sub-text', announce: false, members: { [id]: true } }), 0)
      stub._onPeerMessage(stringify({ __r: 'sub-binary', member: id, track: null, on: true }), 0)
    }
    await subsOf(observer).binaryReady()
    expect(binaryLanesOf(observer)).toBe(100_000)
    await settled()
    const { gap } = await longestGapWhile(async () => {
      const closed = new Promise<void>((resolve) => stub.onClose(() => resolve()))
      stub._onPeerDisconnect(peer, 1)
      await closed
      await subsOf(observer).binaryReady()
    })
    expect(gap).toBeLessThanOrEqual(50)
    expect(binaryLanesOf(observer)).toBe(0)
  }, 120_000)
  it('renews the demand of a client hearing every track of 100,000 members in a heartbeat, with the event loop turning', async () => {
    const room = await create('demand-heartbeat-turns')
    const ids = (await Promise.all(Array.from({ length: 100_000 }, () => room.join()))).map(({ id }) => id)
    const observer = await view(room.id)
    await observer.getParticipants()
    const stub = register(observer)
    attachPeer(stub, { keep: false })
    for (const id of ids) stub._onPeerMessage(stringify({ __r: 'sub-binary', member: id, track: null, on: true }), 0)
    await subsOf(observer).binaryReady()
    await settled()
    const backend = getRoomBackend()
    const commitLane = backend.commitLane
    let renewed = 0
    let inFlight = 0
    backend.commitLane = (id, inc, lane, payload, opts) => {
      const committed = commitLane.call(backend, id, inc, lane, payload, opts)
      if (id !== room.id || lane.kind !== 'control') return committed
      renewed++
      inFlight++
      void committed.finally(() => inFlight--).catch(() => {})
      return committed
    }
    /** No control commit for five timer turns in a row: what the joins and the declarations told is sent. */
    const quiet = async () => {
      for (let still = 0, last = renewed; still < 5; last = renewed) {
        await new Promise((resolve) => setTimeout(resolve, 20))
        still = renewed === last && inFlight === 0 ? still + 1 : 0
      }
    }
    const renewal = async () => {
      await quiet()
      const before = renewed
      const { gap } = await longestGapWhile(async () => {
        // The server's demand heartbeat, which re-tells each of its 100,000 pairs.
        await (observer._replica as unknown as { _heartbeatTick(): Promise<void> })._heartbeatTick()
        await vi.waitFor(() => expect(renewed - before >= 100_000 && inFlight === 0).toBe(true), { timeout: 30_000 })
        await settled()
      })
      return gap
    }
    try {
      // The shorter of two heartbeats, so a scheduling hiccup in one is not read as a stall.
      expect(Math.min(await renewal(), await renewal())).toBeLessThanOrEqual(50)
    } finally {
      backend.commitLane = commitLane
    }
  }, 120_000)
  it('fires every heartbeat of a room of 100,000 members at once, a client hearing every track of each, with the event loop turning', async () => {
    const heartbeats = new Map<unknown, () => void>()
    const { setInterval, clearInterval } = globalThis
    // Not a vi.spyOn(): vitest keeps every spy, and so this room, until the file ends.
    globalThis.setInterval = ((callback: () => void, ms?: number) => {
      const timer = setInterval(callback, ms)
      if (ms === ROOM_HEARTBEAT_INTERVAL_MS) heartbeats.set(timer, callback)
      return timer
    }) as typeof globalThis.setInterval
    globalThis.clearInterval = ((timer: ReturnType<typeof setInterval>) => {
      heartbeats.delete(timer)
      clearInterval(timer)
    }) as typeof globalThis.clearInterval
    let room: ServerRoom
    try {
      room = await create('every-heartbeat-turns')
      const ids = (await Promise.all(Array.from({ length: 100_000 }, () => room.join()))).map(({ id }) => id)
      const observer = await view(room.id)
      await observer.getParticipants()
      const stub = register(observer)
      attachPeer(stub, { keep: false })
      for (const id of ids) stub._onPeerMessage(stringify({ __r: 'sub-binary', member: id, track: null, on: true }), 0)
      await subsOf(observer).binaryReady()
      await settled()
    } finally {
      globalThis.setInterval = setInterval
      globalThis.clearInterval = clearInterval
    }
    const backend = getRoomBackend()
    const commitLane = backend.commitLane
    let renewed = 0
    let inFlight = 0
    backend.commitLane = (id, inc, lane, payload, opts) => {
      const committed = commitLane.call(backend, id, inc, lane, payload, opts)
      if (id !== room.id || lane.kind !== 'control') return committed
      renewed++
      inFlight++
      void committed.finally(() => inFlight--).catch(() => {})
      return committed
    }
    const all = async () => {
      const before = renewed
      const { gap } = await longestGapWhile(async () => {
        // Every timer the room's handles and its replica run each heartbeat, in one turn.
        for (const heartbeat of heartbeats.values()) heartbeat()
        await vi.waitFor(() => expect(renewed - before >= 100_000 && inFlight === 0).toBe(true), { timeout: 30_000 })
        await settled()
      })
      return gap
    }
    try {
      // The shorter of two heartbeats, so a scheduling hiccup in one is not read as a stall.
      expect(Math.min(await all(), await all())).toBeLessThanOrEqual(50)
    } finally {
      backend.commitLane = commitLane
    }
  }, 120_000)
  it('keeps the event loop turning through a sweep of 100,000 lapsed rooms', async () => {
    const backend = getRoomBackend()
    const sweep = async (pass: number) => {
      for (let i = 0; i < 100_000; i++) await backend.directoryPut(`lapsed-${pass}-${i}`, 'inc', 0)
      // Past this process's last sweep, so the room's creation sweeps.
      vi.setSystemTime(Date.now() + 60_000)
      return await longestGapWhile(async () => {
        await create(`after-lapses-${pass}`)
        await vi.waitFor(async () => expect(await backend.directoryDue(Date.now())).toEqual([]), { timeout: 30_000 })
      })
    }
    vi.useFakeTimers({ toFake: ['Date'] })
    try {
      // The shorter of two sweeps, so a scheduling hiccup in one is not read as a stall.
      expect(Math.min((await sweep(1)).gap, (await sweep(2)).gap)).toBeLessThanOrEqual(50)
    } finally {
      vi.useRealTimers()
    }
  }, 60_000)
  it('keeps the event loop turning through a heartbeat replan of one member with 100,000 named tracks', async () => {
    const room = await create('member-replan-turns')
    const publisher = await room.join()
    const observer = await view(room.id)
    await observer.getParticipant(publisher.id)
    for (let c = 0; c < 1_000; c++) {
      const stub = register(observer)
      attachPeer(stub)
      for (let i = 0; i < 100; i++)
        stub._onPeerMessage(stringify({ __r: 'sub-binary', member: publisher.id, track: `c${c}t${i}`, on: true }), 0)
    }
    await subsOf(observer).binaryReady()
    await settled()
    const replan = async () => (await longestGapWhile(() => subsOf(observer).replan())).gap
    // The shorter of two passes, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await replan(), await replan())).toBeLessThanOrEqual(50)
  }, 60_000)
  it('merges one key into a room meta of 100,000 keys with both event loops turning, a view and a client applying it', async () => {
    const room = await create('meta-merge-turns')
    for (let from = 0; from < 100_000; from += 10_000)
      await Room.setAttributes(
        room.id,
        Object.fromEntries(Array.from({ length: 10_000 }, (_, i) => [`k${from + i}`, i])),
      )
    const observer = await view(room.id)
    const peer = attachPeer(register(observer))
    await vi.waitFor(() => expect(peer.decoded().length).toBeGreaterThan(0))
    const { client, emit } = fakeClient(room.id)
    let fed = 0
    const feed = () => {
      for (const frame of peer.decoded().slice(fed))
        if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
      fed = peer.decoded().length
    }
    feed()
    const changes = { server: 0, client: 0 }
    observer.onChange(() => changes.server++)
    client.onChange(() => changes.client++)
    // Timed until each view applied the write. Reading the whole meta builds it, the reader's own cost (an onUpdate
    // listener's too), so it is read once at the end, and its garbage lands in no timed merge.
    const merge = async (value: number) => {
      const { server, client } = changes
      const written = await longestGapWhile(async () => {
        await Room.setAttributes(room.id, { k0: value })
        await vi.waitFor(() => expect(changes.server).toBe(server + 1))
      })
      const { gap } = await longestGapWhile(async () => feed())
      expect(changes.client).toBe(client + 1)
      return Math.max(written.gap, gap)
    }
    await merge(-1)
    // The shorter of two merges, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await merge(1), await merge(2))).toBeLessThanOrEqual(50)
    expect([observer.meta.k0, client.meta.k0, Object.keys(client.meta).length]).toEqual([2, 2, 100_000])
  }, 60_000)
  it("merges one key into a member's meta of 100,000 keys with both event loops turning, a view and a client applying it", async () => {
    const room = await create('member-meta-merge-turns')
    const meta = Object.fromEntries(Array.from({ length: 100_000 }, (_, i) => [`k${i}`, i]))
    const member = await room.join({ meta })
    const observer = await view(room.id)
    observer.onJoin(() => {})
    const peer = attachPeer(register(observer))
    await vi.waitFor(() => expect(observer._model.hasMember(member.id)).toBe(true))
    const { client, emit } = fakeClient(room.id)
    client.onJoin(() => {})
    let fed = 0
    const feed = () => {
      for (const frame of peer.decoded().slice(fed))
        if (frame.tag === TAG.PUBLISH) emit(parse(frame.text), frame.info.seq)
      fed = peer.decoded().length
    }
    await vi.waitFor(() => {
      feed()
      expect(client.snapshot().participants).toHaveLength(1)
    })
    const remote = (await observer.getParticipant(member.id))!
    const seen = (await client.getParticipant(member.id))!
    const changes = { server: 0, client: 0 }
    observer.onChange(() => changes.server++)
    client.onChange(() => changes.client++)
    // Timed until each view applied the write. Reading the whole meta builds it, the reader's own cost, so it is read once
    // at the end, and its garbage lands in no timed merge.
    const merge = async (value: number) => {
      const { server, client } = changes
      const written = await longestGapWhile(async () => {
        await member.setAttributes({ k0: value })
        await vi.waitFor(() => expect(changes.server).toBe(server + 1))
      })
      const { gap } = await longestGapWhile(async () => feed())
      expect(changes.client).toBe(client + 1)
      return Math.max(written.gap, gap)
    }
    await merge(-1)
    // The shorter of two merges, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await merge(1), await merge(2))).toBeLessThanOrEqual(50)
    expect([remote.meta.k0, seen.meta.k0]).toEqual([2, 2])
  }, 60_000)
  it("reads a member's meta of 100,000 keys into a fresh view with the event loop turning", async () => {
    const room = await create('member-meta-read-turns')
    const member = await room.join({
      meta: Object.fromEntries(Array.from({ length: 100_000 }, (_, i) => [`k${i}`, i])),
    })
    const read = async () => {
      const { gap, value } = await longestGapWhile(async () => (await view(room.id)).getParticipant(member.id))
      expect([Object.keys(value!.meta).length, value!.meta.k99999]).toEqual([100_000, 99_999])
      return gap
    }
    // The shorter of two reads, so a scheduling hiccup in one is not read as a stall.
    expect(Math.min(await read(), await read())).toBeLessThanOrEqual(50)
  }, 60_000)
})
/** How many binary lanes the room holds: per track and every-track. */
function binaryLanesOf(room: ServerRoom): number {
  const subs = (room as unknown as { _subs: { _binary: Map<string, unknown>; _everyTrack: Map<string, unknown> } })
    ._subs
  return subs._binary.size + subs._everyTrack.size
}
/** Each declaration replays the retained frames of its track, so a spec's declarations in one turn leave work behind
 *  them: waits it out, until five timers in a row come on time. */
async function settled(): Promise<void> {
  for (let onTime = 0; onTime < 5; ) {
    const turn = performance.now()
    await new Promise((resolve) => setTimeout(resolve, 20))
    onTime = performance.now() - turn < 30 ? onTime + 1 : 0
  }
}
/** The bytes V8 allocated so far; this @types/node predates `total_allocated_bytes`. */
function allocatedBytes(): number {
  return (getHeapStatistics() as ReturnType<typeof getHeapStatistics> & { total_allocated_bytes: number })
    .total_allocated_bytes
}
/** A full collection, as `node --expose-gc` gives it. */
function collectGarbage(): void {
  setFlagsFromString('--expose-gc')
  ;(runInNewContext('gc') as () => void)()
}
function subsOf(room: ServerRoom): {
  replan(): Promise<void>
  binaryReady(): Promise<void>
} {
  return (room as unknown as { _subs: ReturnType<typeof subsOf> })._subs
}
function syncOf(room: ServerRoom): {
  _heartbeatTick(): Promise<void>
  reconcileAuthority(): Promise<void>
} {
  return (room as unknown as { _sync: ReturnType<typeof syncOf> })._sync
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
const opened = { rooms: [] as ServerRoom[], stubs: [] as RoomStubChannel[] }
async function create(id: string): Promise<ServerRoom> {
  const room = (await Room.create(id)) as ServerRoom
  opened.rooms.push(room)
  return room
}
async function view(id: string): Promise<ServerRoom> {
  const room = (await Room.get(id)) as ServerRoom
  opened.rooms.push(room)
  return room
}
function register(room: ServerRoom): RoomStubChannel {
  const stub = new RoomStubChannel(room, { grants: new ResponseRoomGrants() })
  opened.stubs.push(stub)
  stub._registerChannel()
  room._attachStub(stub)
  return stub
}
/** `keep: false` drops the frames sent. */
function attachPeer(stub: RoomStubChannel, { keep = true } = {}) {
  const frames: Uint8Array[] = []
  const peer = new IndexedPeer(
    {
      send: (frame, onCommit) => {
        if (keep) frames.push(frame)
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
