import { afterEach, describe, expect, it, vi } from 'vitest'
import { pageRoom } from '../test/pageRoom.js'
import { Room } from '../index.js'
import { ClientParticipant } from '../client/ClientParticipant.js'
import { RoomDemand } from '../server/lanes/RoomDemand.js'
import { ServerLocalParticipant } from '../server/ServerLocalParticipant.js'
import { RoomReplica } from '../server/RoomReplica.js'
import { RoomModel } from './RoomModel.js'
import type { ParticipantBase } from './ParticipantBase.js'
import { assert } from '../utils/assert.js'
import type { TrackWatch } from './protocol.js'
import type { LocalParticipant, RemoteParticipant, RoomSnapshotView, TrackDemand } from '../types.js'

/** `me`, joined to a page's room of `members`, with what ends its wire and what hands it an event. */
async function joinedIn(members: Record<string, number>) {
  const page = pageRoom(members)
  const me = await page.room.join()
  assert(me instanceof ClientParticipant)
  return { ...page, me }
}

async function joinedAmong(members: Record<string, number>): Promise<ClientParticipant> {
  return (await joinedIn(members)).me
}

describe("a participant's demand", () => {
  it('keeps the tracks it showed when another track came and went between two reads', async () => {
    const me = await joinedAmong({ sam: 1 })
    me.onChange(() => {})
    me._applyDemand('a', { add: ['sam'] })
    const before = me.demand.map((entry) => entry.track)
    me._applyDemand('0', { add: ['sam'] })
    await Promise.resolve()
    me._applyDemand('0', { remove: ['sam'] })
    await Promise.resolve()
    expect({ before, after: me.demand.map((entry) => entry.track) }).toEqual({ before: ['a'], after: ['a'] })
  })

  it('shows a watcher once the room shows them', async () => {
    const { me, deliver } = await joinedIn({ sam: 1 })
    me.onChange(() => {})
    me._applyDemand('a', { add: ['tia'] })
    const before = me.demand.map((entry) => ({
      watchers: entry.watchers.map((watcher) => watcher.id),
      count: entry.watcherCount,
    }))
    deliver({ __r: 'join', id: 'tia', meta: {}, joinedAt: 2 })
    await Promise.resolve()
    const after = me.demand.map((entry) => ({
      watchers: entry.watchers.map((watcher) => watcher.id),
      count: entry.watcherCount,
    }))
    expect({ before, after }).toEqual({
      before: [{ watchers: [], count: 1 }],
      after: [{ watchers: ['tia'], count: 1 }],
    })
  })

  it('after a whole-state reset, lists only the tracks the reset names', async () => {
    const me = await joinedAmong({ sam: 1 })
    me.onChange(() => {})
    me._applyDemand('a', { add: ['sam'] })
    const before = me.demand.map((entry) => entry.track)
    me._applyDemandState([{ track: 'b', watchers: ['sam'], pages: [] }])
    expect({ before, after: me.demand.map((entry) => entry.track) }).toEqual({ before: ['a'], after: ['b'] })
  })
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe("a participant's demand, as its room shows its watchers", () => {
  const settle = async () => {
    for (let turn = 0; turn < 8; turn++) await Promise.resolve()
  }
  /** The `meta.step` of each track's first watcher, in track order. */
  const watcherSteps = (me: LocalParticipant) => me.demand.map((entry) => entry.watchers[0]?.meta.step)

  function onServer(me: LocalParticipant): ServerLocalParticipant {
    if (!ServerLocalParticipant.isServerLocalParticipant(me)) throw new Error('Expected a server-side member')
    return me
  }
  /** The demand `me`'s replica takes from other processes. */
  function incomingOf(me: ServerLocalParticipant): RoomDemand {
    const incoming: unknown = Reflect.get(me.room.replica, 'incomingDemand')
    if (!(incoming instanceof RoomDemand)) throw new Error("Expected the replica's demand")
    return incoming
  }
  /** A server-side room, its member `me`, and the demand its replica takes from other processes. */
  async function serverMember() {
    const room = await Room.create(`demand-${crypto.randomUUID()}`)
    const me = onServer(await room.join())
    return { room, me, incoming: incomingOf(me), model: me.room.model }
  }
  /** Another process's page receives `track` of `member` for `by`, or for nobody it names, or stops. */
  function report(incoming: RoomDemand, member: string, track: string, by: string | undefined, on: boolean) {
    const instance = `other-${by ?? 'anonymous'}`
    incoming.take(instance, { member, track }, on)
    incoming.take(instance, { member, track, by, page: instance }, on)
  }

  it('shows a watcher changed in the same task to a read and to its listener, which hears it once', async () => {
    const { me, deliver } = await joinedIn({ sam: 1 })
    const heard: unknown[] = []
    me.onChange(() => heard.push(watcherSteps(me)))
    deliver({ __r: 'demand', member: me.id, track: 'a', add: ['sam'] })
    await settle()
    heard.length = 0
    deliver({ __r: 'demand', member: me.id, track: 'b', add: ['sam'] })
    deliver({ __r: 'p-meta', id: 'sam', seq: 1, meta: { step: 1 } })
    const read = watcherSteps(me)
    await settle()
    expect({ read, heard }).toEqual({ read: [1, 1], heard: [[1, 1]] })
  })

  it("on the server, shows a watcher's change to the watcher's own listener", async () => {
    const { room, me, incoming } = await serverMember()
    const watcher = await room.join()
    const remote = room.getParticipant(watcher.id)
    if (remote === null) throw new Error('Expected the watcher in the roster')
    me.onChange(() => {})
    report(incoming, me.id, 'a', watcher.id, true)
    await settle()
    const before = watcherSteps(me)
    const heard: unknown[] = []
    remote.onChange(() => heard.push(watcherSteps(me)))
    await watcher.setMeta({ step: 1 })
    const after = watcherSteps(me)
    await Room.close(room.id)
    expect({ before, heard, after }).toEqual({ before: [undefined], heard: [[1]], after: [1] })
  })

  it("on the server, shows a watcher as disconnected to the watcher's own disconnect listener", async () => {
    const { room, me, incoming } = await serverMember()
    const watcher = onServer(await room.join())
    report(incoming, me.id, 'a', watcher.id, true)
    await settle()
    const connected = () => me.demand[0]?.watchers[0]?.isConnected
    const before = connected()
    const heard: unknown[] = []
    watcher.onDisconnect(() => heard.push({ own: watcher.isConnected, demand: connected() }))
    await watcher.room.writes.setMemberConnected(watcher.id, false)
    const after = connected()
    await Room.close(room.id)
    expect({ before, heard, after }).toEqual({ before: true, heard: [{ own: false, demand: false }], after: false })
  })

  /** `watcher`'s room as a process that holds no replica of it sees it: its member `me` joined with a count alone, and
   *  the roster the row loads a step at a time, its own read of it held back. */
  async function joinedWithoutRoster(watcher: ServerLocalParticipant) {
    vi.spyOn(RoomModel.prototype, 'reconcileRoster').mockReturnValue(new Promise(() => {}))
    const replicas = vi.spyOn(RoomReplica, 'of').mockReturnValueOnce(undefined).mockReturnValueOnce(undefined)
    const me = onServer(await Room.join(watcher.room.model.roomId))
    replicas.mockRestore()
    return { me, model: me.room.model, remotes: me.room.remotes }
  }

  it('on a server that joined without its roster, shows a watcher the first roster brings while it loads', async () => {
    const room = await Room.create(`demand-${crypto.randomUUID()}`)
    const watcher = onServer(await room.join())
    const { me, model } = await joinedWithoutRoster(watcher)
    report(incomingOf(me), me.id, 'a', watcher.id, true)
    await settle()
    const watchers = () => me.demand[0]?.watchers.map(({ id }) => id)
    const before = { rosterKnown: model.rosterKnown, watchers: watchers() }
    const roster = {
      members: [watcher.room.model.memberSnapshot(watcher.id), model.memberSnapshot(me.id)],
      departing: new Map(),
    }
    const load = model.reconcilingRoster(roster, [])
    while (!model.hasMember(watcher.id) && load.next().done !== true) {}
    const loading = watchers()
    while (load.next().done !== true) {}
    const loaded = watchers()
    await Room.close(room.id)
    expect({ before, loading, loaded }).toEqual({
      before: { rosterKnown: false, watchers: [] },
      loading: [watcher.id],
      loaded: [watcher.id],
    })
  })

  it("on the server, shows a watcher as disconnected to its own disconnect listener in the room's snapshot too", async () => {
    const { room, me, incoming } = await serverMember()
    const watcher = onServer(await room.join())
    report(incoming, me.id, 'a', watcher.id, true)
    await settle()
    const held = room.snapshot()
    const shown = (snapshot: RoomSnapshotView) => snapshot.participants.find(({ id }) => id === watcher.id)?.isConnected
    const heard: unknown[] = []
    watcher.onDisconnect(() =>
      heard.push({
        snapshot: shown(room.snapshot()),
        participant: room.getParticipant(watcher.id)?.isConnected,
        demand: me.demand[0]?.watchers[0]?.isConnected,
      }),
    )
    await watcher.room.writes.setMemberConnected(watcher.id, false)
    await Room.close(room.id)
    expect({ before: shown(held), heard }).toEqual({
      before: true,
      heard: [{ snapshot: false, participant: false, demand: false }],
    })
  })

  it("on a server that joined without its roster, lists a watcher the first roster brings in the room's snapshot too", async () => {
    const room = await Room.create(`demand-${crypto.randomUUID()}`)
    const watcher = onServer(await room.join())
    const { me, model, remotes } = await joinedWithoutRoster(watcher)
    report(incomingOf(me), me.id, 'a', watcher.id, true)
    await settle()
    const held = remotes.snapshot()
    const listed = (snapshot: RoomSnapshotView) => snapshot.participants.some(({ id }) => id === watcher.id)
    const roster = {
      members: [watcher.room.model.memberSnapshot(watcher.id), model.memberSnapshot(me.id)],
      departing: new Map(),
    }
    const load = model.reconcilingRoster(roster, [])
    while (!model.hasMember(watcher.id) && load.next().done !== true) {}
    const loading = {
      snapshot: listed(remotes.snapshot()),
      participant: remotes.get(watcher.id)?.id,
      demand: me.demand[0]?.watchers.map(({ id }) => id),
    }
    while (load.next().done !== true) {}
    await Room.close(room.id)
    expect({ before: listed(held), loading }).toEqual({
      before: false,
      loading: { snapshot: true, participant: watcher.id, demand: [watcher.id] },
    })
  })

  it('on a server that joined without its roster, tells its room once that the first roster came, at its end', async () => {
    const room = await Room.create(`demand-${crypto.randomUUID()}`)
    const watcher = onServer(await room.join())
    const others = [onServer(await room.join()), onServer(await room.join())]
    const { me, model, remotes } = await joinedWithoutRoster(watcher)
    const heard: boolean[] = []
    remotes.state.onChange(() => heard.push(model.rosterKnown), undefined, remotes.adder)
    const members = [watcher, ...others].map((member) => member.room.model.memberSnapshot(member.id))
    const load = model.reconcilingRoster(
      { members: [...members, model.memberSnapshot(me.id)], departing: new Map() },
      [],
    )
    // Each step in a task of its own, so a notice a step queued is heard before the next.
    while (load.next().done !== true) await settle()
    await settle()
    const rosterKnown = model.rosterKnown
    await Room.close(room.id)
    expect({ rosterKnown, heard }).toEqual({ rosterKnown: true, heard: [true] })
  })

  const names = ['', 'A', 'aaa', 'z', 'alpha', 'a', 'Ω', 'é', '😀', 't0', 't2', 't10']
  const people = ['sam', 'tia', 'joe', 'lee']
  /** Indices below `options`, the same sequence for the same seed. */
  function random(seed: number) {
    let state = seed
    return (options: number) => {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0
      return (state >>> 8) % options
    }
  }
  function nth<T>(items: readonly T[], index: number): T {
    const item = items[index]
    if (item === undefined) throw new Error(`No item ${index}`)
    return item
  }
  const listed = (list: readonly Pick<TrackDemand, 'track' | 'watchers' | 'watcherCount'>[]) =>
    JSON.stringify(list.map(({ track, watchers, watcherCount }) => [track, watchers.map((w) => w.id), watcherCount]))
  /** A line where `me.demand` isn't each of its tracks with the participants `snapshot` lists among who receives it,
   *  the same objects in its order, and how many receive it; nothing once it left. */
  function disagreement(me: ParticipantBase, left: boolean, participants: readonly RemoteParticipant[], at: string) {
    const watches: ReadonlyMap<string, TrackWatch> = left ? new Map() : me._watches
    const expected = Array.from(watches, ([key, watch]) => ({
      track: key === '' ? 'main' : key,
      watchers: participants.filter((participant) => watch.watchers.has(participant.id)),
      watcherCount: watch.watchers.size + watch.pages.size,
    })).sort((a, b) => (a.track < b.track ? -1 : 1))
    const read = me.demand
    const same =
      read.length === expected.length &&
      read.every((entry, i) => {
        const want = expected[i]
        return (
          want !== undefined &&
          entry.track === want.track &&
          entry.watcherCount === want.watcherCount &&
          entry.watchers.length === want.watchers.length &&
          entry.watchers.every((watcher, j) => watcher === want.watchers[j])
        )
      })
    return same ? [] : [`${at}: read ${listed(read)}, the snapshot shows ${listed(expected)}`]
  }

  for (const seed of [0x436da6, 0x5eed02]) {
    it(`on the client, agrees with the room's snapshot at each read of 3,000 seeded changes (seed ${seed})`, async () => {
      const pick = random(seed)
      const wrong: string[] = []
      let page = await joinedIn({ sam: 1, tia: 2 })
      let left = false
      const check = (at: string) => wrong.push(...disagreement(page.me, left, page.room.snapshot().participants, at))
      const listen = () => page.me.onChange(() => check('listener'))
      const demand = (track: string, change: object) =>
        page.deliver({ __r: 'demand', member: page.me.id, track, ...change })
      let active = new Set(['sam', 'tia'])
      let metaSeqs = new Map<string, number>()
      let stopListening = listen()
      for (let step = 1; step <= 3_000; step++) {
        const op = pick(12)
        const who = nth(people, pick(people.length))
        const track = nth(names, pick(names.length))
        const metaSeq = (metaSeqs.get(who) ?? 0) + 1
        switch (op) {
          case 0:
            demand(track, { add: [who] })
            break
          case 1:
            demand(track, { remove: [who] })
            break
          case 2:
            demand(track, { pages: pick(2) ? { add: ['anon'] } : { remove: ['anon'] } })
            break
          case 3:
            demand(track, { add: [who], pages: { add: ['anon'] } })
            if (pick(2)) demand(track, { remove: [who], pages: { remove: ['anon'] } })
            break
          case 4:
            if (active.has(who)) break
            page.deliver({ __r: 'join', id: who, meta: { step }, joinedAt: step })
            active.add(who)
            metaSeqs.delete(who)
            break
          case 5:
            page.deliver({ __r: 'leave', id: who })
            active.delete(who)
            break
          case 6:
          case 7:
            if (!active.has(who)) break
            metaSeqs.set(who, metaSeq)
            page.deliver(
              op === 6
                ? { __r: 'p-meta', id: who, seq: metaSeq, meta: { step } }
                : { __r: 'p-meta', id: who, seq: metaSeq, connected: pick(2) === 0 },
            )
            break
          case 8:
            page.deliver({
              __r: 'demand-state',
              member: page.me.id,
              tracks: names.filter(() => pick(3) === 0).map((key) => ({ track: key, watchers: [who], pages: [] })),
            })
            break
          case 9: {
            const members = people
              .filter(() => pick(3) !== 0)
              .map((id) => {
                const seq = (metaSeqs.get(id) ?? 0) + 1
                metaSeqs.set(id, seq)
                return { id, joinedAt: step, meta: { step }, metaSeq: seq, identity: null }
              })
            active = new Set(members.map(({ id }) => id))
            const me = { id: page.me.id, joinedAt: 0, meta: {}, metaSeq: 0, identity: null }
            page.deliver({ __r: 'roster', part: 0, members: [me, ...members] })
            break
          }
          case 10:
            stopListening()
            stopListening = pick(2) ? listen() : page.me.onChange(() => {})
            break
          case 11:
            if (pick(10) !== 0) break
            page.endWire()
            left = true
            check('left')
            page = await joinedIn({ sam: 1, tia: 2 })
            left = false
            active = new Set(['sam', 'tia'])
            metaSeqs = new Map()
            stopListening = pick(2) ? listen() : () => {}
            break
        }
        check(`step ${step} op ${op}`)
        await settle()
        check(`step ${step} op ${op}, settled`)
      }
      expect({ disagreements: wrong.length, first: wrong.slice(0, 3) }).toEqual({ disagreements: 0, first: [] })
    })

    it(`on the server, agrees with the room's snapshot at each read of 3,000 seeded changes (seed ${seed})`, async () => {
      const pick = random(seed)
      const wrong: string[] = []
      const { room, incoming, model } = await serverMember()
      const first = await room.join()
      if (!ServerLocalParticipant.isServerLocalParticipant(first)) throw new Error('Expected a server-side member')
      let me: ServerLocalParticipant = first
      let left = false
      const check = (at: string) => wrong.push(...disagreement(me, left, room.snapshot().participants, at))
      const listen = () => me.onChange(() => check('listener'))
      const watchers = new Map<string, LocalParticipant>()
      const ids = new Map<string, string>()
      for (const who of ['sam', 'tia']) {
        const watcher = await room.join({ meta: { who } })
        watchers.set(who, watcher)
        ids.set(who, watcher.id)
      }
      const metaSeqs = new Map<string, number>()
      let stopListening = listen()
      for (let step = 1; step <= 3_000; step++) {
        const op = pick(12)
        const who = nth(people, pick(people.length))
        const track = nth(names, pick(names.length))
        const by = ids.get(who) ?? `absent-${who}`
        const watcher = watchers.get(who)
        const metaSeq = (metaSeqs.get(by) ?? 0) + 1
        switch (op) {
          case 0:
            report(incoming, me.id, track, by, true)
            break
          case 1:
            report(incoming, me.id, track, by, false)
            break
          case 2:
            report(incoming, me.id, track, undefined, pick(2) === 0)
            break
          case 3:
            report(incoming, me.id, track, by, true)
            if (pick(2)) report(incoming, me.id, track, by, false)
            break
          case 4: {
            if (watcher) break
            const joined = await room.join({ meta: { who, step }, hidden: pick(4) === 0 })
            watchers.set(who, joined)
            ids.set(who, joined.id)
            break
          }
          case 5:
            if (!watcher) break
            if (pick(2)) await watcher.leave()
            else model.applyLeave(watcher.id, { type: 'left' })
            watchers.delete(who)
            break
          case 6:
          case 7:
            if (!watcher) break
            metaSeqs.set(by, metaSeq)
            model.applyParticipantMeta(
              by,
              op === 6 ? { seq: metaSeq, meta: { who, step } } : { seq: metaSeq, connected: pick(2) === 0 },
            )
            break
          case 8:
            for (const key of names) {
              for (const id of ids.values()) report(incoming, me.id, key, id, false)
              report(incoming, me.id, key, undefined, pick(3) === 0)
            }
            break
          case 9: {
            // The roster read again, a member at a time, with a read between each two.
            const members = [...model.entries()].map((entry) => model.memberSnapshot(entry.id))
            const roster = members.map((member) =>
              member.id === watcher?.id ? { ...member, meta: { who, step }, metaSeq } : member,
            )
            const steps = model.reconcilingRoster({ members: roster }, [])
            for (let next = steps.next(); next.done !== true; next = steps.next()) check(`step ${step} roster`)
            if (watcher) metaSeqs.set(by, metaSeq)
            break
          }
          case 10:
            stopListening()
            stopListening = pick(2) ? listen() : me.onChange(() => {})
            break
          case 11: {
            if (pick(10) !== 0) break
            await me.leave()
            left = true
            check('left')
            const next = await room.join()
            if (!ServerLocalParticipant.isServerLocalParticipant(next)) throw new Error('Expected a server-side member')
            me = next
            left = false
            report(incoming, me.id, '', by, true)
            stopListening = pick(2) ? listen() : () => {}
            break
          }
        }
        check(`step ${step} op ${op}`)
        await settle()
        check(`step ${step} op ${op}, settled`)
      }
      await Room.close(room.id)
      expect({ disagreements: wrong.length, first: wrong.slice(0, 3) }).toEqual({ disagreements: 0, first: [] })
    })
  }
})
