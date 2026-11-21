import { afterEach, describe, expect, it, vi } from 'vitest'
import { pageRoom } from '../test/pageRoom.js'
import { Room } from '../index.js'
import { RoomRemotes } from './RoomView.js'
import { ClientParticipant } from '../client/ClientParticipant.js'
import { RoomDemand } from '../server/lanes/RoomDemand.js'
import { ServerLocalParticipant } from '../server/ServerLocalParticipant.js'
import type { DemandChange, TrackWatch, TrackWatchState } from './protocol.js'
import type { LocalParticipant, RemoteParticipant, TrackDemand } from '../types.js'

/** A member and who receives each of its tracks, as the room told it. */
type DemandReader = LocalParticipant & { readonly _watches: ReadonlyMap<string, TrackWatch> }

/** How the page's room hands its member who receives each of its tracks. */
type DemandHolder = DemandReader & {
  _applyDemand(track: string, change: DemandChange): void
  _applyDemandState(tracks: TrackWatchState[]): void
}

/** The watchers of `me`'s default track, after the room reports `watchers` receiving it. */
function watchersReported(me: DemandHolder, watchers: string[]): string[] {
  me._applyDemandState([{ track: '', watchers, pages: [] }])
  return me.demand[0]!.watchers.map((watcher) => watcher.id)
}

/** `me`, joined to a page's room of `members`, with what ends its wire and what hands it an event. */
async function joinedIn(members: Record<string, number>) {
  const page = pageRoom(members)
  return { ...page, me: (await page.room.join()) as DemandHolder }
}

async function joinedAmong(members: Record<string, number>): Promise<DemandHolder> {
  return (await joinedIn(members)).me
}

describe("a track's watchers", () => {
  it('are listed in the order the room lists its participants, whatever their join times', async () => {
    const me = await joinedAmong({ sam: 2, tia: 1 })
    expect(watchersReported(me, ['tia', 'sam'])).toEqual(['sam', 'tia'])
  })

  it('who joined at the same time keep that order when the room reports them again in another', async () => {
    const me = await joinedAmong({ sam: 1, tia: 1 })
    expect(watchersReported(me, ['sam', 'tia'])).toEqual(['sam', 'tia'])
    expect(watchersReported(me, ['tia', 'sam'])).toEqual(['sam', 'tia'])
  })
})

describe("a participant's demand", () => {
  it('lists its tracks by name in code-unit order, whatever order the room told them in', async () => {
    const told = async (tracks: string[]) => {
      const me = await joinedAmong({ sam: 1 })
      for (const track of tracks) me._applyDemand(track, { add: ['sam'] })
      return me.demand.map((entry) => entry.track)
    }
    expect(await told(['mic', '', 'Screen'])).toEqual(['Screen', 'main', 'mic'])
    expect(await told(['Screen', 'mic', ''])).toEqual(['Screen', 'main', 'mic'])
  })

  it('reads who receives a track once per change of it, though a listener reads the whole list at each change', async () => {
    const me = await joinedAmong({ sam: 1 })
    me.onChange(() => void me.demand)
    const shownOf = vi.spyOn(RoomRemotes.prototype, 'shownOf')
    for (let i = 0; i < 1_000; i++) {
      me._applyDemand(`t${i}`, { add: ['sam'] })
      // Each change in a task of its own, as the room tells them.
      await Promise.resolve()
    }
    expect({ tracks: me.demand.length, reads: shownOf.mock.calls.length }).toEqual({ tracks: 1_000, reads: 1_000 })
  })

  it('reads none of its tracks while nothing reads its demand or listens', async () => {
    const { me, deliver } = await joinedIn({ sam: 1 })
    const watches = vi.spyOn(ClientParticipant.prototype, '_watches', 'get')
    for (let i = 0; i < 1_000; i++) deliver({ __r: 'demand', member: me.id, track: `t${i}`, add: ['sam'] })
    const reads = watches.mock.calls.length
    expect({ reads, tracks: me.demand.length }).toEqual({ reads: 0, tracks: 1_000 })
  })

  it('keeps an entry, and the list, the same object while nothing in them changes', async () => {
    const me = await joinedAmong({ sam: 1 })
    me.onChange(() => {})
    me._applyDemand('a', { add: ['sam'] })
    const before = me.demand
    me._applyDemand('b', { add: ['sam'] })
    const after = me.demand
    expect({
      list: after === before,
      a: after.find((entry) => entry.track === 'a') === before[0],
      again: me.demand === after,
    }).toEqual({ list: false, a: true, again: true })
  })

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

  for (const listener of ['reading', 'silent', 'none'] as const) {
    it(`keeps no more of its tracks than it has or last listed, and nothing once it left (listener: ${listener})`, async () => {
      const { me, deliver, endWire } = await joinedIn({ sam: 1 })
      if (listener === 'reading') me.onChange(() => void me.demand)
      if (listener === 'silent') me.onChange(() => {})
      const kept = () => {
        const sizes = ['demandEntries', 'staleTracks', 'listChanges'].map((field) => {
          const value: unknown = Reflect.get(me, field)
          if (!(value instanceof Map || value instanceof Set)) throw new Error(`Expected the ${field} it keeps`)
          return value.size
        })
        return sizes.reduce((sum, size) => sum + size, 0)
      }
      const settle = async () => {
        for (let turn = 0; turn < 8; turn++) await Promise.resolve()
      }
      // 512 tracks, each wanted and then not, one at a time: it has one at most, and lists none, so it keeps that
      // one's entry and one change of it waiting for a read at most.
      let most = 0
      for (let i = 0; i < 512; i++) {
        deliver({ __r: 'demand', member: me.id, track: `t${i}`, add: ['sam'] })
        await settle()
        most = Math.max(most, kept())
        deliver({ __r: 'demand', member: me.id, track: `t${i}`, remove: ['sam'] })
        await settle()
        most = Math.max(most, kept())
      }
      for (const track of ['a', 'b', 'c']) deliver({ __r: 'demand', member: me.id, track, add: ['sam'] })
      await settle()
      const listed = me.demand.length
      endWire()
      expect({ atMostTwo: most <= 2, listed, left: kept(), demand: me.demand }).toEqual({
        atMostTwo: true,
        listed: 3,
        left: 0,
        demand: [],
      })
    })
  }

  it('is empty once it left', async () => {
    const { me, endWire } = await joinedIn({ sam: 1 })
    me.onChange(() => {})
    me._applyDemand('a', { add: ['sam'] })
    expect(me.demand.length).toBe(1)
    endWire()
    expect(me.demand).toEqual([])
  })
})

describe("a server-side participant's demand", () => {
  /** A server-side room whose member `me` hears its changes, and a second view of it that subscribes to tracks. */
  async function serverRoom() {
    const id = `demand-${crypto.randomUUID()}`
    const room = await Room.create(id)
    const me = await room.join()
    me.onChange(() => void me.demand)
    const viewer = await Room.getOrCreate(id)
    return { me, viewer }
  }
  const until = async (done: () => boolean) => {
    for (let turn = 0; turn < 1_000 && !done(); turn++) await new Promise((resolve) => setTimeout(resolve, 0))
  }

  it('reads who receives a track once per change of it, though a listener reads the whole list at each change', async () => {
    const { me, viewer } = await serverRoom()
    const shownOf = vi.spyOn(RoomRemotes.prototype, 'shownOf')
    for (let i = 0; i < 1_000; i++) viewer.subscribeBinary(() => {}, { track: `t${i}` })
    await until(() => me.demand.length === 1_000)
    expect({ tracks: me.demand.length, reads: shownOf.mock.calls.length }).toEqual({ tracks: 1_000, reads: 1_000 })
  })

  it('keeps an entry, and the list, the same object while nothing in them changes, and is empty once it left', async () => {
    const { me, viewer } = await serverRoom()
    viewer.subscribeBinary(() => {}, { track: 'a' })
    await until(() => me.demand.length === 1)
    const before = me.demand
    viewer.subscribeBinary(() => {}, { track: 'b' })
    await until(() => me.demand.length === 2)
    const after = me.demand
    const kept = {
      list: after === before,
      a: after.find((entry) => entry.track === 'a') === before[0],
      again: me.demand === after,
    }
    await me.leave()
    expect({ kept, left: me.demand }).toEqual({ kept: { list: false, a: true, again: true }, left: [] })
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

  /** A server-side room, its member `me`, and the demand its replica takes from other processes. */
  async function serverMember() {
    const room = await Room.create(`demand-${crypto.randomUUID()}`)
    const me = await room.join()
    if (!ServerLocalParticipant.isServerLocalParticipant(me)) throw new Error('Expected a server-side member')
    const incoming: unknown = Reflect.get(me.room.replica, 'incomingDemand')
    if (!(incoming instanceof RoomDemand)) throw new Error("Expected the replica's demand")
    return { room, me, incoming, model: me.room.model }
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

  it('tells its listeners, once, when only a watcher it shows changed', async () => {
    const { me, deliver } = await joinedIn({ sam: 1 })
    me._applyDemand('a', { add: ['sam'] })
    const heard: unknown[] = []
    me.onChange(() => heard.push(watcherSteps(me)))
    deliver({ __r: 'p-meta', id: 'sam', seq: 1, meta: { step: 1 } })
    await settle()
    expect(heard).toEqual([[1]])
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
  function disagreement(me: DemandReader, left: boolean, participants: readonly RemoteParticipant[], at: string) {
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

describe("a participant's leave listeners", () => {
  it('hear that it left, all but one that an earlier one removed', async () => {
    const { room, endWire } = pageRoom()
    const me = await room.join()
    const heard: string[] = []
    me.onLeave(() => removeSecond())
    const removeSecond = me.onLeave(() => heard.push('second'))
    me.onLeave(() => heard.push('third'))

    endWire()

    expect(heard).toEqual(['third'])
  })
})
