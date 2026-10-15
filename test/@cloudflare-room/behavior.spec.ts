import { afterAll, beforeAll, expect, test } from 'vitest'
import { Miniflare } from 'miniflare'
import { bundleWorker } from './bundle.js'

let miniflare: Miniflare | undefined

beforeAll(async () => {
  miniflare = new Miniflare({
    // Listed, not scanned: Miniflare's scan refuses the bundle's import() of a computed specifier.
    modules: [{ type: 'ESModule', path: 'worker-entry.mjs', contents: await bundleWorker() }],
    compatibilityDate: '2025-08-15',
    compatibilityFlags: ['nodejs_compat'],
    durableObjects: {
      ROOM: { className: 'RoomProbeDurableObject', useSQLite: true },
      TelefuncDurableObject: { className: 'SessionDurableObject' },
      TELEFUNC: { className: 'TelefuncProbeDurableObject', useSQLite: true },
    },
  })
})

afterAll(async () => {
  await miniflare?.dispose()
})

async function probe(path: string): Promise<unknown> {
  const response = await miniflare!.dispatchFetch(`https://room.test${path}`)
  const result = await response.json()
  expect(response.status, JSON.stringify(result)).toBe(200)
  return result
}

test('a handoff to a session that fails is lost: the commit counts the route, and its delivery still settles', async () => {
  expect(await probe('/lost-target')).toEqual({ receivers: 1, settlement: 'resolved' })
})

test("a session's Room frames leave the authority without waiting on each other, and arrive in commit order", async () => {
  const frames = ['hold', ...Array.from({ length: 10 }, (_, index) => `frame-${index}`)]
  expect(await probe('/pipelined-delivery')).toEqual({
    whileHeld: frames,
    settlements: frames.map(() => 'resolved'),
  })
})

test('a room authority arms its alarm while it holds a route or an orphaned generation, keeps a due retry where a later call would push it, and clears it once they are gone', async () => {
  expect(await probe('/alarm-policy')).toEqual({
    idle: null,
    afterRoute: 'armed',
    afterUnsubscribe: null,
    afterReopen: 'armed',
    retryKept: true,
    afterAlarm: null,
  })
})

test("a binary commit reaches its lane's sessions, its member's every-track sessions and its track's every-member sessions", async () => {
  expect(await probe('/every-track')).toEqual({
    receivers: [3, 1, 1, 3],
    everyTrack: ['camera-1', 'mic-1', 'camera-2'],
    everyMember: ['camera-1', 'other', 'camera-2'],
    camera: ['camera-1', 'camera-2'],
  })
})

test("a member's join, delivered to a session watching every member's frames, opens that member's lane there", async () => {
  expect(await probe('/join-opens-lane')).toEqual([7])
})

test('a route renews under its own lease only', async () => {
  expect(await probe('/route-renewal')).toEqual({ live: true, otherLease: false })
})

test('heads, cells and a stale commit cross native Durable Object RPC intact', async () => {
  expect(await probe('/native-rpc')).toEqual({
    headConfig: [0x11, 0x22, 0x33],
    cell: [0x44, 0x55],
    count: 1,
    staleCell: { stale: 'cell', key: 'm:missing' },
  })
})

test('a retained Room payload larger than a SQLite row replays whole, as native bytes', async () => {
  expect(await probe('/large-retained')).toEqual({ bytes: 25 * 1024 * 1024, firstMismatch: -1 })
})

test("a room authority's first write that SQLite refuses leaves its tables for the next calls", async () => {
  expect(await probe('/refused-first-write')).toEqual({ refused: true, listed: ['room'] })
})

test('Broadcast reaches every session DO in the isolate in seq order, each through its own I/O', async () => {
  const inOrder = [
    { seq: 1, text: 'one' },
    { seq: 2, text: 'two' },
    { seq: 3, text: 'three' },
  ]
  expect(await probe('/broadcast-sessions')).toEqual({ a: inOrder, b: inOrder })
})

test("a session DO whose calls' callbacks all closed keeps no timer of the GC scan, while another session DO's callback is open (#469)", async () => {
  expect(await probe('/callback-timers')).toEqual({
    whileOnlyBHolds: { a: 0, made: ['a', 'b'] },
    afterBothLeft: { a: 0, b: 0 },
  })
})

test("closes a room nothing holds as Room.close() does, and one whose server is gone lapses at its authority's alarm", async () => {
  expect(await probe('/auto-close')).toEqual({
    alarmArmed: true,
    afterTimeout: { live: 'closed', gone: 'open' },
    afterLapse: 'absent',
    alarmAfterLapse: null,
  })
})

test('lists room meta keys in the order its writes gave them: Room.setMeta() its own, Room.setAttributes() after the keys already there', async () => {
  expect(await probe('/meta-order')).toEqual([
    ['b', 'a', 'c'],
    ['a', 'c', 'aa', 'b'],
    ['z', 'y', 'x'],
    ['z', 'y', 'x', 'w'],
  ])
})

test("resolves an acknowledged send with its handler's reply as soon as a frame after the lost reply reaches its sender", async () => {
  expect(await probe('/lost-dm-ack')).toBe('handled:move')
})
