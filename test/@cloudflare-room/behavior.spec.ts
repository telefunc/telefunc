import { afterAll, beforeAll, expect, test } from 'vitest'
import { Miniflare } from 'miniflare'
import { bundleWorker } from './bundle.js'

let miniflare: Miniflare | undefined

beforeAll(async () => {
  miniflare = new Miniflare({
    modules: true,
    script: await bundleWorker(),
    compatibilityDate: '2025-08-06',
    compatibilityFlags: ['nodejs_compat'],
    durableObjects: {
      ROOM: { className: 'RoomProbeDurableObject', useSQLite: true },
      TelefuncDurableObject: { className: 'SessionDurableObject' },
      PUBLIC: { className: 'PublicDurableObject', useSQLite: true },
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

test('a room authority arms its alarm while it holds a route, and clears it once the route is gone', async () => {
  expect(await probe('/alarm-policy')).toEqual({ idle: null, afterRoute: 'armed', afterUnsubscribe: null })
})

test('a route renews under its own lease only', async () => {
  expect(await probe('/route-renewal')).toEqual({ live: true, otherLease: false })
})

test('heads, cells and a stale commit cross native Durable Object RPC intact', async () => {
  expect(await probe('/native-rpc')).toEqual({
    headConfig: [0x11, 0x22, 0x33],
    cell: [0x44, 0x55],
    staleCell: { stale: 'cell', key: 'm:missing' },
  })
})

test('a retained Room payload larger than a SQLite row replays whole, as native bytes', async () => {
  expect(await probe('/large-retained')).toEqual({ bytes: 25 * 1024 * 1024, first: 0x11, last: 0xee })
})

test('Broadcast reaches every session DO in the isolate in seq order, each through its own I/O', async () => {
  const inOrder = [
    { seq: 1, text: 'one' },
    { seq: 2, text: 'two' },
    { seq: 3, text: 'three' },
  ]
  expect(await probe('/broadcast-sessions')).toEqual({ a: inOrder, b: inOrder })
})
