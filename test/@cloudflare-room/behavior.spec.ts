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

test('public Room lifecycle and authority settlement controls execute on Cloudflare Durable Objects', async () => {
  const response = await miniflare!.dispatchFetch('https://room.test/probe')
  const result = await response.json()
  expect(response.status, JSON.stringify(result)).toBe(200)
  expect(result).toEqual({
    publicLifecycle: {
      created: true,
      joined: true,
      publishedAndSubscribed: [{ kind: 'public-path' }],
      receivedFromPublisher: true,
      closed: true,
    },
    restartSettlement: {
      old: 'Cloudflare Room delivery has an unknown delivery token',
      new: 'resolved',
    },
    lostTarget: { receivers: 1, settlement: 'resolved' },
    alarmPolicy: {
      idle: null,
      afterRoute: 'armed',
      afterUnsubscribe: null,
    },
    nativeRpc: {
      headConfig: [0x11, 0x22, 0x33],
      cell: [0x44, 0x55],
      staleCell: { stale: 'cell', key: 'm:missing' },
    },
  })
})

test('retained Room payloads above the base64-expanded RPC ceiling replay as native bytes', async () => {
  const response = await miniflare!.dispatchFetch('https://room.test/large-retained')
  const result = await response.json()
  expect(response.status, JSON.stringify(result)).toBe(200)
  expect(result).toEqual({
    bytes: 25 * 1024 * 1024,
    first: 0x11,
    last: 0xee,
  })
})

test('Broadcast reaches every session DO in the isolate in seq order, each through its own I/O', async () => {
  const response = await miniflare!.dispatchFetch('https://room.test/broadcast-sessions')
  const result = await response.json()
  expect(response.status, JSON.stringify(result)).toBe(200)
  const inOrder = [
    { seq: 1, text: 'one' },
    { seq: 2, text: 'two' },
    { seq: 3, text: 'three' },
  ]
  expect(result).toEqual({ a: inOrder, b: inOrder })
})
