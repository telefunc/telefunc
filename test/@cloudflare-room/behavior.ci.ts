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
      ROOM: { className: 'TelefuncRoomDurableObject', useSQLite: true },
      TelefuncDurableObject: { className: 'SessionDurableObject' },
      PUBLIC_ROOM: { className: 'PublicRoomDurableObject', useSQLite: true },
      PUBLIC_SESSION: { className: 'PublicRoomSessionDurableObject' },
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
    facadeSettlementOrdering: {
      firstSeq: 1,
      secondSeq: 2,
      firstBeforeSecond: 'settled',
    },
    lifecycle: {
      receivers: 1,
      delivered: [1],
      closed: true,
      generations: [],
      invalidations: ['terminal'],
    },
    terminalDrop: {
      state: 'terminated',
      invalidations: ['terminal'],
    },
    preAckTerminalDrop: {
      state: 'terminated',
      invalidations: ['terminal'],
      generations: [],
    },
    cancelled: 'Cloudflare Room delivery cancelled before handoff',
    cancellationDeliveries: [1],
    fanoutOrdering: {
      firstSettlementBeforeRelease: 'pending',
      firstSettlementAfterRelease: 'rejected',
      secondSettlement: 'delivery probe rejected',
      fastDeliveriesBeforeRelease: [1],
      slowDeliveriesBeforeRelease: [1],
    },
    evictionInvalidations: {
      settlements: ['delivery probe rejected', 'delivery probe rejected', 'delivery probe rejected'],
      invalidations: ['recoverable'],
    },
    unknown: 'Cloudflare Room delivery has an unknown delivery token',
  })
})
