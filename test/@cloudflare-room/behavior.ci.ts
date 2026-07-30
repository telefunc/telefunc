import { afterAll, beforeAll, expect, test } from 'vitest'
import { Miniflare } from 'miniflare'
import { bundleWorker } from './bundle.js'

let miniflare: Miniflare

beforeAll(async () => {
  miniflare = new Miniflare({
    modules: true,
    script: await bundleWorker(),
    compatibilityDate: '2025-08-06',
    compatibilityFlags: ['nodejs_compat'],
    durableObjects: {
      ROOM: { className: 'TelefuncRoomDurableObject', useSQLite: true },
      TelefuncDurableObject: { className: 'SessionDurableObject' },
    },
  })
})

afterAll(async () => {
  await miniflare.dispose()
})

test('open, join, commit, deliver, close, cancellation, and authority restart settle honestly', async () => {
  const response = await miniflare.dispatchFetch('https://room.test/probe')
  expect(response.status).toBe(200)
  expect(await response.json()).toEqual({
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
