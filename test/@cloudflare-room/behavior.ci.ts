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
      secondBeforeRelease: 'pending',
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
      ready: 'Cloudflare Room subscription terminated before acknowledgement',
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
    coordinatorFanout: {
      outcomes: 65,
      deliveries: 65,
    },
    transientFailureRecovery: {
      settlements: [
        'delivery probe rejected',
        'delivery probe rejected',
        'delivery probe rejected',
        'delivery probe rejected',
        'delivery probe rejected',
      ],
      recovered: 'resolved',
      deliveries: [1, 2, 3, 4, 5, 6],
      invalidations: ['terminal'],
    },
    restartSettlement: {
      old: 'Cloudflare Room delivery has an unknown delivery token',
      new: 'resolved',
    },
    alarmPolicy: {
      idle: null,
      afterRoute: 'armed',
      afterUnsubscribe: null,
    },
    controlPreconditions: {
      waitForCommit: 'response reordering probe was not prepared',
      releaseCommit: 'response reordering probe was not prepared',
      releaseDelivery: 'response reordering probe was not prepared',
      waitForRegistration: 'registration hold probe was not prepared',
      releaseRegistration: 'registration hold probe was not prepared',
    },
    nativeRpc: {
      headConfig: [0x11, 0x22, 0x33],
      cell: [0x44, 0x55],
      validationError: "head CX: {delete} is legal only against a 'closed' tombstone, not 'open'",
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
