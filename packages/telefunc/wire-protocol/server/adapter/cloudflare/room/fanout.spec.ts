import { expect, test } from 'vitest'
import {
  dispatchRoomShardFanout,
  Fanout,
  ROOM_FANOUT_WIDTH,
  type RoomShardFanoutNamespace,
  type RoomShardFanoutRequest,
} from './fanout.js'

const target = (subscriberDoId: string, leaseId = subscriberDoId) => ({
  subscriberDoId,
  leaseId,
  generationToken: 'generation',
})
const deliveryInfo = (seq = 1) => ({ roomId: 'room', inc: 'inc', laneKey: 'semantic', seq, timestamp: 1 })
const targets = (count: number) =>
  Array.from({ length: count }, (_, index) => target(`subscriber-${index}`, `lease-${index}`))

test('rejects a queued delivery cancelled by incarnation cleanup before handoff', async () => {
  const firstStarted = deferred<void>()
  const releaseFirst = deferred<void>()
  const delivered: number[] = []
  const fanout = new Fanout(async (_targets, _frame, info) => {
    delivered.push(info.seq)
    if (info.seq === 1) {
      firstStarted.resolve()
      await releaseFirst.promise
    }
  })
  const route = target('subscriber', 'lease')
  const first = fanout.enqueue('inc', 'semantic', [route], new Uint8Array([1]), deliveryInfo())
  const second = fanout.enqueue('inc', 'semantic', [route], new Uint8Array([2]), deliveryInfo(2))

  await firstStarted.promise
  fanout.clearIncarnation('inc')
  releaseFirst.resolve()

  await expect(fanout.await(first)).resolves.toBeUndefined()
  await expect(fanout.await(second)).rejects.toThrow('cancelled before handoff')
  expect(delivered).toEqual([1])
})

test('does not alias an old delivery token to a reconstructed authority attempt', async () => {
  const route = target('subscriber', 'lease')
  const priorAuthority = new Fanout(async () => {})
  const oldToken = priorAuthority.enqueue('inc', 'semantic', [route], new Uint8Array([1]), deliveryInfo())
  const reconstructedAuthority = new Fanout(async () => {})
  const newToken = reconstructedAuthority.enqueue('inc', 'semantic', [route], new Uint8Array([2]), deliveryInfo(2))

  await expect(reconstructedAuthority.await(oldToken)).rejects.toThrow('unknown delivery token')
  await expect(reconstructedAuthority.await(newToken)).resolves.toBeUndefined()
})

test('keeps the lane gated until every target attempt settles after one rejects', async () => {
  const slowStarted = deferred<void>()
  const releaseSlow = deferred<void>()
  let nextStarted = false
  const fanout = new Fanout(async (targets, _frame, info) => {
    const target = targets[0]!
    if (info.seq === 2) {
      nextStarted = true
      return
    }
    if (target.subscriberDoId === 'fast') throw new Error('fast rejection')
    slowStarted.resolve()
    await releaseSlow.promise
  })
  const first = fanout.enqueue('inc', 'semantic', [target('fast'), target('slow')], new Uint8Array([1]), deliveryInfo())
  const second = fanout.enqueue('inc', 'semantic', [target('slow')], new Uint8Array([2]), deliveryInfo(2))

  await slowStarted.promise
  await new Promise<void>((resolve) => setTimeout(resolve, 0))
  expect(nextStarted).toBe(false)
  releaseSlow.resolve()
  await expect(fanout.await(first)).rejects.toThrow('fast rejection')
  await expect(fanout.await(second)).resolves.toBeUndefined()
})

test('does not issue a flat authority subrequest for every target above the Workers free-tier cap', async () => {
  let authorityDispatches = 0
  const delivered = new Set<string>()
  const fanout = new Fanout(async (targets) => {
    authorityDispatches += 1
    for (const target of targets) delivered.add(target.subscriberDoId)
  })
  const routes = targets(1_001)
  const token = fanout.enqueue('inc', 'semantic', routes, new Uint8Array([1]), deliveryInfo())

  await fanout.await(token)
  expect(delivered.size).toBe(routes.length)
  expect(authorityDispatches).toBeLessThan(1_000)
})

test('keeps every recursive coordinator invocation within the configured fanout width', async () => {
  const routes = targets(ROOM_FANOUT_WIDTH ** 2 + 1)
  const invocationSubrequests: number[] = []
  const delivered = new Set<string>()

  const runInvocation = async (request: RoomShardFanoutRequest, currentCoordinator?: string) => {
    let subrequests = 0
    const namespace: RoomShardFanoutNamespace = {
      idFromString: (id) => ({ kind: 'target' as const, id }),
      idFromName: (name) => ({ kind: 'coordinator' as const, name }),
      get(id) {
        const address = id as { kind: 'target'; id: string } | { kind: 'coordinator'; name: string }
        return {
          async telefuncRoomDeliver(delivery) {
            subrequests += 1
            delivered.add(delivery.subscriberDoId)
          },
          async telefuncRoomInvalidate() {
            subrequests += 1
          },
          async telefuncRoomFanout(child) {
            subrequests += 1
            if (address.kind !== 'coordinator') throw new Error('fanout targeted a subscriber')
            if (address.name === currentCoordinator) throw new Error('fanout coordinator called itself')
            return runInvocation(child, address.name)
          },
        }
      },
    }
    const outcomes = await dispatchRoomShardFanout(namespace, request)
    invocationSubrequests.push(subrequests)
    return outcomes
  }

  const outcomes = await runInvocation({
    operation: 'deliver',
    roomId: 'room',
    inc: 'inc',
    laneKey: 'semantic',
    targets: routes,
    frame: new Uint8Array([1]),
    seq: 1,
    timestamp: 1,
    path: 'root',
  })

  expect(outcomes).toHaveLength(routes.length)
  expect(delivered.size).toBe(routes.length)
  expect(Math.max(...invocationSubrequests)).toBeLessThanOrEqual(ROOM_FANOUT_WIDTH)
})

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((res) => {
    resolve = res
  })
  return { promise, resolve }
}
