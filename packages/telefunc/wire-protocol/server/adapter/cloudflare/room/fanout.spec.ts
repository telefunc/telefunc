import { expect, test } from 'vitest'
import {
  dispatchRoomFanout,
  Fanout,
  ROOM_FANOUT_WIDTH,
  type RoomFanoutNamespace,
  type RoomFanoutRequest,
} from './fanout.js'

const routeTo = (sessionDoId: string, leaseId = sessionDoId) => ({
  roomId: 'room',
  inc: 'inc',
  laneKey: 'semantic',
  sessionDoId,
  leaseId,
})
const deliveryInfo = (seq = 1) => ({ inc: 'inc', laneKey: 'semantic', seq, timestamp: 1 })
const routesTo = (count: number) =>
  Array.from({ length: count }, (_, index) => routeTo(`session-${index}`, `lease-${index}`))

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
  const route = routeTo('session', 'lease')
  const first = fanout.enqueue([route], new Uint8Array([1]), deliveryInfo())
  const second = fanout.enqueue([route], new Uint8Array([2]), deliveryInfo(2))

  await firstStarted.promise
  fanout.clearIncarnation('inc')
  releaseFirst.resolve()

  await expect(fanout.await(first)).resolves.toBeUndefined()
  await expect(fanout.await(second)).rejects.toThrow('cancelled before handoff')
  expect(delivered).toEqual([1])
})

test('does not alias an old delivery token to a reconstructed authority attempt', async () => {
  const route = routeTo('session', 'lease')
  const priorAuthority = new Fanout(async () => {})
  const oldToken = priorAuthority.enqueue([route], new Uint8Array([1]), deliveryInfo())
  const reconstructedAuthority = new Fanout(async () => {})
  const newToken = reconstructedAuthority.enqueue([route], new Uint8Array([2]), deliveryInfo(2))

  await expect(reconstructedAuthority.await(oldToken)).rejects.toThrow('unknown delivery token')
  await expect(reconstructedAuthority.await(newToken)).resolves.toBeUndefined()
})

test("starts a lane's next frame only after a rejected handoff settles", async () => {
  const firstStarted = deferred<void>()
  const rejectFirst = deferred<void>()
  let nextStarted = false
  const fanout = new Fanout(async (_targets, _frame, info) => {
    if (info.seq === 2) {
      nextStarted = true
      return
    }
    firstStarted.resolve()
    await rejectFirst.promise
    throw new Error('handoff rejection')
  })
  const route = routeTo('session', 'lease')
  const first = fanout.enqueue([route], new Uint8Array([1]), deliveryInfo())
  const second = fanout.enqueue([route], new Uint8Array([2]), deliveryInfo(2))

  await firstStarted.promise
  await new Promise<void>((resolve) => setTimeout(resolve, 0))
  expect(nextStarted).toBe(false)
  rejectFirst.resolve()
  await expect(fanout.await(first)).rejects.toThrow('handoff rejection')
  await expect(fanout.await(second)).resolves.toBeUndefined()
  expect(nextStarted).toBe(true)
})

test('keeps every recursive coordinator invocation within the configured fanout width', async () => {
  const routes = routesTo(ROOM_FANOUT_WIDTH ** 2 + 1)
  const invocationSubrequests: number[] = []
  const delivered = new Set<string>()

  const runInvocation = async (request: RoomFanoutRequest, currentCoordinator?: string) => {
    let subrequests = 0
    const namespace: RoomFanoutNamespace = {
      idFromString: (id) => ({ kind: 'session' as const, id }),
      idFromName: (name) => ({ kind: 'coordinator' as const, name }),
      get(id) {
        const address = id as { kind: 'session'; id: string } | { kind: 'coordinator'; name: string }
        return {
          async telefuncRoomDeliver(delivery) {
            subrequests += 1
            delivered.add(delivery.sessionDoId)
          },
          async telefuncRoomInvalidate() {
            subrequests += 1
          },
          async telefuncRoomFanout(child) {
            subrequests += 1
            if (address.kind !== 'coordinator') throw new Error('fanout targeted a session')
            if (address.name === currentCoordinator) throw new Error('fanout coordinator called itself')
            return runInvocation(child, address.name)
          },
        }
      },
    }
    const outcomes = await dispatchRoomFanout(namespace, request)
    invocationSubrequests.push(subrequests)
    return outcomes
  }

  const outcomes = await runInvocation({
    operation: 'deliver',
    routes,
    payload: new Uint8Array([1]),
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
