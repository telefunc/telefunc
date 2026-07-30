import { expect, test } from 'vitest'
import { Fanout } from './fanout.js'

test('rejects a queued delivery cancelled by incarnation cleanup before handoff', async () => {
  const firstStarted = deferred<void>()
  const releaseFirst = deferred<void>()
  const delivered: number[] = []
  const fanout = new Fanout(async (_target, _frame, info) => {
    delivered.push(info.seq)
    if (info.seq === 1) {
      firstStarted.resolve()
      await releaseFirst.promise
    }
  })
  const target = { subscriberDoId: 'subscriber', leaseId: 'lease', generationToken: 'generation' }
  const info = { roomId: 'room', inc: 'inc', laneKey: 'semantic', seq: 1, timestamp: 1 }
  const first = fanout.enqueue('inc', 'semantic', [target], new Uint8Array([1]), info)
  const second = fanout.enqueue('inc', 'semantic', [target], new Uint8Array([2]), { ...info, seq: 2 })

  await firstStarted.promise
  fanout.clearIncarnation('inc')
  releaseFirst.resolve()

  await expect(fanout.await(first)).resolves.toBeUndefined()
  await expect(fanout.await(second)).rejects.toThrow('cancelled before handoff')
  expect(delivered).toEqual([1])
})

test('does not alias an old delivery token to a reconstructed authority attempt', async () => {
  const target = { subscriberDoId: 'subscriber', leaseId: 'lease', generationToken: 'generation' }
  const info = { roomId: 'room', inc: 'inc', laneKey: 'semantic', seq: 1, timestamp: 1 }
  const priorAuthority = new Fanout(async () => {})
  const oldToken = priorAuthority.enqueue('inc', 'semantic', [target], new Uint8Array([1]), info)
  const reconstructedAuthority = new Fanout(async () => {})
  const newToken = reconstructedAuthority.enqueue('inc', 'semantic', [target], new Uint8Array([2]), {
    ...info,
    seq: 2,
  })

  await expect(reconstructedAuthority.await(oldToken)).rejects.toThrow('unknown delivery token')
  await expect(reconstructedAuthority.await(newToken)).resolves.toBeUndefined()
})

test('keeps the lane gated until every target attempt settles after one rejects', async () => {
  const slowStarted = deferred<void>()
  const releaseSlow = deferred<void>()
  let nextStarted = false
  const fanout = new Fanout(async (target, _frame, info) => {
    if (info.seq === 2) {
      nextStarted = true
      return
    }
    if (target.subscriberDoId === 'fast') throw new Error('fast rejection')
    slowStarted.resolve()
    await releaseSlow.promise
  })
  const info = { roomId: 'room', inc: 'inc', laneKey: 'semantic', seq: 1, timestamp: 1 }
  const first = fanout.enqueue(
    'inc',
    'semantic',
    [
      { subscriberDoId: 'fast', leaseId: 'fast', generationToken: 'generation' },
      { subscriberDoId: 'slow', leaseId: 'slow', generationToken: 'generation' },
    ],
    new Uint8Array([1]),
    info,
  )
  const second = fanout.enqueue(
    'inc',
    'semantic',
    [{ subscriberDoId: 'slow', leaseId: 'slow', generationToken: 'generation' }],
    new Uint8Array([2]),
    { ...info, seq: 2 },
  )

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
  const fanout = new Fanout(async (target) => {
    authorityDispatches += 1
    delivered.add(target.subscriberDoId)
  })
  const targets = Array.from({ length: 1_001 }, (_, index) => ({
    subscriberDoId: `subscriber-${index}`,
    leaseId: `lease-${index}`,
    generationToken: 'generation',
  }))
  const token = fanout.enqueue('inc', 'semantic', targets, new Uint8Array([1]), {
    roomId: 'room',
    inc: 'inc',
    laneKey: 'semantic',
    seq: 1,
    timestamp: 1,
  })

  await fanout.await(token)
  expect(delivered.size).toBe(targets.length)
  expect(authorityDispatches).toBeLessThan(1_000)
})

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((res) => {
    resolve = res
  })
  return { promise, resolve }
}
