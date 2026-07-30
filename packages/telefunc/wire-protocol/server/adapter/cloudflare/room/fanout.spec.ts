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

test('rejects an unknown delivery token after authority memory is lost', async () => {
  const fanout = new Fanout(async () => {})

  await expect(fanout.await('d-from-prior-authority-instance')).rejects.toThrow('unknown delivery token')
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

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((res) => {
    resolve = res
  })
  return { promise, resolve }
}
