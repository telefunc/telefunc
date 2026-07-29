import { expect, test } from 'vitest'
import { Fanout } from './fanout.js'

test('uses observable TypeScript-private storage on the Durable Object fanout path', () => {
  const fanout = new Fanout(async () => {})
  expect(Object.keys(fanout).sort()).toEqual(
    '_attempts,_chains,_defer,_deliver,_incarnationFences,_tokenSeq'.split(','),
  )
})

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

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((res) => {
    resolve = res
  })
  return { promise, resolve }
}
