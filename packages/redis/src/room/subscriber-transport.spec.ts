import { EventEmitter } from 'node:events'
import type { Redis } from 'ioredis'
import { expect, onTestFinished, test } from 'vitest'
import { RedisSubscriptionDriver } from './subscriber-transport.js'

test('a subscriber dropping before the commit returns rejects its delivery without an unhandled rejection', async () => {
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  onTestFinished(() => void process.off('unhandledRejection', onUnhandled))
  const subscriber = Object.assign(new EventEmitter(), { status: 'ready', subscribe: async () => {}, disconnect() {} })
  const driver = new RedisSubscriptionDriver({
    prefix: 'telefunc',
    createSubscriber: async () => subscriber as unknown as Redis,
    captureGeneration: async () => 'generation',
    validateGeneration: async () => true,
  })
  const source = { roomId: 'room', inc: 'inc', lane: { kind: 'semantic' } } as const
  const attempt = driver.bind(source).open(
    () => {},
    () => 1,
  )
  await attempt.ready
  const flush = driver.prepareFlush(source)
  subscriber.emit('close')
  await new Promise((resolve) => setTimeout(resolve, 10))
  expect(unhandled).toEqual([])
  await expect(flush.delivery).rejects.toThrow()
})
