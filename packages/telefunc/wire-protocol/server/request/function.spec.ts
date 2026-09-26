import { expect, test } from 'vitest'
import { functionReviver } from './function.js'

test('a callback the server calls without awaiting leaves no unhandled rejection when its channel fails', async () => {
  const channel = {
    send: async () => {
      throw new Error('channel closed')
    },
    close: async () => {},
    abort: () => {},
  }
  const { value } = functionReviver.revive(
    { channelId: 'cb' } as never,
    {
      createChannel: () => channel,
      validators: new Map(),
    } as never,
  ) as { value: (...args: unknown[]) => Promise<unknown> }
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => void unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  try {
    value('tick') // as the stream page's progress callbacks are called
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(unhandled).toEqual([])
    await expect(value('awaited')).rejects.toThrow('channel closed')
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
})
