import { afterEach, expect, test, vi } from 'vitest'
import { pumpClientProducerToChannel } from './pumpToChannel.js'
import { ClientChannel } from '../channel.js'
import { config } from '../../../client/clientConfig.js'
import { CHANNEL_PUMP_TAG_DATA, CHANNEL_PUMP_TAG_ERROR } from '../../constants.js'

afterEach(() => {
  vi.restoreAllMocks()
  delete config.fetch
})

test('an upload whose source fails part-way ends the server stream with an error, not as complete', async () => {
  config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
  vi.spyOn(ClientChannel.prototype, 'onOpen').mockImplementation((callback: () => void) => callback())
  const sent: number[] = []
  vi.spyOn(ClientChannel.prototype, '_sendBinary').mockImplementation(((data: Uint8Array) => {
    sent.push(data[0]!)
  }) as never)
  const chunks = (async function* () {
    yield new Uint8Array([1]) as Uint8Array<ArrayBuffer>
    throw new Error('the file read failed')
  })()
  pumpClientProducerToChannel(() => ({ chunks, cancel: () => {} }), ['sse'], 'http://pump.test/_telefunc')
  await new Promise((resolve) => setTimeout(resolve, 10))
  expect(sent).toEqual([CHANNEL_PUMP_TAG_DATA, CHANNEL_PUMP_TAG_ERROR])
})

test('an upload that ends while the server is away waits for it as long as the channel would', async () => {
  vi.useFakeTimers()
  try {
    config.fetch = async () => new Response(new ReadableStream({ start() {} }), { status: 200 })
    vi.spyOn(ClientChannel.prototype, 'onOpen').mockImplementation((callback: () => void) => callback())
    vi.spyOn(ClientChannel.prototype, '_sendBinary').mockImplementation((() => {}) as never)
    const close = vi.spyOn(ClientChannel.prototype, 'close')
    const chunks = (async function* () {
      yield new Uint8Array([1]) as Uint8Array<ArrayBuffer>
    })()
    pumpClientProducerToChannel(() => ({ chunks, cancel: () => {} }), ['sse'], 'http://pump-away.test/_telefunc')
    await vi.advanceTimersByTimeAsync(0)
    let settled = false
    void close.mock.results[0]!.value.then(() => (settled = true))
    await vi.advanceTimersByTimeAsync(8_000) // an outage the reconnect window covers
    expect(settled).toBe(false)
    ;(close.mock.contexts[0] as ClientChannel).abort()
  } finally {
    vi.useRealTimers()
  }
})
