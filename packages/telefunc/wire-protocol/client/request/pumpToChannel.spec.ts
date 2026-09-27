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
