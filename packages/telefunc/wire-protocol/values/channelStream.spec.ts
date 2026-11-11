import { describe, expect, test } from 'vitest'

import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { createStreamingReplacer } from './server/response/registry.js'
import { createStreamingReviver } from './client/response/registry.js'
import { ChannelStreamSource } from './ChannelStreamSource.js'
import { sendChunks } from './sendChunks.js'
import { CHANNEL_PUMP_TAG_DATA, CHANNEL_PUMP_TAG_PART, STREAM_CHUNK_PART_BYTES } from '../constants.js'
import type { ClientReviverContext, ServerReplacerContext, StreamingProducer } from './types.js'

/** A channel in memory: each send waits until the reader takes it, as a channel's credit would. */
function memoryChannel() {
  let listener: ((frame: Uint8Array) => Promise<void>) | null = null
  let onClose: (() => void) | null = null
  const send = (frame: Uint8Array) => listener!(frame)
  return {
    listenBinary(callback: (frame: Uint8Array) => Promise<void>) {
      listener = callback
      return () => {}
    },
    onClose(callback: () => void) {
      onClose = callback
    },
    close: async () => {
      onClose?.()
      return 0 as const
    },
    abort() {},
    sendBinary: send,
    _sendBinary: send,
  }
}

/** What the page revives of a returned value whose streams go over transport 'channel': the server's pump sends each
 *  stream on a channel of its own, and the page reads it. */
function returnOverChannel(value: unknown): unknown {
  const producers: (() => StreamingProducer)[] = []
  const server: ServerReplacerContext = {
    createChannel: () => {
      throw new Error('Unexpected channel')
    },
    registerChannel() {},
    sendStream(createProducer) {
      producers.push(createProducer)
      return { metadata: { __index: producers.length - 1 }, close() {}, abort() {} }
    },
    validators: new Map(),
  }
  const replacer = createStreamingReplacer(
    () => server,
    () => {},
    [],
  )
  const body = stringify(value, { forbidReactElements: true, replacer })
  const page: ClientReviverContext = {
    createChannel: () => {
      throw new Error('Unexpected channel')
    },
    createBroadcast: () => {
      throw new Error('Unexpected broadcast')
    },
    receiveStream(metadata) {
      if (!('__index' in metadata)) throw new Error('Unexpected metadata')
      const channel = memoryChannel()
      const source = ChannelStreamSource.create(channel as never)
      void sendChunks(channel, producers[metadata.__index]!(), new Promise(() => {}))
      return source
    },
  }
  return parse(body, { reviver: createStreamingReviver(page, (revived) => revived.value, []) })
}

describe('a value streamed over transport channel, larger than the 1 MiB parts it goes in', () => {
  test.each([1_048_600, 3_000_000])('a generator yields it whole: %i chars', async (size) => {
    const gen = returnOverChannel(
      (async function* () {
        yield 'x'.repeat(size)
        yield 'done'
      })(),
    ) as AsyncGenerator<string>
    expect((await gen.next()).value).toBe('x'.repeat(size))
    expect((await gen.next()).value).toBe('done')
    expect((await gen.next()).done).toBe(true)
  })

  test.each([1_048_600, 3_000_000])('a promise resolves to it whole: %i chars', async (size) => {
    const { p } = returnOverChannel({ p: Promise.resolve('y'.repeat(size)) }) as { p: Promise<string> }
    expect(await p).toBe('y'.repeat(size))
  })

  test('a stream gets each chunk whole', async () => {
    const chunk = new Uint8Array(3 * 1024 * 1024 + 7)
    for (let i = 0; i < chunk.length; i++) chunk[i] = i % 251
    const stream = returnOverChannel(
      new ReadableStream({
        start(controller) {
          controller.enqueue(chunk)
          controller.close()
        },
      }),
    ) as ReadableStream<Uint8Array>
    const chunks: Uint8Array[] = []
    for await (const received of stream) chunks.push(received)
    expect(chunks.map((received) => received.byteLength)).toEqual([chunk.byteLength])
    expect(Buffer.compare(chunks[0]!, chunk)).toBe(0)
  })
})

describe('a chunk sent in parts', () => {
  setFlagsFromString('--expose-gc')
  const gc: () => void = runInNewContext('gc')

  /** Sends a part-sized frame for each tag, and returns weak references to their buffers. */
  async function sendFrames(channel: ReturnType<typeof memoryChannel>, tags: number[]) {
    const sent: WeakRef<ArrayBuffer>[] = []
    for (const tag of tags) {
      const frame = new Uint8Array(1 + STREAM_CHUNK_PART_BYTES)
      frame[0] = tag
      sent.push(new WeakRef(frame.buffer))
      await channel.sendBinary(frame)
    }
    return sent
  }

  test.each(['cancelled', 'completed'])('a %s read keeps none of its parts', async (ending) => {
    const channel = memoryChannel()
    const source = ChannelStreamSource.create(channel as never)
    const read = source.readNextChunk()
    const sent = await sendFrames(channel, Array(10).fill(CHANNEL_PUMP_TAG_PART))
    if (ending === 'cancelled') {
      source.cancel()
      expect(await read).toBe(null)
    } else {
      sent.push(...(await sendFrames(channel, [CHANNEL_PUMP_TAG_DATA])))
      expect((await read)!.byteLength).toBe(11 * STREAM_CHUNK_PART_BYTES)
    }
    for (let i = 0; i < 8; i++) {
      gc()
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
    expect(sent.filter((buffer) => buffer.deref() !== undefined)).toHaveLength(0)
  })
})
