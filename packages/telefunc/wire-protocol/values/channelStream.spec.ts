import { describe, expect, test, vi } from 'vitest'

import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { stringify } from '@brillout/json-serializer/stringify'
import { createStreamingReplacer } from './server/response/registry.js'
import { ResponseValues } from './client/response/registry.js'
import * as streams from './client/streams.js'
import * as channels from './client/channels.js'
import * as broadcasts from './client/broadcasts.js'
import { readableStreamReplacer } from './client/request/readableStream.js'
import { createStreamProducer } from './server/response/createStreamProducer.js'
import { ChannelStreamSource } from './ChannelStreamSource.js'
import { sendChunks } from './sendChunks.js'
import {
  CHANNEL_PUMP_TAG_DATA,
  CHANNEL_PUMP_TAG_ERROR,
  CHANNEL_PUMP_TAG_PART,
  STREAM_CHUNK_PART_BYTES,
} from '../constants.js'
import { queueMemory } from '../channel/server/channelMemory.js'
import { ChannelOverflowError } from '../channel/channelErrors.js'
import { config } from '../../node/server/serverConfig.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { unsubscribeOf } from '../channel/channel.js'
import type { ClientReviverContext, ServerReplacerContext, StreamingProducer } from './types.js'

const allFeatures = { streams, channels, broadcasts }

config.channel = { maxMemory: '2m' }

/** What this server's channels hold, on the ledger every channel shares. */
const ledgerTotal = () => getGlobalObject('channelMemory.ts', { total: 0 }).total

/** A channel in memory: each send waits until the reader takes it, as a channel's credit would. */
function memoryChannel() {
  let listener: ((frame: Uint8Array) => Promise<void>) | null = null
  let onClose: (() => void) | null = null
  const send = (frame: Uint8Array) => listener!(frame)
  return {
    listenBinary(callback: (frame: Uint8Array) => Promise<void>) {
      listener = callback
      return unsubscribeOf(() => {})
    },
    onClose(callback: (err: Error | null) => void) {
      onClose = () => callback(null)
      return () => {}
    },
    close: async () => {
      onClose?.()
      return null
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
      const source = ChannelStreamSource.create(channel)
      void sendChunks(channel, producers[metadata.__index]!(), new Promise(() => {}))
      return source
    },
  }
  return new ResponseValues(body, [], true).revive(page, (revived) => revived.value, allFeatures)
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

test('a stream over transport channel gets nothing of an empty chunk: one between two others arrives as two chunks', async () => {
  const stream = returnOverChannel(
    new ReadableStream({
      start(controller) {
        for (const text of ['aaa', '', 'bb']) controller.enqueue(new TextEncoder().encode(text))
        controller.close()
      },
    }),
  ) as ReadableStream<Uint8Array>
  const chunks: string[] = []
  for await (const received of stream) chunks.push(new TextDecoder().decode(received))
  expect(chunks).toEqual(['aaa', 'bb'])
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
    const source = ChannelStreamSource.create(channel)
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

  test('on a server, a chunk that fits arrives whole, and its parts count toward maxMemory only while it arrives', async () => {
    const channel = memoryChannel()
    const source = ChannelStreamSource.create(channel, { memory: queueMemory() })
    const read = source.readNextChunk()
    await sendFrames(channel, [CHANNEL_PUMP_TAG_PART])
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(ledgerTotal()).toBe(1 + STREAM_CHUNK_PART_BYTES)
    await sendFrames(channel, [CHANNEL_PUMP_TAG_DATA])
    expect((await read)?.byteLength).toBe(2 * STREAM_CHUNK_PART_BYTES)
    expect(ledgerTotal()).toBe(0)
  })

  test('on a server, a chunk whose parts pass maxMemory ends the read with ChannelOverflowError, and nothing of it stays counted', async () => {
    const channel = memoryChannel()
    const source = ChannelStreamSource.create(channel, { memory: queueMemory() })
    const read = source.readNextChunk()
    // The reader takes no frame after the one it refuses, so the rest of the send waits for good.
    void sendFrames(channel, [
      CHANNEL_PUMP_TAG_PART,
      CHANNEL_PUMP_TAG_PART,
      CHANNEL_PUMP_TAG_PART,
      CHANNEL_PUMP_TAG_DATA,
    ])
    const failure = await read.then(
      () => undefined,
      (err: unknown) => err,
    )
    expect(failure).toBeInstanceOf(ChannelOverflowError)
    expect(ledgerTotal()).toBe(0)
  })
})

describe("a client's upload stream that fails", () => {
  test("of a chunk that isn't a Uint8Array: the page's console says so, and the upload fails as a failing stream does", async () => {
    const logged: unknown[] = []
    const consoleError = vi.spyOn(console, 'error').mockImplementation((error: unknown) => void logged.push(error))
    try {
      const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
        start(controller) {
          // An object, as a JavaScript app enqueues one.
          controller.enqueue(JSON.parse('{ "part": 1 }'))
          controller.close()
        },
      })
      const channel = memoryChannel()
      ChannelStreamSource.create(channel)
      const sending = sendChunks(channel, readableStreamReplacer.createProducer(stream), new Promise(() => {}))
      const message = 'Stream chunks given to a telefunction must be Uint8Array.'
      await expect(sending).rejects.toThrow(message)
      expect(logged).toEqual([expect.objectContaining({ message: expect.stringContaining(message) })])
    } finally {
      consoleError.mockRestore()
    }
  })

  test("on a server, reading it throws after the chunks before the failure, with an error that says the client's stream failed", async () => {
    const channel = memoryChannel()
    const source = ChannelStreamSource.create(channel, { memory: queueMemory() })
    const first = source.readNextChunk()
    await channel.sendBinary(new Uint8Array([CHANNEL_PUMP_TAG_DATA, 7]))
    expect(await first).toEqual(new Uint8Array([7]))
    // What the client's pump sends where its source fails: no detail of the client's error.
    const failed = source.readNextChunk().then(
      () => undefined,
      (err: unknown) => err,
    )
    await channel.sendBinary(new Uint8Array([CHANNEL_PUMP_TAG_ERROR, ...new TextEncoder().encode('{}')]))
    const failure = await failed
    expect(failure).toBeInstanceOf(Error)
    expect(failure).toHaveProperty('message', "Stream ended: the client's ReadableStream failed before its end")
  })
})

describe('a stream sent over transport channel and read to its end', () => {
  test.each([
    [
      "a page's upload argument",
      (stream: ReadableStream<Uint8Array<ArrayBuffer>>) => readableStreamReplacer.createProducer(stream),
    ],
    ["a telefunction's returned stream", createStreamProducer],
  ])('%s is let go of: unlocked, and its own cancel() resolves', async (_, producerOf) => {
    const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
      start(controller) {
        controller.enqueue(Uint8Array.of(4, 5, 6))
        controller.close()
      },
    })
    const producer = producerOf(stream)
    const channel = memoryChannel()
    const source = ChannelStreamSource.create(channel)
    const sent = sendChunks(channel, producer, new Promise(() => {}))
    expect(await source.readNextChunk()).toEqual(Uint8Array.of(4, 5, 6))
    expect(await source.readNextChunk()).toBe(null)
    await sent
    expect(stream.locked).toBe(false)
    await expect(stream.cancel('the caller is done with it')).resolves.toBeUndefined()
  })
})
