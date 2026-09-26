import { describe, expect, test } from 'vitest'

import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { createStreamingReplacer } from './server/response/registry.js'
import { createStreamingReviver } from './client/response/registry.js'
import { ServerChannel } from './server/channel.js'
import { ServerBroadcast } from './server/server-broadcast.js'
import { wrapProxy } from './wrapProxy.js'
import { serializeTelefunctionResult } from '../node/server/runTelefunc/serializeTelefunctionResult.js'
import { parseHttpRequest } from '../node/server/runTelefunc/parseHttpRequest.js'
import { createRequestContext } from '../node/server/context/requestContext.js'
import { parseResponse } from './client/response/parse.js'
import { STREAM_TRANSPORT, type StreamTransport } from './constants.js'
import { config, getServerConfig } from '../node/server/serverConfig.js'
import type { AbortError } from '../shared/Abort.js'
import type {
  ClientReviverContext,
  InternalClientReviverContext,
  InternalServerReplacerContext,
  ReplacerType,
  ReviverType,
  ServerReplacerContext,
  ServerReviverContext,
  StreamingProducer,
  StreamSource,
  TypeContract,
} from './types.js'

// ───────────────────────────────────────────────────────────────────────────
// Reference-identity-preserving serialization — bug classes targeted: duplicate
// replace() side effects (stub channels minted per occurrence, lifecycle
// registered per occurrence), duplicate revive() side effects (client objects
// minted per occurrence, N wire subscriptions for one server value), and
// over-deduplication (distinct values merged into one).
// ───────────────────────────────────────────────────────────────────────────

// ===== Registry-level harness =====
// Real registries, real stringify/parse, real ServerChannel/ServerBroadcast —
// contexts mirror serializeTelefunctionResult / reviveResponse minus transport.

type Lifecycle = { close: () => Promise<void> | void; abort: (abortError: AbortError) => void }

function createServerHarness(extensionTypes: ReplacerType<TypeContract, ServerReplacerContext>[] = []) {
  const registeredChannels: unknown[] = []
  const producers: { createProducer: () => StreamingProducer; index: number }[] = []
  const lifecycles: Lifecycle[] = []
  let nextIndex = 0
  const context: InternalServerReplacerContext = {
    createChannel(opts) {
      const channel = new ServerChannel(opts)
      context.registerChannel(channel)
      return channel as never
    },
    registerChannel(channel) {
      registeredChannels.push(channel)
    },
    sendStream(createProducer) {
      const index = nextIndex++
      producers.push({ createProducer, index })
      return { metadata: { __index: index }, close() {}, abort() {} }
    },
    validators: new Map(),
    responseState: (_key, init) => init(),
  }
  const replacer = createStreamingReplacer(
    () => context,
    (replaced) => lifecycles.push(replaced),
    extensionTypes,
  )
  function serialize(value: unknown): string {
    return stringify(value, { forbidReactElements: true, replacer })
  }
  return { serialize, registeredChannels, producers, lifecycles }
}

function createClientHarness(extensionTypes: ReviverType<TypeContract, ClientReviverContext>[] = []) {
  const mintedChannels: { channelId: string; ack?: boolean }[] = []
  const mintedBroadcasts: { channelId: string; key: string }[] = []
  const lifecycles: { value: unknown; close: () => Promise<void> | void; abort: (abortError: AbortError) => void }[] =
    []
  const context: InternalClientReviverContext = {
    shareLifecycle() {},
    createChannel(opts) {
      mintedChannels.push(opts)
      return { kind: 'client-channel', ...opts, close: async () => {}, abort: () => {} } as never
    },
    createBroadcast(opts) {
      mintedBroadcasts.push(opts)
      return { kind: 'client-broadcast', ...opts, close: async () => {}, abort: () => {} } as never
    },
    receiveStream() {
      throw new Error('registry-level harness does not stream')
    },
    waitFor() {},
  }
  const reviver = createStreamingReviver(
    context,
    (revived) => {
      // Mirror reviveResponse: the value handed out (and cached) is the GC wrapper.
      revived.value = wrapProxy(revived.value as object)
      lifecycles.push(revived)
    },
    extensionTypes,
  )
  const parseBody = (body: string) => parse(body, { reviver })
  return { parseBody, mintedChannels, mintedBroadcasts, lifecycles }
}

// ===== Room / LocalParticipant extension types =====
// Shaped after the Room API prototype: replace() mints a fresh stub channel per
// call and attaches it to the live server object — exactly the side effect that
// must not run once per occurrence.

class TestServerRoom {
  readonly id: string
  readonly stubsAttached: unknown[] = []
  constructor(id: string) {
    this.id = id
  }
  _attachStub(stub: unknown) {
    this.stubsAttached.push(stub)
  }
}

class TestLocalParticipant {
  readonly id: string
  channelsMinted = 0
  constructor(id: string) {
    this.id = id
  }
}

type RoomContract = TypeContract<
  TestServerRoom,
  { kind: string; roomId: string },
  { channelId: string; roomId: string }
>
type ParticipantContract = TypeContract<
  TestLocalParticipant,
  { kind: string; participantId: string },
  { channelId: string; participantId: string }
>

function makeRoomExtension() {
  const counters = { serverClose: 0, serverAbort: 0, clientRevive: 0, clientClose: 0, clientAbort: 0 }
  const serverType: ReplacerType<RoomContract, ServerReplacerContext> = {
    prefix: '!TestRoom:',
    detect: (value): value is TestServerRoom => value instanceof TestServerRoom,
    replace(room, context) {
      const stub = context.createChannel()
      room._attachStub(stub)
      return {
        metadata: { channelId: (stub as { id: string }).id, roomId: room.id },
        close() {
          counters.serverClose++
        },
        abort() {
          counters.serverAbort++
        },
      }
    },
  }
  const clientType: ReviverType<RoomContract, ClientReviverContext> = {
    prefix: '!TestRoom:',
    revive(metadata) {
      counters.clientRevive++
      return {
        value: { kind: 'client-room', roomId: metadata.roomId },
        close() {
          counters.clientClose++
        },
        abort() {
          counters.clientAbort++
        },
      }
    },
  }
  return { serverType, clientType, counters }
}

function makeParticipantExtension() {
  const counters = { clientRevive: 0 }
  const serverType: ReplacerType<ParticipantContract, ServerReplacerContext> = {
    prefix: '!TestParticipant:',
    detect: (value): value is TestLocalParticipant => value instanceof TestLocalParticipant,
    replace(participant, context) {
      const channel = context.createChannel()
      participant.channelsMinted++
      return {
        metadata: { channelId: (channel as { id: string }).id, participantId: participant.id },
        close() {},
        abort() {},
      }
    },
  }
  const clientType: ReviverType<ParticipantContract, ClientReviverContext> = {
    prefix: '!TestParticipant:',
    revive(metadata) {
      counters.clientRevive++
      return {
        value: { kind: 'client-participant', participantId: metadata.participantId },
        close() {},
        abort() {},
      }
    },
  }
  return { serverType, clientType, counters }
}

// ───────────────────────────────────────────────────────────────────────────
// One payload — registry-level round-trips
// ───────────────────────────────────────────────────────────────────────────

describe('reference identity — duplicates in one payload', () => {
  test('Channel: N references serialize to one identity and revive to one object', () => {
    const server = createServerHarness()
    const channel = new ServerChannel()
    const body = server.serialize({ ch: channel, chDupe: channel, list: [channel, { deep: channel }] })

    // One registration, one lifecycle — not four.
    expect(server.registeredChannels).toEqual([channel])
    expect(server.lifecycles).toHaveLength(1)
    // One channelId on the wire, everywhere.
    expect(body.match(new RegExp(channel.id, 'g'))).toHaveLength(4)

    const client = createClientHarness()
    const parsed = client.parseBody(body) as {
      ch: unknown
      chDupe: unknown
      list: [unknown, { deep: unknown }]
    }
    expect(client.mintedChannels).toHaveLength(1)
    expect(client.mintedChannels[0]!.channelId).toBe(channel.id)
    expect(client.lifecycles).toHaveLength(1)
    expect(parsed.ch).toBe(parsed.chDupe)
    expect(parsed.ch).toBe(parsed.list[0])
    expect(parsed.ch).toBe(parsed.list[1].deep)
  })

  test('Broadcast: duplicated references mint one client broadcast', () => {
    const server = createServerHarness()
    const broadcast = new ServerBroadcast({ key: 'room:identity' })
    const body = server.serialize({ room: broadcast, roomDupe: broadcast, roomDupe2: broadcast })

    expect(server.registeredChannels).toEqual([broadcast])
    expect(server.lifecycles).toHaveLength(1)

    const client = createClientHarness()
    const parsed = client.parseBody(body) as { room: unknown; roomDupe: unknown; roomDupe2: unknown }
    expect(client.mintedBroadcasts).toHaveLength(1)
    expect(parsed.room).toBe(parsed.roomDupe)
    expect(parsed.room).toBe(parsed.roomDupe2)
  })

  test('Room (extension): one stub channel minted, one _attachStub, one client room', () => {
    const { serverType, clientType, counters } = makeRoomExtension()
    const server = createServerHarness([serverType as ReplacerType<TypeContract, ServerReplacerContext>])
    const room = new TestServerRoom('lobby')
    const body = server.serialize({ room, roomDupe: room, roomDupe2: room })

    // The repro: three occurrences used to mint three stub channels.
    expect(room.stubsAttached).toHaveLength(1)
    expect(server.registeredChannels).toHaveLength(1)
    expect(server.lifecycles).toHaveLength(1)

    const client = createClientHarness([clientType as ReviverType<TypeContract, ClientReviverContext>])
    const parsed = client.parseBody(body) as { room: unknown; roomDupe: unknown; roomDupe2: unknown }
    expect(counters.clientRevive).toBe(1)
    expect(parsed.room).toBe(parsed.roomDupe)
    expect(parsed.room).toBe(parsed.roomDupe2)
    expect((parsed.room as { roomId: string }).roomId).toBe('lobby')
  })

  test('LocalParticipant (extension): duplicated references mint one channel', () => {
    const { serverType, clientType, counters } = makeParticipantExtension()
    const server = createServerHarness([serverType as ReplacerType<TypeContract, ServerReplacerContext>])
    const participant = new TestLocalParticipant('me')
    const body = server.serialize({ me: participant, self: participant })

    expect(participant.channelsMinted).toBe(1)
    expect(server.registeredChannels).toHaveLength(1)

    const client = createClientHarness([clientType as ReviverType<TypeContract, ClientReviverContext>])
    const parsed = client.parseBody(body) as { me: unknown; self: unknown }
    expect(counters.clientRevive).toBe(1)
    expect(parsed.me).toBe(parsed.self)
  })

  test('Function: duplicated references share one channel and one client wrapper', () => {
    const server = createServerHarness()
    const fn = (x: number) => x * 2
    const body = server.serialize({ fn, fnDupe: fn })

    expect(server.registeredChannels).toHaveLength(1)

    const client = createClientHarness()
    const parsed = client.parseBody(body) as { fn: unknown; fnDupe: unknown }
    expect(client.mintedChannels).toHaveLength(1)
    expect(typeof parsed.fn).toBe('function')
    expect(parsed.fn).toBe(parsed.fnDupe)
  })

  test('streamed values: duplicated references register one producer', () => {
    const server = createServerHarness()
    const gen = (async function* () {
      yield 1
    })()
    const body = server.serialize({ gen, genDupe: gen })
    expect(server.producers).toHaveLength(1)

    // Both keys carry the same __index.
    const parsed = JSON.parse(body) as { gen: string; genDupe: string }
    expect(parsed.gen).toBe(parsed.genDupe)
  })

  test('distinct values of the same type stay distinct', () => {
    const server = createServerHarness()
    const a = new ServerChannel()
    const b = new ServerChannel()
    const body = server.serialize({ a, b })

    expect(server.registeredChannels).toEqual([a, b])
    expect(server.lifecycles).toHaveLength(2)

    const client = createClientHarness()
    const parsed = client.parseBody(body) as { a: unknown; b: unknown }
    expect(client.mintedChannels).toHaveLength(2)
    expect(parsed.a).not.toBe(parsed.b)
  })

  test('close/abort lifecycle registers once per identity — closing closes once', async () => {
    const { serverType, clientType, counters } = makeRoomExtension()
    const server = createServerHarness([serverType as ReplacerType<TypeContract, ServerReplacerContext>])
    const room = new TestServerRoom('lifecycle')
    const body = server.serialize({ room, roomDupe: room })

    expect(server.lifecycles).toHaveLength(1)
    for (const { close } of server.lifecycles) await close()
    for (const { abort } of server.lifecycles) abort({ abortValue: undefined } as AbortError)
    expect(counters.serverClose).toBe(1)
    expect(counters.serverAbort).toBe(1)

    const client = createClientHarness([clientType as ReviverType<TypeContract, ClientReviverContext>])
    client.parseBody(body)
    expect(client.lifecycles).toHaveLength(1)
    for (const { close } of client.lifecycles) await close()
    for (const { abort } of client.lifecycles) abort({ abortValue: undefined } as AbortError)
    expect(counters.clientClose).toBe(1)
    expect(counters.clientAbort).toBe(1)
  })

  test('metadata with JSON-escaped characters keeps identity across occurrences', () => {
    const { serverType, clientType } = makeRoomExtension()
    const server = createServerHarness([serverType as ReplacerType<TypeContract, ServerReplacerContext>])
    // '<' and '/' trigger the serializer's HTML-safety escaping; '"' exercises JSON escapes.
    const room = new TestServerRoom('a/b<c>"d"')
    const body = server.serialize({ room, roomDupe: room })

    const client = createClientHarness([clientType as ReviverType<TypeContract, ClientReviverContext>])
    const parsed = client.parseBody(body) as { room: { roomId: string }; roomDupe: unknown }
    expect(parsed.room).toBe(parsed.roomDupe)
    expect(parsed.room.roomId).toBe('a/b<c>"d"')
  })

  test('two distinct values with identical metadata fail loudly (identity contract)', () => {
    const constantMetadataType: ReplacerType<
      TypeContract<TestServerRoom, unknown, { constant: true }>,
      ServerReplacerContext
    > = {
      prefix: '!TestConstant:',
      detect: (value): value is TestServerRoom => value instanceof TestServerRoom,
      replace() {
        return { metadata: { constant: true }, close() {}, abort() {} }
      },
    }
    const server = createServerHarness([constantMetadataType as ReplacerType<TypeContract, ServerReplacerContext>])
    const a = new TestServerRoom('a')
    const b = new TestServerRoom('b')
    // Same reference: deduplicated, no complaint.
    expect(() => server.serialize({ a, aDupe: a })).not.toThrow()
    // Distinct references, identical wire strings: the client could not tell them apart.
    expect(() => server.serialize({ a, b })).toThrow(/same wire representation/)
  })

  test('primitive special values bypass identity (no reference to preserve)', () => {
    const SENTINEL = 'telefunc-spec-sentinel'
    let replaceCalls = 0
    const primitiveType: ReplacerType<TypeContract<string, unknown, { n: number }>, ServerReplacerContext> = {
      prefix: '!TestPrimitive:',
      detect: (value): value is string => value === SENTINEL,
      replace() {
        replaceCalls++
        return { metadata: { n: replaceCalls }, close() {}, abort() {} }
      },
    }
    const server = createServerHarness([primitiveType as ReplacerType<TypeContract, ServerReplacerContext>])
    const body = server.serialize({ a: SENTINEL, b: SENTINEL })
    // Two occurrences, two replace() calls — primitives are value-semantic.
    expect(replaceCalls).toBe(2)

    let reviveCalls = 0
    const primitiveReviver: ReviverType<TypeContract<string, unknown, { n: number }>, ClientReviverContext> = {
      prefix: '!TestPrimitive:',
      revive(metadata) {
        reviveCalls++
        return { value: { n: metadata.n }, close() {}, abort() {} }
      },
    }
    const client = createClientHarness([primitiveReviver as ReviverType<TypeContract, ClientReviverContext>])
    const parsed = client.parseBody(body) as { a: { n: number }; b: { n: number } }
    // Distinct metadata → distinct wire strings → distinct revivals.
    expect(reviveCalls).toBe(2)
    expect(parsed.a).not.toBe(parsed.b)
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Full pipeline
// (serializeTelefunctionResult → inline binary body → parseResponse)
// ───────────────────────────────────────────────────────────────────────────

async function roundTrip(
  telefunctionReturn: unknown,
  opts: {
    serverExtensions?: ReplacerType<TypeContract, ServerReplacerContext>[]
    clientExtensions?: ReviverType<TypeContract, ClientReviverContext>[]
    streamTransport?: StreamTransport
    /** The network between the server's body and the client's. */
    network?: TransformStream<Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>>
  } = {},
) {
  const streamTransport = opts.streamTransport ?? STREAM_TRANSPORT.BINARY_INLINE
  const extensionName = `ref-identity-spec-${nextExtensionId++}`
  if (opts.serverExtensions) {
    config.extensions.push({ name: extensionName, responseTypes: opts.serverExtensions })
  }
  try {
    const requestContext = createRequestContext(new Request('http://localhost/_telefunc', { method: 'POST' }))
    const result = serializeTelefunctionResult({
      telefunctionReturn,
      telefunctionName: 'testFn',
      telefuncFilePath: '/pages/spec/ref-identity.telefunc.ts',
      telefunctionAborted: false,
      context: {},
      requestContext,
      abortSignal: requestContext.abortSignal,
      streamTransport,
      useNodeStream: false,
      serverConfig: {
        log: { shieldErrors: { dev: false, prod: false } },
      },
    })
    const body = result.body as ReadableStream<Uint8Array<ArrayBuffer>>
    const response =
      result.type === 'text'
        ? new Response(result.body)
        : new Response(opts.network ? body.pipeThrough(opts.network) : body, {
            headers: {
              'content-type':
                streamTransport === STREAM_TRANSPORT.SSE_INLINE ? 'text/event-stream' : 'application/octet-stream',
            },
          })
    const abortController = new AbortController()
    const parsed = (await parseResponse(
      response,
      {
        telefunctionName: 'testFn',
        telefuncFilePath: '/pages/spec/ref-identity.telefunc.ts',
        abortController,
        channel: { transports: ['sse'] },
        requestCloseHandlers: [],
        extensionResponseTypes: opts.clientExtensions ?? [],
        headers: null,
        telefuncUrl: 'http://localhost/_telefunc',
      },
      undefined,
    )) as { ret: unknown }
    return { ret: parsed.ret, abortController }
  } finally {
    const index = config.extensions.findIndex((extension) => extension.name === extensionName)
    if (index >= 0) config.extensions.splice(index, 1)
  }
}

let nextExtensionId = 0

async function collect<T>(gen: AsyncGenerator<T>): Promise<T[]> {
  const out: T[] = []
  for await (const chunk of gen) out.push(chunk)
  return out
}

async function teeAndDrop(stream: ReadableStream<Uint8Array<ArrayBuffer>>): Promise<ReadableStream<Uint8Array>> {
  const { ret } = await roundTrip({ stream })
  return (ret as { stream: ReadableStream<Uint8Array> }).stream.tee()[0]
}

describe('reference identity — full pipeline', () => {
  test("two returned streams read one after the other both complete, as the docs' concurrent downloads may be", async () => {
    const source = () => {
      let sent = 0
      return new ReadableStream<Uint8Array<ArrayBuffer>>({
        pull(controller) {
          if (sent++ === 32)
            controller.close() // 2 MiB
          else controller.enqueue(new Uint8Array(64 * 1024))
        },
      })
    }
    const { ret } = await roundTrip({ first: source(), second: source() })
    const { first, second } = ret as Record<'first' | 'second', ReadableStream<Uint8Array>>
    const bytes = async (stream: ReadableStream<Uint8Array>) => (await new Response(stream).arrayBuffer()).byteLength
    // The second one first, as `await dl2.saveToMemory()` before dl1's: the first one's bytes arrive meanwhile.
    expect(await bytes(second)).toBe(2 * 1024 * 1024)
    expect(await bytes(first)).toBe(2 * 1024 * 1024)
  })

  test('duplicated async generator: one producer, one client object, chunks delivered once', async () => {
    const gen = (async function* () {
      yield 1
      yield 2
      yield 3
    })()
    const { ret } = await roundTrip({ gen, genDupe: gen })
    const retTyped = ret as { gen: AsyncGenerator<number>; genDupe: AsyncGenerator<number> }

    expect(retTyped.gen).toBe(retTyped.genDupe)
    // One consumer sees every chunk — duplicated producers used to steal chunks
    // from one another (each occurrence pulled the same underlying generator).
    expect(await collect(retTyped.gen)).toEqual([1, 2, 3])
  })

  test('the response body is cancelled once every value is done or cancelled', async () => {
    let upstreamCancelled = false
    const pending = new ReadableStream({ cancel: () => void (upstreamCancelled = true) })
    const { ret } = await roundTrip({ gen: (async function* () {})(), pending })
    const retTyped = ret as { gen: AsyncGenerator<never>; pending: ReadableStream }
    expect(await collect(retTyped.gen)).toEqual([])
    await retTyped.pending.cancel()
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(upstreamCancelled).toBe(true)
  })

  test('duplicated ReadableStream: previously crashed with a locked-stream error', async () => {
    const payload = new TextEncoder().encode('stream-bytes')
    const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
      start(controller) {
        controller.enqueue(payload as Uint8Array<ArrayBuffer>)
        controller.close()
      },
    })
    // Before identity dedup this threw synchronously: the second occurrence's
    // producer called stream.getReader() on an already-locked stream.
    const { ret } = await roundTrip({ stream, streamDupe: stream })
    const retTyped = ret as { stream: ReadableStream<Uint8Array>; streamDupe: ReadableStream<Uint8Array> }

    expect(retTyped.stream).toBe(retTyped.streamDupe)
    const reader = retTyped.stream.getReader()
    const chunks: number[] = []
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      chunks.push(...value)
    }
    expect(new TextDecoder().decode(new Uint8Array(chunks))).toBe('stream-bytes')
  })

  test('duplicated promise: one resolution frame, same client promise', async () => {
    const promise = Promise.resolve({ answer: 42 })
    const { ret } = await roundTrip({ p: promise, pDupe: promise })
    const retTyped = ret as { p: Promise<{ answer: number }>; pDupe: Promise<{ answer: number }> }

    expect(retTyped.p).toBe(retTyped.pDupe)
    expect(await retTyped.p).toEqual({ answer: 42 })
  })

  test('duplicated File: one byte stream, same client File promise', async () => {
    const file = new File(['file-contents'], 'notes.txt', { type: 'text/plain', lastModified: 1234567890 })
    const { ret } = await roundTrip({ file, fileDupe: file })
    const retTyped = ret as { file: Promise<File>; fileDupe: Promise<File> }

    expect(retTyped.file).toBe(retTyped.fileDupe)
    const revived = await retTyped.file
    expect(revived.name).toBe('notes.txt')
    expect(await revived.text()).toBe('file-contents')
  })

  test('abort reaches a duplicated value exactly once', async () => {
    const { serverType, clientType, counters } = makeRoomExtension()
    const room = new TestServerRoom('abort-once')
    const { ret, abortController } = await roundTrip(
      { room, roomDupe: room },
      {
        serverExtensions: [serverType as ReplacerType<TypeContract, ServerReplacerContext>],
        clientExtensions: [clientType as ReviverType<TypeContract, ClientReviverContext>],
      },
    )
    const retTyped = ret as { room: unknown; roomDupe: unknown }
    expect(retTyped.room).toBe(retTyped.roomDupe)

    abortController.abort()
    expect(counters.clientAbort).toBe(1)
  })

  test('a cancelled inline stream frees the frames it buffered, even once its done frame arrived', async () => {
    const encode = (text: string) => new TextEncoder().encode(text) as Uint8Array<ArrayBuffer>
    class RawChunks {
      constructor(readonly chunks: string[]) {}
    }
    const prefix = '!RefIdentityRawChunks:'
    const serverType = {
      prefix,
      detect: (value: unknown): value is RawChunks => value instanceof RawChunks,
      replace(value: RawChunks, context: ServerReplacerContext) {
        const sent = context.sendStream(() => ({
          chunks: (async function* () {
            for (const chunk of value.chunks) yield encode(chunk)
          })(),
          cancel() {},
        }))
        return { metadata: sent.metadata, close() {}, abort() {} }
      },
    }
    const clientType = {
      prefix,
      revive: (metadata: never, context: ClientReviverContext) => ({
        value: context.receiveStream(metadata),
        close() {},
        abort() {},
      }),
    }
    let releaseA!: () => void
    const gate = new Promise<void>((resolve) => (releaseA = resolve))
    const a = new ReadableStream<Uint8Array<ArrayBuffer>>({
      async start(controller) {
        controller.enqueue(encode('a1'))
        await gate
        controller.close()
      },
    })
    const { ret } = await roundTrip(
      { a, b: new RawChunks(['b1', 'b2']) },
      {
        serverExtensions: [serverType as unknown as ReplacerType<TypeContract, ServerReplacerContext>],
        clientExtensions: [clientType as unknown as ReviverType<TypeContract, ClientReviverContext>],
      },
    )
    const retTyped = ret as { a: ReadableStream<Uint8Array>; b: StreamSource }
    const reader = retTyped.a.getReader()
    await reader.read()
    const pending = reader.read()
    // While `a` waits, the demuxer reads all of `b`, its done frame included, into b's buffer.
    await new Promise((resolve) => setTimeout(resolve, 50))
    releaseA()
    await pending
    retTyped.b.cancel()
    expect(await retTyped.b.readNextChunk()).toBe(null)
  })

  test.each([STREAM_TRANSPORT.BINARY_INLINE, STREAM_TRANSPORT.SSE_INLINE])(
    '%s: a body that drops after one inline stream finished leaves no unhandled rejection',
    async (streamTransport) => {
      const encode = (text: string) => new TextEncoder().encode(text) as Uint8Array<ArrayBuffer>
      const unhandled: unknown[] = []
      const onUnhandled = (reason: unknown) => unhandled.push(reason)
      process.on('unhandledRejection', onUnhandled)
      try {
        let drop!: (error: Error) => void
        const network = new TransformStream<Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>>({
          start: (controller) => void (drop = (error) => controller.error(error)),
        })
        const a = new ReadableStream<Uint8Array<ArrayBuffer>>({
          start(controller) {
            controller.enqueue(encode('a1'))
            controller.close()
          },
        })
        const b = new ReadableStream<Uint8Array<ArrayBuffer>>({
          start: (controller) => controller.enqueue(encode('b1')),
        })
        const { ret } = await roundTrip({ a, b }, { streamTransport, network })
        const streams = ret as { a: ReadableStream<Uint8Array>; b: ReadableStream<Uint8Array> }
        const readerA = streams.a.getReader()
        while (!(await readerA.read()).done);
        const readerB = streams.b.getReader()
        await readerB.read()
        drop(new TypeError('network error'))
        await expect(readerB.read()).rejects.toThrow('network error')
        await new Promise((resolve) => setTimeout(resolve, 0))
        expect(unhandled).toEqual([])
      } finally {
        process.off('unhandledRejection', onUnhandled)
      }
    },
  )

  test.each([STREAM_TRANSPORT.BINARY_INLINE, STREAM_TRANSPORT.SSE_INLINE])(
    '%s: aborting a call whose body dropped leaves no unhandled rejection',
    async (streamTransport) => {
      const unhandled: unknown[] = []
      const onUnhandled = (reason: unknown) => unhandled.push(reason)
      process.on('unhandledRejection', onUnhandled)
      // Node rethrows a rejected promise an event listener returns as an uncaught exception.
      process.on('uncaughtException', onUnhandled)
      try {
        let drop!: (error: Error) => void
        const network = new TransformStream<Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>>({
          start: (controller) => void (drop = (error) => controller.error(error)),
        })
        const { abortController } = await roundTrip({ b: new ReadableStream() }, { streamTransport, network })
        drop(new TypeError('network error'))
        abortController.abort()
        // The error reaches the client's body through the network stream's pipe, a few turns later.
        await new Promise((resolve) => setTimeout(resolve, 20))
        expect(unhandled).toEqual([])
      } finally {
        process.off('unhandledRejection', onUnhandled)
        process.off('uncaughtException', onUnhandled)
      }
    },
  )

  test('a tee() branch keeps a returned stream open after the stream itself is dropped', async () => {
    let controller!: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>
    const branch = await teeAndDrop(new ReadableStream({ start: (c) => void (controller = c) }))
    for (let cycle = 0; cycle < 8; cycle++) {
      ;(globalThis as { gc(): void }).gc()
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    controller.enqueue(new TextEncoder().encode('tail'))
    controller.close()
    expect(await new Response(branch).text()).toBe('tail')
  })
})

// ───────────────────────────────────────────────────────────────────────────
// Late extension-type registration
// Extension wire types registered while the target telefunc module loads (i.e.
// after request routing, before args are revived / the result is serialized)
// must be visible to that same request — request types are read at resolve time
// and response types at serialize time, not snapshotted at request start.
// ───────────────────────────────────────────────────────────────────────────

describe('extension wire types registered while a telefunc module loads', () => {
  const EXTENSION_NAME = 'late-extension-types-spec'
  const REQUEST_PREFIX = '!LateRequestSpec:'
  const RESPONSE_PREFIX = '!LateResponseSpec:'

  const requestReviver: ReviverType<
    TypeContract<unknown, { revived: string }, { mark: string }>,
    ServerReviverContext
  > = {
    prefix: REQUEST_PREFIX,
    revive(metadata) {
      return { value: { revived: metadata.mark }, close() {}, abort() {} }
    },
  }

  const responseReplacer: ReplacerType<
    TypeContract<{ __lateResponse: true; mark: string }, unknown, { mark: string }>,
    ServerReplacerContext
  > = {
    prefix: RESPONSE_PREFIX,
    detect(value): value is { __lateResponse: true; mark: string } {
      return typeof value === 'object' && value !== null && '__lateResponse' in value
    },
    replace(value) {
      return { metadata: { mark: value.mark }, close() {}, abort() {} }
    },
  }

  function registerExtension() {
    config.extensions.push({
      name: EXTENSION_NAME,
      requestTypes: [requestReviver as ReviverType<TypeContract, ServerReviverContext>],
      responseTypes: [responseReplacer as ReplacerType<TypeContract, ServerReplacerContext>],
    })
  }

  function unregisterExtension() {
    const index = config.extensions.findIndex((extension) => extension.name === EXTENSION_NAME)
    if (index >= 0) config.extensions.splice(index, 1)
  }

  test('transforms request values with requestTypes registered after routing', async () => {
    const marker = { __lateRequest: true, mark: 'request-mark' }
    const ordinaryPrefixedString = `${REQUEST_PREFIX}ordinary-string`
    const date = new Date('2026-07-23T00:00:00.000Z')
    const body = stringify(
      {
        file: '/spec/Late.telefunc.ts',
        name: 'onLate',
        args: [marker, ordinaryPrefixedString, date],
      },
      {
        replacer(_key, value, serializer) {
          if (value !== marker) return undefined
          return {
            replacement: REQUEST_PREFIX + serializer({ mark: marker.mark }),
            resolved: true,
          }
        },
      },
    )
    const request = new Request('http://localhost/_telefunc', { method: 'POST', body })
    const requestContext = createRequestContext(request)

    const parsed = await parseHttpRequest({
      request,
      requestContext,
      logMalformedRequests: false,
      serverConfig: getServerConfig(),
    })
    expect(parsed.isMalformedRequest).toBe(false)
    if (parsed.isMalformedRequest || parsed.isSseRequest) throw new Error('expected a telefunction request')

    // Registration happens after routing/parse — exactly the window the target module loads in.
    registerExtension()
    try {
      const resolved = parsed.resolveRequest((() => undefined) as never)
      expect(resolved.isMalformedRequest).toBe(false)
      if (resolved.isMalformedRequest) throw new Error('expected a resolved request')

      expect(resolved.telefunctionArgs[0]).toEqual({ revived: 'request-mark' })
      expect(resolved.telefunctionArgs[1]).toBe(ordinaryPrefixedString)
      expect(resolved.telefunctionArgs[2]).toEqual(date)
    } finally {
      unregisterExtension()
    }
  })

  test('serializes response values with responseTypes registered after the request config snapshot', () => {
    const requestStartConfig = getServerConfig()
    registerExtension()
    try {
      const requestContext = createRequestContext(new Request('http://localhost/_telefunc', { method: 'POST' }))

      const result = serializeTelefunctionResult({
        telefunctionReturn: { __lateResponse: true, mark: 'response-mark' },
        telefunctionName: 'onLate',
        telefuncFilePath: '/spec/Late.telefunc.ts',
        telefunctionAborted: false,
        context: {},
        requestContext,
        abortSignal: requestContext.abortSignal,
        streamTransport: STREAM_TRANSPORT.BINARY_INLINE,
        useNodeStream: false,
        serverConfig: requestStartConfig,
      })

      expect(result.type).toBe('text')
      if (result.type !== 'text') throw new Error('expected a text response')
      expect(result.body).toContain(RESPONSE_PREFIX)
    } finally {
      unregisterExtension()
    }
  })
})
