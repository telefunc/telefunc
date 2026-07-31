import { describe, expect, test } from 'vitest'

import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { createStreamingReplacer } from './server/response/registry.js'
import { createStreamingReviver } from './client/response/registry.js'
import { ServerChannel } from './server/channel.js'
import { ServerBroadcast } from './server/server-broadcast.js'
import { wrapProxy } from './wrapProxy.js'
import { serializeTelefunctionResult } from '../node/server/runTelefunc/serializeTelefunctionResult.js'
import { createRequestContext } from '../node/server/context/requestContext.js'
import { parseResponse } from './client/response/parse.js'
import { STREAM_TRANSPORT } from './constants.js'
import type { AbortError } from '../shared/Abort.js'
import type {
  ClientReviverContext,
  ReplacerType,
  ReviverType,
  ServerReplacerContext,
  StreamingProducer,
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
  const context: ServerReplacerContext = {
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
  const context: ClientReviverContext = {
    adoptSubordinate() {},
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
  } = {},
) {
  const requestContext = createRequestContext(new Request('http://localhost/_telefunc', { method: 'POST' }))
  const result = serializeTelefunctionResult({
    telefunctionReturn,
    telefunctionName: 'testFn',
    telefuncFilePath: '/pages/spec/ref-identity.telefunc.ts',
    telefunctionAborted: false,
    context: {},
    requestContext,
    abortSignal: requestContext.abortSignal,
    streamTransport: STREAM_TRANSPORT.BINARY_INLINE,
    useNodeStream: false,
    serverConfig: {
      extensionResponseTypes: opts.serverExtensions ?? [],
      log: { shieldErrors: { dev: false, prod: false } },
    },
  })
  const response =
    result.type === 'text'
      ? new Response(result.body)
      : new Response(result.body as ReadableStream<Uint8Array<ArrayBuffer>>, {
          headers: { 'content-type': 'application/octet-stream' },
        })
  const abortController = new AbortController()
  const parsed = (await parseResponse(response, {
    telefunctionName: 'testFn',
    telefuncFilePath: '/pages/spec/ref-identity.telefunc.ts',
    abortController,
    channel: { transports: ['sse'] },
    requestCloseHandlers: [],
    extensionResponseTypes: opts.clientExtensions ?? [],
    headers: null,
    telefuncUrl: 'http://localhost/_telefunc',
  })) as { ret: unknown }
  return { ret: parsed.ret, abortController }
}

async function collect<T>(gen: AsyncGenerator<T>): Promise<T[]> {
  const out: T[] = []
  for await (const chunk of gen) out.push(chunk)
  return out
}

describe('reference identity — full pipeline', () => {
  test('duplicated async generator: one producer, one client object, chunks delivered once', async () => {
    const gen = (async function* () {
      yield 1
      yield 2
      yield 3
    })()
    let upstreamCancelled = false
    const pending = new ReadableStream({ cancel: () => void (upstreamCancelled = true) })
    const { ret } = await roundTrip({ gen, genDupe: gen, pending })
    const retTyped = ret as { gen: AsyncGenerator<number>; genDupe: AsyncGenerator<number> }

    expect(retTyped.gen).toBe(retTyped.genDupe)
    // One consumer sees every chunk — duplicated producers used to steal chunks
    // from one another (each occurrence pulled the same underlying generator).
    expect(await collect(retTyped.gen)).toEqual([1, 2, 3])
    await (ret as { pending: ReadableStream }).pending.cancel()
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
})
