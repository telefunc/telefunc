import { afterEach, assert, beforeEach, expect, test, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { concat, encodeIndexedFrame, encodeU32, textEncoder } from '../../wire/envelope.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'
import type { ClientReplacerContext } from '../types.js'

type ClientChannelModule = typeof import('../../channel/client/ClientChannel.js')
type LoadFeaturesModule = typeof import('./loadFeatures.js')

// A page's first call that carries a value loads that value's code: each chunk's load is held here till a test lets it
// finish, or fails, as the chunk's request would.
const FEATURES = ['streams', 'channels', 'broadcasts'] as const
type Feature = (typeof FEATURES)[number]

let gates: Record<Feature, { promise: Promise<void>; open: () => void }>
let failing: Set<Feature>
let imported: Feature[]
/** The chunk loads a test started, each settled before the next test's fresh modules. */
let loads: Promise<unknown>[]
let posts: string[]
let respond: (signal: AbortSignal | undefined) => Response
let cancelled: number

beforeEach(() => {
  Reflect.deleteProperty(globalThis, '_telefunc')
  vi.resetModules()
  failing = new Set()
  imported = []
  loads = []
  posts = []
  cancelled = 0
  gates = {
    streams: newGate(),
    channels: newGate(),
    broadcasts: newGate(),
  }
  // The client runs in a browser only.
  vi.stubGlobal('window', {
    scrollY: 0,
    location: { href: 'http://localhost/' },
    // The page's global object.
    get fetch() {
      return globalThis.fetch
    },
    addEventListener() {},
    removeEventListener() {},
  })
  for (const feature of FEATURES) {
    vi.doMock(`./${feature}.js`, (importOriginal: () => Promise<unknown>) => {
      imported.push(feature)
      const load = (async () => {
        await gates[feature].promise
        if (failing.delete(feature)) throw new Error(`The ${feature} chunk failed to load`)
        return importOriginal()
      })()
      loads.push(load)
      return load
    })
  }
})
afterEach(async () => {
  // A load a test left held finishes before the next test's fresh modules.
  for (const feature of FEATURES) gates[feature].open()
  await Promise.allSettled(loads)
  for (const feature of FEATURES) vi.doUnmock(`./${feature}.js`)
  vi.doUnmock('./loadFeatures.js')
  vi.unstubAllGlobals()
})

function newGate() {
  let open = () => {}
  const promise = new Promise<void>((resolve) => (open = resolve))
  return { promise, open }
}

async function page() {
  const { config } = await import('../../../client/clientConfig.js')
  config.fetch = async (_url, init) => {
    const body = init?.body
    if (typeof body === 'string' && body.includes('"file"')) {
      posts.push(body)
      return respond(init?.signal ?? undefined)
    }
    // A channel's own requests: there is no server.
    return new Promise<Response>(() => {})
  }
  const { remoteTelefunctionCall } = await import('../../../client/remoteTelefunctionCall.js')
  const { withContext } = await import('../../../client/withContext.js')
  const { ConnectionError } = await import('../../../client/ConnectionError.js')
  const call = (args: unknown[]) => remoteTelefunctionCall('/p.telefunc.ts', 'onP', args)
  return { config, call, withContext, ConnectionError }
}

const isAbort = (err: unknown) => typeof err === 'object' && err !== null && 'isAbort' in err && err.isAbort === true
const settledWithin = (promise: Promise<unknown>, ms: number) =>
  Promise.race([
    promise.then(
      (value) => value,
      (err: unknown) => err,
    ),
    new Promise((resolve) => setTimeout(() => resolve('pending'), ms)),
  ])

const tag = (prefix: string, metadata: object) => prefix + JSON.stringify(metadata)
const CHANNEL = tag('!TelefuncChannel:', { channelId: 'c1' })
const GENERATOR = tag('!TelefuncGenerator:', { __index: 0 })

/** A text response, its values as the server writes them. */
const text = (ret: unknown) => () => new Response(JSON.stringify({ ret }))

/** A streamed response whose stream 0 sends `42` and stays open; a fetch that is aborted errors it, as browsers do. */
function streamed(
  ret: Record<string, string>,
  { sse = false, frames = [encodeIndexedFrame(0, textEncoder.encode('42'))], onCancel = () => {} } = {},
) {
  return (signal: AbortSignal | undefined) => {
    const metadata = textEncoder.encode(JSON.stringify({ ret }))
    let sent = false
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        signal?.addEventListener('abort', () => controller.error(new DOMException('aborted', 'AbortError')))
      },
      pull(controller) {
        if (sent) return
        sent = true
        const bytes = concat(encodeU32(metadata.length), metadata, ...frames)
        controller.enqueue(sse ? textEncoder.encode(`data: ${uint8ArrayToBase64url(bytes)}\n\n`) : bytes)
      },
      cancel() {
        cancelled++
        onCancel()
      },
    })
    return new Response(body, { headers: { 'content-type': sse ? 'text/event-stream' : 'application/octet-stream' } })
  }
}

/** A streamed response of an app's `config.fetch` whose body outlives the call's signal and sends nothing yet. */
function liveResponse(sse: boolean, onCancel: () => void) {
  const body = new ReadableStream({ cancel: onCancel })
  return new Response(body, { headers: { 'content-type': sse ? 'text/event-stream' : 'application/octet-stream' } })
}

/** A call that loads the streams chunk. */
async function warmStreams(call: (args: unknown[]) => Promise<unknown>) {
  gates.streams.open()
  respond = streamed({ gen: GENERATOR })
  await call([])
}

test('a failed chunk load rejects the call with ConnectionError, cancels its body, and the next call loads it', async () => {
  const { call, ConnectionError } = await page()
  respond = streamed({ gen: GENERATOR })
  failing.add('streams')
  gates.streams.open()
  expect(await settledWithin(call([]), 1000)).toBeInstanceOf(ConnectionError)
  expect(cancelled).toBe(1)
  const ret = await call([])
  expect(ret).toHaveProperty('gen')
})

test.each([
  ['the channel', false],
  ['the generator', false],
  ['the channel', true],
  ['the generator', true],
] as const)(
  'a load that fails after the streamed body is read cancels the body once and releases its reader (%s first, SSE: %s)',
  async (first, sse) => {
    const { call, ConnectionError } = await page()
    gates.streams.open()
    const ret = first === 'the channel' ? { channel: CHANNEL, gen: GENERATOR } : { gen: GENERATOR, channel: CHANNEL }
    let ownCancels = 0
    const response = streamed(ret, { sse, onCancel: () => ownCancels++ })(undefined)
    respond = () => response
    failing.add('channels')
    gates.channels.open()

    await expect(call([])).rejects.toBeInstanceOf(ConnectionError)
    expect(ownCancels).toBe(1)
    assert(response.body)
    expect(response.body.locked).toBe(false)
    const reader = response.body.getReader()
    try {
      expect(await reader.read()).toEqual({ done: true, value: undefined })
    } finally {
      reader.releaseLock()
    }
  },
)

test.each([false, true])(
  'a streamed response releases its reader once its values read all they carried (SSE: %s)',
  async (sse) => {
    const { call } = await page()
    const { close } = await import('../../../client/close.js')
    gates.streams.open()
    const frames = [
      encodeIndexedFrame(0, textEncoder.encode('41')),
      encodeIndexedFrame(0, textEncoder.encode('42')),
      encodeIndexedFrame(0, new Uint8Array()),
      encodeU32(0),
    ]
    // Counted for this response alone: an earlier test's body may be cancelled as its values are collected.
    let ownCancels = 0
    const response = streamed({ gen: GENERATOR }, { sse, frames, onCancel: () => ownCancels++ })(undefined)
    respond = () => response
    const ret = await call([])
    assertHas(ret, 'gen')
    assert(isAsyncIterable(ret.gen))
    expect(await Array.fromAsync(ret.gen)).toEqual([41, 42])
    assert(response.body)
    expect(response.body.locked).toBe(false)
    await close(ret)
    expect(response.body.locked).toBe(false)
    expect(ownCancels).toBe(0)
  },
)

test('a call aborted while its chunk loads rejects at once, and leaves no unhandled rejection', async () => {
  const { call, withContext } = await page()
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  try {
    respond = streamed({ gen: GENERATOR })
    const controller = new AbortController()
    const pending = withContext(call, { signal: controller.signal })([])
    await vi.waitFor(() => expect(imported).toEqual(['streams']))
    controller.abort()
    expect(isAbort(await settledWithin(pending, 200))).toBe(true)
    await new Promise((resolve) => setTimeout(resolve, 20))
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
  expect(unhandled).toEqual([])
})

test("a call aborted while its chunk loads cancels the body of an app's fetch that doesn't end it", async () => {
  const { call, withContext } = await page()
  let ownCancels = 0
  // An app's `config.fetch` whose body outlives the call's signal: only the call's own cancel stops it.
  respond = () => streamed({ gen: GENERATOR }, { onCancel: () => ownCancels++ })(undefined)
  const controller = new AbortController()
  const pending = withContext(call, { signal: controller.signal })([])
  await vi.waitFor(() => expect(imported).toEqual(['streams']))
  controller.abort()
  expect(isAbort(await settledWithin(pending, 200))).toBe(true)
  expect(ownCancels).toBe(1)
})

test.each([false, true])(
  "a call aborted as its app's fetch hands over a live body, streams' code loaded, cancels it once and lets it go (SSE: %s)",
  async (sse) => {
    const { call, withContext } = await page()
    await warmStreams(call)
    // Each microtask from the fetch's return to the reader's first read.
    for (let ticks = 0; ticks <= 16; ticks++) {
      let ownCancels = 0
      const response = liveResponse(sse, () => ownCancels++)
      const controller = new AbortController()
      respond = () => {
        void abortAfterTicks(controller, ticks)
        return response
      }
      const settled = await settledWithin(withContext(call, { signal: controller.signal })([]), 200)
      expect(isAbort(settled), `aborted after ${ticks} microtasks`).toBe(true)
      expect(ownCancels).toBe(1)
      assert(response.body)
      expect(response.body.locked).toBe(false)
    }
  },
)

test.each([false, true])(
  "a call aborted as its app's fetch reads out the response's first bytes, streams' code loaded, rejects with Abort (SSE: %s)",
  async (sse) => {
    const { call, withContext } = await page()
    await warmStreams(call)
    const controller = new AbortController()
    let ownCancels = 0
    const metadata = textEncoder.encode(JSON.stringify({ ret: { gen: GENERATOR } }))
    const bytes = concat(encodeU32(metadata.length), metadata, encodeIndexedFrame(0, textEncoder.encode('42')))
    // Pulled at the reader's first read.
    const body = new ReadableStream<Uint8Array>(
      {
        pull(sink) {
          sink.enqueue(sse ? textEncoder.encode(`data: ${uint8ArrayToBase64url(bytes)}\n\n`) : bytes)
          controller.abort()
        },
        cancel() {
          ownCancels++
        },
      },
      { highWaterMark: 0 },
    )
    const response = new Response(body, {
      headers: { 'content-type': sse ? 'text/event-stream' : 'application/octet-stream' },
    })
    respond = () => response
    const settled = await settledWithin(withContext(call, { signal: controller.signal })([]), 200)
    expect(isAbort(settled), String(settled)).toBe(true)
    expect(ownCancels).toBe(1)
    expect(response.body?.locked).toBe(false)
  },
)

test.each(['a streamed response', 'a text response', 'a claimed upload'] as const)(
  'a call aborted once its chunk loaded, before it resumes, rejects without using it (%s)',
  async (kind) => {
    abortOnceLoaded()
    const { call, withContext } = await page()
    let ownCancels = 0
    const response = liveResponse(false, () => ownCancels++)
    respond = kind === 'a streamed response' ? () => response : text(CHANNEL)
    const upload = new ReadableStream({
      cancel() {
        ownCancels++
      },
    })
    const controller = new AbortController()
    for (const feature of FEATURES) gates[feature].open()
    const pending = withContext(call, { signal: controller.signal })(kind === 'a claimed upload' ? [upload] : [])
    expect(isAbort(await settledWithin(pending, 5000))).toBe(true)
    expect(posts).toHaveLength(kind === 'a claimed upload' ? 0 : 1)
    if (kind !== 'a text response') expect(ownCancels).toBe(1)
    assert(response.body)
    expect(response.body.locked).toBe(false)
  },
)

test('a call aborted while its text response waits for its chunk rejects at once, telling onAbort() once', async () => {
  const { call, withContext } = await page()
  const { onAbort } = await import('../../../client/remoteTelefunctionCall/onAbort.js')
  let told = 0
  onAbort(() => told++)
  respond = text(CHANNEL)
  const controller = new AbortController()
  const pending = withContext(call, { signal: controller.signal })([])
  await vi.waitFor(() => expect(imported).toEqual(['channels']))
  controller.abort()
  expect(isAbort(await settledWithin(pending, 200))).toBe(true)
  expect(told).toBe(1)
})

test("a call aborted while its text response's body stalls, channels' code loaded, cancels it once and opens no channel", async () => {
  const { config, call, withContext } = await page()
  gates.channels.open()
  respond = text(CHANNEL)
  await call([])
  const used: string[] = []
  config.fetch = recordingFetch('page', used)
  let pulled = false
  let ownCancels = 0
  // The body of an app's `config.fetch` that the call's signal doesn't end.
  const body = new ReadableStream<Uint8Array>(
    {
      pull() {
        pulled = true
        return new Promise(() => {})
      },
      cancel() {
        ownCancels++
      },
    },
    { highWaterMark: 0 },
  )
  const response = new Response(body, { headers: { 'content-type': 'text/plain' } })
  respond = () => response
  const controller = new AbortController()
  const connectionKey = crypto.randomUUID()
  const pending = withContext(call, { signal: controller.signal, channel: { connectionKey } })([])
  await vi.waitFor(() => expect(pulled).toBe(true))
  controller.abort()
  expect(isAbort(await settledWithin(pending, 200))).toBe(true)
  expect(ownCancels).toBe(1)
  expect(response.body?.locked).toBe(false)
  await new Promise((resolve) => setTimeout(resolve, 20))
  expect(used).toEqual(['page:call'])
})

test.each(["the call's cancel ends it", 'the abort errors it, as a fetch does'] as const)(
  "a call aborted while its text response's body stalls tells onAbort() once, the error it rejects with (%s)",
  async (ending) => {
    const { call, withContext } = await page()
    const { onAbort } = await import('../../../client/remoteTelefunctionCall/onAbort.js')
    const told: unknown[] = []
    onAbort((err) => told.push(err))
    let pulled = false
    respond = (signal) => {
      const body = new ReadableStream<Uint8Array>(
        {
          start(controller) {
            if (ending === 'the abort errors it, as a fetch does') {
              signal?.addEventListener('abort', () => controller.error(new DOMException('aborted', 'AbortError')))
            }
          },
          pull() {
            pulled = true
            return new Promise(() => {})
          },
        },
        { highWaterMark: 0 },
      )
      return new Response(body, { headers: { 'content-type': 'text/plain' } })
    }
    const controller = new AbortController()
    const pending = withContext(call, { signal: controller.signal })([])
    await vi.waitFor(() => expect(pulled).toBe(true))
    controller.abort()
    const rejected = await settledWithin(pending, 200)
    expect(isAbort(rejected)).toBe(true)
    expect(told).toHaveLength(1)
    expect(told[0]).toBe(rejected)
  },
)

test('a cold upload aborted as its channel opens sends nothing and cancels the upload once', async () => {
  const controller = new AbortController()
  vi.doMock('../../channel/client/ClientChannel.js', async (importOriginal: () => Promise<ClientChannelModule>) => {
    const original = await importOriginal()
    class AbortingChannel<A = unknown, B = unknown> extends original.ClientChannel<A, B> {
      constructor(options: ConstructorParameters<typeof original.ClientChannel>[0]) {
        super(options)
        controller.abort()
      }
    }
    return { ...original, ClientChannel: AbortingChannel }
  })
  try {
    const { call, withContext } = await page()
    respond = text(1)
    let sourceCancelled = 0
    const upload = new ReadableStream({
      cancel() {
        sourceCancelled++
      },
    })
    const pending = withContext(call, { signal: controller.signal })([upload])
    gates.channels.open()
    expect(isAbort(await settledWithin(pending, 200))).toBe(true)
    expect(posts).toEqual([])
    expect(sourceCancelled).toBe(1)
    expect(upload.locked).toBe(false)
  } finally {
    vi.doUnmock('../../channel/client/ClientChannel.js')
  }
})

test('a call with an upload aborted while channels load sends nothing and cancels the upload, as a sent one does', async () => {
  const { call, withContext } = await page()
  let sourceCancelled = 0
  const upload = new ReadableStream({
    cancel() {
      sourceCancelled++
    },
  })
  const controller = new AbortController()
  const pending = withContext(call, { signal: controller.signal })([upload])
  await vi.waitFor(() => expect(imported).toEqual(['channels']))
  controller.abort()
  expect(isAbort(await settledWithin(pending, 200))).toBe(true)
  expect(posts).toEqual([])
  expect(sourceCancelled).toBe(1)
})

test('an already-aborted upload cancels its source without starting a chunk load', async () => {
  const { call, withContext } = await page()
  const cancel = vi.fn()
  const upload = new ReadableStream({ cancel })
  const controller = new AbortController()
  controller.abort()
  failing.add('channels')
  gates.channels.open()

  expect(isAbort(await settledWithin(withContext(call, { signal: controller.signal })([upload]), 200))).toBe(true)
  expect(cancel).toHaveBeenCalledOnce()
  expect(posts).toEqual([])
  expect(imported).toEqual([])
})

test('a callback call sends the values its invocation read', async () => {
  const { call } = await page()
  respond = text(1)
  let n = 10
  const pending = call([
    {
      get read() {
        return n
      },
      callback() {},
    },
  ])
  n = 20
  gates.channels.open()
  await pending
  expect(posts).toHaveLength(1)
  expect(JSON.parse(posts[0] ?? '').args[0].read).toBe(10)
})

test.each(['headers', 'httpHeaders'] as const)(
  "a cold callback call and the wire its channel opens send the config.%s of the call's invocation",
  async (option) => {
    const { config, call } = await page()
    const sent: string[] = []
    config.fetch = async (_url, init) => {
      sent.push(new Headers(init?.headers).get('x-workspace') ?? 'none')
      const body = init?.body
      if (typeof body === 'string' && body.includes('"file"')) return text(1)()
      return new Promise<Response>(() => {})
    }
    respond = text(1)
    const headers = { 'x-workspace': 'before' }
    config[option] = headers
    const pending = call([{ callback() {} }])
    headers['x-workspace'] = 'after'
    gates.channels.open()
    await pending
    await vi.waitFor(() => expect(sent.length).toBeGreaterThanOrEqual(2))
    expect(new Set(sent)).toEqual(new Set(['before']))
  },
)

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g
/** A callback, an upload and plain data: what a call carries over channels of their own, and beside them. */
const carried = () => [{ n: 1, callback() {} }, new ReadableStream(), '!TelefuncFunction:app']

test("a cold callback and upload call sends the body a page with channels' code sends, but for the channel ids", async () => {
  const { call } = await page()
  respond = text(1)
  const cold = call(carried())
  gates.channels.open()
  await cold
  await call(carried())
  expect(imported).toEqual(['channels'])
  const [coldBody = '', warmBody = ''] = posts
  expect(coldBody.match(UUID)).toHaveLength(2)
  expect(coldBody.replace(UUID, 'id')).toBe(warmBody.replace(UUID, 'id'))
})

test("a cold upload's call and the wire its channel opens use the fetch of the call's invocation", async () => {
  const { config, call } = await page()
  const used: string[] = []
  respond = text(1)
  config.fetch = recordingFetch('before', used)
  const pending = call([new ReadableStream()])
  config.fetch = recordingFetch('after', used)
  gates.channels.open()
  await pending
  await vi.waitFor(() => expect(used).toContain('before:wire'))
  expect(used.filter((use) => use.startsWith('after'))).toEqual([])
  expect(used).toContain('before:call')
})

test("without config.fetch, a cold upload's call and the wire its channel opens use the page's fetch of the call's invocation", async () => {
  const used: string[] = []
  respond = text(1)
  vi.stubGlobal('fetch', recordingFetch('before', used))
  const { remoteTelefunctionCall } = await import('../../../client/remoteTelefunctionCall.js')
  const pending = remoteTelefunctionCall('/p.telefunc.ts', 'onP', [new ReadableStream()])
  vi.stubGlobal('fetch', recordingFetch('after', used))
  gates.channels.open()
  await pending
  await vi.waitFor(() => expect(used).toContain('before:wire'))
  expect(used.filter((use) => use.startsWith('after'))).toEqual([])
  expect(used).toContain('before:call')
})

test("a second client module instance's channels open with its own fetch", async () => {
  const first = await page()
  respond = text(1)
  gates.channels.open()
  await first.call([{ callback() {} }])
  vi.resetModules()
  const second = await page()
  const used: string[] = []
  second.config.fetch = recordingFetch('second', used)
  const connectionKey = crypto.randomUUID()
  await second.withContext(second.call, { channel: { connectionKey } })([{ callback() {} }])
  await vi.waitFor(() => expect(used).toContain('second:wire'))
  expect(used).toContain('second:call')
})

test('the channel a cold call opens is the one its tag names', async () => {
  const opened: string[] = []
  vi.doMock('../../channel/client/ClientChannel.js', async (importOriginal: () => Promise<ClientChannelModule>) => {
    const original = await importOriginal()
    class RecordedChannel<A = unknown, B = unknown> extends original.ClientChannel<A, B> {
      constructor(options: ConstructorParameters<typeof original.ClientChannel>[0]) {
        super(options)
        opened.push(this.id)
      }
    }
    return { ...original, ClientChannel: RecordedChannel }
  })
  try {
    const { call } = await page()
    respond = text(1)
    const pending = call(carried())
    gates.channels.open()
    await pending
    const [callback, upload] = JSON.parse(posts[0] ?? '').args
    const tagged = [callback.callback, upload].map(
      (tag: string) => JSON.parse(tag.slice(tag.indexOf(':') + 1)).channelId,
    )
    expect(opened.toSorted()).toEqual(tagged.toSorted())
  } finally {
    vi.doUnmock('../../channel/client/ClientChannel.js')
  }
})

test("a cold callback's channel is the call's once: the call's abort aborts it once", async () => {
  let aborts = 0
  vi.doMock('../../channel/client/ClientChannel.js', async (importOriginal: () => Promise<ClientChannelModule>) => {
    const original = await importOriginal()
    class CountedChannel<A = unknown, B = unknown> extends original.ClientChannel<A, B> {
      override abort(abortValue?: unknown, message?: string) {
        aborts++
        super.abort(abortValue, message)
      }
    }
    return { ...original, ClientChannel: CountedChannel }
  })
  try {
    const { call, withContext } = await page()
    respond = text(1)
    const controller = new AbortController()
    const pending = withContext(call, { signal: controller.signal })([() => {}])
    gates.channels.open()
    await pending
    controller.abort()
    expect(aborts).toBe(1)
  } finally {
    vi.doUnmock('../../channel/client/ClientChannel.js')
  }
})

/** An extension whose replacer creates a channel, or sends a stream, for its `{ kind: 'ask' }` values. */
function askExtension(method: 'createChannel' | 'sendStream') {
  return {
    name: 'ask',
    requestTypes: [
      {
        prefix: '!Ask:',
        detect: (value: unknown): value is { kind: 'ask' } =>
          typeof value === 'object' && value !== null && 'kind' in value && value.kind === 'ask',
        replace(_value: unknown, context: ClientReplacerContext) {
          if (method === 'sendStream') return context.sendStream(() => ({ chunks: oneChunk(31), cancel() {} }))
          const channel = context.createChannel({ ack: true })
          return {
            metadata: { channelId: channel.id },
            async close() {
              await channel.close()
            },
            abort: () => channel.abort(),
          }
        },
      },
    ],
  }
}

test.each(['createChannel', 'sendStream'] as const)(
  'an extension that imports telefunc/__internal replaces its value with %s() at invocation, in one POST',
  async (method) => {
    const { call, config } = await page()
    gates.channels.open()
    gates.broadcasts.open()
    // The extension's client code: its values create channels.
    await import('../../../client/__internal.js')
    imported = []
    config.extensions.push(askExtension(method))
    respond = text(1)
    const pending = call([{ kind: 'ask' }])
    expect(posts).toHaveLength(1)
    expect(JSON.parse(posts[0] ?? '').args[0]).toMatch(/^!Ask:\{"channelId":"[^"]+"\}$/)
    expect(await pending).toBe(1)
    expect(imported).toEqual([])
  },
)

test.each(['createChannel', 'sendStream'] as const)(
  'an extension whose replacer calls %s() without importing telefunc/__internal gets a usage error naming it',
  async (method) => {
    const { call, config } = await page()
    config.extensions.push(askExtension(method))
    expect(() => call([{ kind: 'ask' }])).toThrow("the extension's client code should `import 'telefunc/__internal'`")
    expect(posts).toEqual([])
    expect(imported).toEqual([])
  },
)

test('an extension value of metadata only is replaced once, at invocation, and its call sent at once', async () => {
  const { call, config } = await page()
  let detected = 0
  config.extensions.push({
    name: 'quantity',
    requestTypes: [
      {
        prefix: '!Quantity:',
        detect: (value: unknown): value is { kind: 'order'; quantity: number } => {
          const is = typeof value === 'object' && value !== null && 'kind' in value && value.kind === 'order'
          if (is) detected++
          return is
        },
        replace: (value: { quantity: number }) => ({ metadata: { quantity: value.quantity }, close() {}, abort() {} }),
      },
    ],
  })
  respond = text(1)
  const order = { kind: 'order', quantity: 1 }
  const pending = call([order])
  order.quantity = 2
  expect(posts).toHaveLength(1)
  expect(await pending).toBe(1)
  expect(JSON.parse(posts[0] ?? '').args[0]).toBe('!Quantity:{"quantity":1}')
  expect(detected).toBe(1)
  expect(imported).toEqual([])
})

test("a cold upload is the call's from its invocation: locked at return, then sent", async () => {
  const { call } = await page()
  respond = text(1)
  const upload = new ReadableStream({
    pull(controller) {
      controller.close()
    },
  })
  const pending = call([upload])
  expect(upload.locked).toBe(true)
  expect(() => upload.getReader()).toThrow()
  gates.channels.open()
  expect(await pending).toBe(1)
  expect(posts).toHaveLength(1)
})

test('serialization failure at invocation cancels the upload an earlier argument claimed', async () => {
  const { call, withContext } = await page()
  const cancel = vi.fn()
  const upload = new ReadableStream({ cancel })
  const second = new ReadableStream()
  const secondReader = second.getReader()
  const controller = new AbortController()

  try {
    expect(() => withContext(call, { signal: controller.signal })([upload, second])).toThrow(/locked/)
    expect(cancel).toHaveBeenCalledOnce()
    expect(upload.locked).toBe(false)
    controller.abort()
    expect(cancel).toHaveBeenCalledOnce()
    expect(posts).toEqual([])
    expect(imported).toEqual([])
  } finally {
    secondReader.releaseLock()
  }
})

test("a cold upload whose source's cancel throws leaves no unhandled rejection", async () => {
  const { call, withContext } = await page()
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  try {
    const upload = new ReadableStream({
      cancel() {
        throw new Error('the source fails to cancel')
      },
    })
    const controller = new AbortController()
    const pending = withContext(call, { signal: controller.signal })([upload])
    await vi.waitFor(() => expect(imported).toEqual(['channels']))
    controller.abort()
    expect(isAbort(await settledWithin(pending, 200))).toBe(true)
    await new Promise((resolve) => setTimeout(resolve, 20))
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
  expect(unhandled).toEqual([])
})

test('a later invalid argument cancels a resource created by an extension replacer', async () => {
  const { call, config, withContext } = await page()
  const resource = {}
  const abort = vi.fn()
  config.extensions.push({
    name: 'resource',
    requestTypes: [
      {
        prefix: '!Resource:',
        detect: (value: unknown): value is object => value === resource,
        replace: () => ({ metadata: {}, close() {}, abort }),
      },
    ],
  })
  const controller = new AbortController()
  const cyclic: { self?: unknown } = {}
  cyclic.self = cyclic

  expect(() => withContext(call, { signal: controller.signal })([resource, cyclic])).toThrow()
  controller.abort()
  expect(abort).toHaveBeenCalledOnce()
  expect(posts).toEqual([])
})

test('a reply chunk load failure cancels the ongoing request upload', async () => {
  const { call, ConnectionError } = await page()
  const cancel = vi.fn()
  const upload = new ReadableStream({ cancel })
  gates.channels.open()
  failing.add('streams')
  gates.streams.open()
  respond = streamed({ gen: GENERATOR })

  await expect(call([upload])).rejects.toBeInstanceOf(ConnectionError)
  expect(cancel).toHaveBeenCalledOnce()
  expect(upload.locked).toBe(false)
  expect(posts).toHaveLength(1)
})

test('a returned promise stays a promise when another value of the response loads its chunk', async () => {
  const { call } = await page()
  await warmStreams(call)
  respond = streamed({ channel: CHANNEL, promise: tag('!TelefuncPromise:', { __index: 0 }) })
  const pending = call([])
  gates.channels.open()
  const ret = await pending
  assertHas(ret, 'promise')
  expect(ret.promise).toBeInstanceOf(Promise)
  expect(await ret.promise).toBe(42)
})

test('each call loads only the code the values it carries need', async () => {
  const { call } = await page()
  for (const feature of FEATURES) gates[feature].open()
  respond = streamed({ gen: GENERATOR })
  await call([])
  expect(imported).toEqual(['streams'])
  respond = text(CHANNEL)
  await call([])
  expect(imported).toEqual(['streams', 'channels'])
  respond = text(tag('!TelefuncBroadcast:', { channelId: 'b1', key: 'k' }))
  await call([])
  expect(imported).toEqual(['streams', 'channels', 'broadcasts'])
})

test('an extension that imports telefunc/__internal creates a broadcast and a channel from its response, loading no other code', async () => {
  const { call, config } = await page()
  gates.channels.open()
  gates.broadcasts.open()
  // The extension's client code: its values create channels and broadcasts.
  await import('../../../client/__internal.js')
  imported = []
  const created: string[] = []
  config.extensions.push({
    name: 'probe',
    responseTypes: [
      {
        prefix: '!TelefuncProbe:',
        revive(metadata: { channelId: string }, context) {
          created.push(context.createChannel({ channelId: metadata.channelId }).id)
          const value = context.createBroadcast({ channelId: 'b1', key: 'probe' })
          return {
            value,
            async close() {
              await value.close()
            },
            abort: () => value.abort(),
          }
        },
      },
    ],
  })
  respond = text(tag('!TelefuncProbe:', { channelId: 'p1' }))
  expect(await call([])).toHaveProperty('subscribe', expect.any(Function))
  expect(created).toEqual(['p1'])
  expect(imported).toEqual([])
})

test('an extension response whose values are their metadata loads no code', async () => {
  const { call, config } = await page()
  for (const feature of FEATURES) {
    failing.add(feature)
    gates[feature].open()
  }
  config.extensions.push({
    name: 'count',
    responseTypes: [
      {
        prefix: '!Count:',
        revive: (metadata: { count: number }) => ({ value: { count: metadata.count }, close() {}, abort() {} }),
      },
    ],
  })
  respond = text(tag('!Count:', { count: 7 }))
  expect(await call([])).toHaveProperty('count', 7)
  expect(imported).toEqual([])
})

test('a second client module instance gets native Files and Blobs from the shared feature cache', async () => {
  const first = await page()
  await warmStreams(first.call)
  vi.resetModules()
  const second = await page()
  respond = streamed(
    {
      file: tag('!TelefuncFile:', { __index: 0, size: 3, name: 'cat.txt', type: 'text/plain', lastModified: 12 }),
      blob: tag('!TelefuncBlob:', { __index: 1, size: 3, type: 'text/plain' }),
    },
    {
      frames: [
        encodeIndexedFrame(0, textEncoder.encode('cat')),
        encodeIndexedFrame(0, new Uint8Array()),
        encodeIndexedFrame(1, textEncoder.encode('dog')),
        encodeIndexedFrame(1, new Uint8Array()),
      ],
    },
  )

  const ret = await second.call([])
  assertHas(ret, 'file')
  assertHas(ret, 'blob')
  assert(ret.file instanceof File)
  assert(ret.blob instanceof Blob)
  expect(ret.file.name).toBe('cat.txt')
  expect(ret.file.lastModified).toBe(12)
  expect(await ret.file.text()).toBe('cat')
  expect(await ret.blob.text()).toBe('dog')
})

test.each<[Feature, string | Record<string, number>]>([
  ['channels', 'Documentation example: "!TelefuncChannel:sample'],
  ['broadcasts', { '!TelefuncBroadcast:category': 42 }],
  ['streams', 'Documentation example: "!TelefuncStream:sample'],
])('plain response data does not load %s even if its asset fails', async (feature, value) => {
  const { call } = await page()
  failing.add(feature)
  for (const group of FEATURES) gates[group].open()
  const body = stringify({ ret: value })
  respond = () => new Response(body)
  const jsonParse = vi.spyOn(JSON, 'parse')
  try {
    const ret = await call([value])
    if (typeof value === 'object') {
      assert(typeof ret === 'object' && ret !== null)
      expect(Object.fromEntries(Object.entries(ret))).toEqual(value)
    } else {
      expect(ret).toEqual(value)
    }
    expect(jsonParse.mock.calls.filter(([input]) => input === body)).toHaveLength(1)
  } finally {
    jsonParse.mockRestore()
  }
  expect(posts).toHaveLength(1)
  expect(imported).toEqual([])
})

test('plain RPC: sent at once, an app string that looks like a value loads nothing and comes back as it was', async () => {
  const { call } = await page()
  const value = ['!', '!Telefunc', CHANNEL, '!TelefuncRoom:x']
  respond = () => new Response(stringify({ ret: value }))
  const pending = call([value])
  expect(posts).toHaveLength(1)
  const ret = await pending
  assert(Array.isArray(ret))
  expect([...ret]).toEqual(value)
  expect(imported).toEqual([])
})

test('plain RPC: a response without a body stream, as fetch polyfills return, is read by its text()', async () => {
  const { call } = await page()
  const response = new Response(JSON.stringify({ ret: 'Welcome Eva' }), { headers: { 'content-type': 'text/plain' } })
  // `whatwg-fetch`, React Native's fetch: the response has no `body`, its `text()` reads it.
  Object.defineProperty(response, 'body', { value: null })
  respond = () => response
  expect(await call([])).toBe('Welcome Eva')
})

/** An app's `config.fetch` that records, in `used`, each request it makes as `<name>:call` or `<name>:wire`. */
function recordingFetch(name: string, used: string[]): typeof fetch {
  return async (_url, init) => {
    const body = init?.body
    if (typeof body === 'string' && body.includes('"file"')) {
      used.push(`${name}:call`)
      posts.push(body)
      return respond(init?.signal ?? undefined)
    }
    used.push(`${name}:wire`)
    return new Promise<Response>(() => {})
  }
}

async function abortAfterTicks(controller: AbortController, ticks: number) {
  for (let tick = 0; tick < ticks; tick++) await null
  controller.abort()
}

/** The page's chunk loads settle, then the call is aborted before its code resumes. */
function abortOnceLoaded() {
  vi.doMock('./loadFeatures.js', async (importOriginal: () => Promise<LoadFeaturesModule>) => {
    const original = await importOriginal()
    const loadFeatures: LoadFeaturesModule['loadFeatures'] = async (features, call) => {
      const loaded = await original.loadFeatures(features, call)
      call.abortController.abort()
      return loaded
    }
    return { ...original, loadFeatures }
  })
}

async function* oneChunk(byte: number) {
  yield new Uint8Array([byte])
}

function isAsyncIterable(value: unknown): value is AsyncIterable<unknown> {
  return typeof value === 'object' && value !== null && Symbol.asyncIterator in value
}

function assertHas<K extends string>(value: unknown, key: K): asserts value is Record<K, unknown> {
  expect(value).toHaveProperty(key)
}
