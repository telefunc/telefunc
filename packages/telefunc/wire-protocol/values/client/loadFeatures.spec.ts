import { afterEach, assert, beforeEach, expect, test, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { concat, encodeIndexedFrame, encodeU32, textEncoder } from '../../wire/envelope.js'
import { uint8ArrayToBase64url } from '../../wire/base64url.js'

// A page's first call that carries a value loads that value's code: each chunk's load is held here till a test lets it
// finish, or fails, as the chunk's request would.
const FEATURES = ['streams', 'channels', 'broadcasts'] as const
type Feature = (typeof FEATURES)[number]

let gates: Record<Feature, { promise: Promise<void>; open: () => void }>
let failing: Set<Feature>
let imported: Feature[]
let posts: string[]
let respond: (signal: AbortSignal | undefined) => Response
let cancelled: number

beforeEach(() => {
  delete (globalThis as { _telefunc?: unknown })._telefunc
  vi.resetModules()
  failing = new Set()
  imported = []
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
    addEventListener() {},
    removeEventListener() {},
  })
  for (const feature of FEATURES) {
    vi.doMock(`./${feature}.js`, async (importOriginal: () => Promise<unknown>) => {
      imported.push(feature)
      await gates[feature].promise
      if (failing.delete(feature)) throw new Error(`The ${feature} chunk failed to load`)
      return importOriginal()
    })
  }
})
afterEach(async () => {
  // A load a test left held finishes before the next test's fresh modules.
  for (const feature of FEATURES) gates[feature].open()
  await new Promise((resolve) => setTimeout(resolve, 50))
  for (const feature of FEATURES) vi.doUnmock(`./${feature}.js`)
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
  { sse = false, frames = [encodeIndexedFrame(0, textEncoder.encode('42'))] } = {},
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
      },
    })
    return new Response(body, { headers: { 'content-type': sse ? 'text/event-stream' : 'application/octet-stream' } })
  }
}

/** A call that loads the streams chunk. */
async function warmStreams(call: (args: unknown[]) => Promise<unknown>) {
  gates.streams.open()
  respond = streamed({ gen: GENERATOR })
  await call([])
}

// Failures first: a chunk that loaded once stays loaded in this process.

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

test('a load that fails after the streamed body is read cancels the body, whichever value comes first', async () => {
  const { call, ConnectionError } = await page()
  await warmStreams(call)
  respond = streamed({ channel: CHANNEL, gen: GENERATOR })
  failing.add('channels')
  gates.channels.open()
  expect(await settledWithin(call([]), 1000)).toBeInstanceOf(ConnectionError)
  // The first call's body is still being read, by its generator.
  await vi.waitFor(() => expect(cancelled).toBe(1))
})

test.each([false, true])('a failed load releases the acquired response reader (SSE: %s)', async (sse) => {
  const { call, ConnectionError } = await page()
  gates.streams.open()
  const response = streamed({ channel: CHANNEL, gen: GENERATOR }, { sse })(undefined)
  respond = () => response
  failing.add('channels')
  gates.channels.open()

  await expect(call([])).rejects.toBeInstanceOf(ConnectionError)
  expect(cancelled).toBe(1)
  assert(response.body)
  expect(response.body.locked).toBe(false)
  const reader = response.body.getReader()
  try {
    expect(await reader.read()).toEqual({ done: true, value: undefined })
  } finally {
    reader.releaseLock()
  }
})

test('a call aborted while its chunk loads rejects at once, cancels its body once, and leaves no unhandled rejection', async () => {
  const { call, withContext } = await page()
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  respond = streamed({ gen: GENERATOR })
  const controller = new AbortController()
  const pending = withContext(call, { signal: controller.signal })([])
  await vi.waitFor(() => expect(imported).toEqual(['streams']))
  controller.abort()
  expect(isAbort(await settledWithin(pending, 200))).toBe(true)
  await new Promise((resolve) => setTimeout(resolve, 20))
  process.off('unhandledRejection', onUnhandled)
  expect(unhandled).toEqual([])
})

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

test('a held callback preserves an extension tag that shares the replay prefix', async () => {
  const { call, config } = await page()
  const prefix = '!TelefuncHeld:'
  config.extensions.push({
    name: 'app-tag',
    requestTypes: [
      {
        prefix,
        detect: (value: unknown): value is { kind: 'tag'; value: number } =>
          typeof value === 'object' && value !== null && 'kind' in value && value.kind === 'tag',
        replace: (value: { value: number }) => ({ metadata: { value: value.value }, close() {}, abort() {} }),
      },
    ],
  })
  respond = text(1)
  const pending = call([{ kind: 'tag', value: 7 }, (n: number) => n * 2])
  gates.channels.open()

  await pending
  expect(posts).toHaveLength(1)
  const { args } = JSON.parse(posts[0] ?? '')
  expect(args[0]).toBe('!TelefuncHeld:{"value":7}')
  expect(args[1]).toMatch(/^!TelefuncFunction:/)
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

test('a response carrying an inline stream and a channel loads both, then revives both', async () => {
  const { call } = await page()
  for (const feature of FEATURES) gates[feature].open()
  respond = streamed({ gen: GENERATOR, channel: CHANNEL })
  const ret = await call([])
  assertHas(ret, 'gen')
  assertHas(ret, 'channel')
  expect(imported.sort()).toEqual(['channels', 'streams'])
})

test("an extension's value loads what its code imports, and no other chunk", async () => {
  const { call, config } = await page()
  gates.channels.open()
  // The extension's client code: its values create channels.
  await import('../../../client/__internal.js')
  const created: string[] = []
  config.extensions.push({
    name: 'probe',
    responseTypes: [
      {
        prefix: '!TelefuncProbe:',
        revive(metadata: { channelId: string }, context) {
          created.push(context.createChannel({ channelId: metadata.channelId }).id)
          return { value: {}, close() {}, abort() {} }
        },
      },
    ],
  })
  respond = text(tag('!TelefuncProbe:', { channelId: 'p1' }))
  await call([])
  expect(created).toEqual(['p1'])
  expect(imported).toEqual(['channels'])
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

function assertHas<K extends string>(value: unknown, key: K): asserts value is Record<K, unknown> {
  expect(value).toHaveProperty(key)
}
