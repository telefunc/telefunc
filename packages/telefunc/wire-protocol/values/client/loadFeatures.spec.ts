import { afterEach, assert, beforeEach, expect, test, vi } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { concat, encodeIndexedFrame, encodeU32, textEncoder } from '../../wire/envelope.js'

// A page's first call that carries a value loads that value's code: each chunk's load is held here till a test lets it
// finish, or fails, as the chunk's request would.
const FEATURES = ['streams'] as const
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
  gates = { streams: newGate() }
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
const GENERATOR = tag('!TelefuncGenerator:', { __index: 0 })

/** A streamed response whose stream 0 sends `42` and stays open; a fetch that is aborted errors it, as browsers do. */
function streamed(ret: Record<string, string>) {
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
        controller.enqueue(
          concat(encodeU32(metadata.length), metadata, encodeIndexedFrame(0, textEncoder.encode('42'))),
        )
      },
      cancel() {
        cancelled++
      },
    })
    return new Response(body, { headers: { 'content-type': 'application/octet-stream' } })
  }
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

test('plain RPC: sent at once, an app string that looks like a value loads nothing and comes back as it was', async () => {
  const { call } = await page()
  const value = ['!', '!Telefunc', tag('!TelefuncChannel:', { channelId: 'c1' }), '!TelefuncRoom:x']
  respond = () => new Response(stringify({ ret: value }))
  const pending = call([value])
  expect(posts).toHaveLength(1)
  const ret = await pending
  assert(Array.isArray(ret))
  expect([...ret]).toEqual(value)
  expect(imported).toEqual([])
})
