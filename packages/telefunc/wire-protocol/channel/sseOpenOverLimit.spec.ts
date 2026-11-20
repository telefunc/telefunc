// An abort whose reason is over the server's frame limit, made before the page's first open, whose body would carry it:
// a real server and a real page, in one process, over SSE. The page sizes the ABORT as it sizes a message, so the abort
// goes without its reason and `abort()` throws; the other channel of the open is untouched (decisions/516-sse-carrier.md,
// the ruling of 22:17Z on the open's body).

import { afterAll, beforeAll, expect, test, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { Telefunc } from '../../serve/node.js'
import { config as serverConfig } from '../../node/server/serverConfig.js'
import { ServerChannel } from './server/ServerChannel.js'
import { ClientChannel } from './client/ClientChannel.js'
import { getChannelMux } from './server/ChannelMux.js'

// The frame limit at 1 MiB, not 64 MiB, on both ends: an over-limit reason is built in milliseconds.
vi.mock('../constants.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../constants.js')>()),
  WIRE_MAX_RAW_FRAME_BYTES: 2 ** 20,
}))

serverConfig.channel = { transports: ['sse'] }

/** How many opens the page made. */
let opens = 0
const realFetch = globalThis.fetch
async function countingFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  if (new Headers(init?.headers).get('accept') === 'text/event-stream') opens++
  return realFetch(input, init)
}

let server: Server
let telefuncUrl: string

beforeAll(async () => {
  vi.stubGlobal('fetch', countingFetch)
  const telefunc = new Telefunc()
  server = createServer((req, res) => void telefunc.serve({ req, res }))
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
})
afterAll(() => {
  vi.unstubAllGlobals()
  server.closeAllConnections()
  server.close()
})

/** Resolves once `done()` holds, checked every 10 ms; rejects after `ms`. */
async function until(done: () => boolean, ms = 3_500): Promise<void> {
  const deadline = performance.now() + ms
  while (!done()) {
    if (performance.now() > deadline) throw new Error('timed out waiting')
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
}

/** `err` as a row compares it: its name, its message and, for an abort, its value. */
function ended(err: unknown) {
  if (!(err instanceof Error)) return err
  return { name: err.name, message: err.message, abortValue: 'abortValue' in err ? err.abortValue : 'none' }
}

test("an abort before the first open, its reason over the server's frame limit, goes without the reason and throws; the open's other channel goes on", async () => {
  const aborted = new ServerChannel<string, string>()
  const other = new ServerChannel<string, string>()
  for (const channel of [aborted, other]) getChannelMux().registerChannel(channel)
  const serverEnds: { channel: string; err: unknown }[] = []
  aborted.onClose((err) => void serverEnds.push({ channel: 'aborted', err: ended(err) }))
  other.onClose((err) => void serverEnds.push({ channel: 'other', err: ended(err) }))
  const got: string[] = []
  other.listen((message) => void got.push(message))

  const connection = { telefuncUrl, transports: ['sse' as const], idleTimeout: 0, connectionKey: crypto.randomUUID() }
  const page = new ClientChannel<string, string>({ channelId: aborted.id, connection })
  const otherPage = new ClientChannel<string, string>({ channelId: other.id, connection })
  const pageEnds: { channel: string; err: unknown }[] = []
  page.onClose((err) => void pageEnds.push({ channel: 'aborted', err: ended(err) }))
  otherPage.onClose((err) => void pageEnds.push({ channel: 'other', err: ended(err) }))
  // Before the first open: its ABORT rides in the open's body. Its reason, 2 MiB, is over the limit.
  expect(() => page.abort('x'.repeat(2 * 2 ** 20))).toThrow(
    /Channel abort reason too large: \d+ bytes encoded, the server accepts 1048576 at most$/,
  )
  void otherPage.send('alongside').catch(() => {})

  await until(() => got.includes('alongside') && serverEnds.length > 0)
  void otherPage.send('after').catch(() => {})
  await until(() => got.includes('after'))
  const abortedEnd = { name: 'Abort', message: expect.any(String), abortValue: undefined }
  expect(serverEnds).toEqual([{ channel: 'aborted', err: abortedEnd }])
  expect(pageEnds).toEqual([{ channel: 'aborted', err: abortedEnd }])
  expect(got).toEqual(['alongside', 'after'])
  expect(opens).toBe(1)
})
