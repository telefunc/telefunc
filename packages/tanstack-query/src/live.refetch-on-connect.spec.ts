// A live query starts emitting the moment it is serialized, but the client that receives it connects a
// moment later. Everything emitted in between waits in the server channel's pre-peer buffer, which is
// byte-capped and discards silently — `ServerChannel.send`'s rejection is swallowed, so no layer above
// can tell a frame died there. The repair is one bit, held outside the buffer, that re-sends a single
// invalidation on first connect.
//
// These drive the whole chain rather than any part of it: request fence → a write landing between the
// read and serialization → the real tag subscribe/scan → the real wire replacer → a real `ServerChannel`
// with no peer attached → the real SSE transport, server handler and mux → client revival → the real
// `live()` adapter tap → real reconcile and peer attach → invalidate → refetch. The only thing absent is
// the network hop: the client's own `fetch` is answered by the real server handler in-process.
//
// They assert the one thing a consumer can observe: whether the query cache ends up holding the CURRENT
// rows. Asserting that the server sent a frame would prove nothing — that was never in doubt. What was in
// doubt is whether anything reaches the client after the buffer eats it.
//
// The query runs on a `QueryObserver` over a real `QueryClient`, not a framework hook — the adapter wraps
// a `queryFn`, so there is nothing framework-shaped to mount. That buys an ACTIVE query, which is what
// makes invalidation refetch at all; it costs the last inch, so what these prove is the observed cache
// value rather than what any given hook renders with it.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryObserver } from '@tanstack/query-core'

// `telefunc/client` resolves by CONDITION: node gets a no-op shim whose `withContext` is `(fn) => fn`.
// This adapter ships to the browser, so give it the real module — otherwise the call context it depends
// on is stubbed out from under it. Mirrors live.spec.ts.
vi.mock('telefunc/client', async () => await import('../../telefunc/client/withContext.js'))

// Keeps the request context alive across the macrotask awaits below (stamping the fence awaits the tag
// hub's readiness barrier). Without it the default sync context is gone by serialize time.
import '../../telefunc/node/server/context/async.js'
import { restoreContext } from '../../telefunc/node/server/context/context.js'
import type { Context } from '../../telefunc/node/server/context/context.js'
import { config as serverConfig } from '../../telefunc/node/server/serverConfig.js'
import { config as clientConfig } from '../../telefunc/client/clientConfig.js'
import { handleSseChannelRequest } from '../../telefunc/wire-protocol/server/sse.js'
import { serializeTelefunctionResult } from '../../telefunc/node/server/runTelefunc/serializeTelefunctionResult.js'
import { createRequestContext } from '../../telefunc/node/server/context/requestContext.js'
import { parseResponse } from '../../telefunc/wire-protocol/client/response/parse.js'
import { LiveCell } from '../../telefunc/node/server/live/live.js'
import type { Live } from '../../telefunc/node/server/live/live.js'
import { stampRequestStartFence } from '../../telefunc/node/server/live/tags.js'
import { getTagHub, _resetTagHubsForTesting } from '../../telefunc/node/server/live/tagHub.js'
import {
  getBroadcastAdapter,
  _resetBroadcastAdapterForTesting,
  DefaultBroadcastAdapter,
} from '../../telefunc/wire-protocol/server/broadcast.js'
import { STREAM_TRANSPORT } from '../../telefunc/wire-protocol/constants.js'
import { live } from './live.js'

const TELEFUNC_URL = 'http://localhost/_telefunc'
const TELEFUNC_ID = { telefunctionName: 'onGetTodos', telefuncFilePath: '/pages/todos/Todos.telefunc.ts' }
const TAG = 'todos'

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** Wait until `predicate` holds, then give up — deliberately well inside vitest's 5s test timeout, so a
 *  chain that never delivers fails on the ASSERTION that follows (naming the stale value the client was
 *  left holding) rather than on a timeout. A timed-out test says only that something didn't happen; the
 *  assertion says what the client actually ended up with. The working chain settles in ~30ms. */
async function waitUntil(predicate: () => boolean, timeoutMs = 1500): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await delay(10)
  }
}

/** The wire: the client's own `fetch`, answered by the REAL server SSE handler in-process. Nothing here
 *  models the protocol — the client speaks to `handleSseChannelRequest`, which drives the real mux, the
 *  real reconcile and the real `_attachPeer`. */
const inProcessFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  const request = new Request(input as string, { ...init, duplex: 'half' } as RequestInit)
  const response = await handleSseChannelRequest(request)
  if (!response) return new Response('', { status: 404 })
  const headers = new Headers(response.headers)
  headers.set('content-type', response.contentType)
  return new Response(response.body as BodyInit, { status: response.statusCode, headers })
}) as unknown as typeof fetch

/**
 * One telefunction call, end to end. `read` stands in for a telefunction body: it runs after the fence is
 * stamped, exactly as a real body does, and returns the Live it wants to hand back. What follows is the
 * real serialize → wire → revive path, so what comes out is a client handle, not a server cell.
 */
async function callTelefunction(
  read: () => LiveCell<string> | Promise<LiveCell<string>>,
  connectionKey: string,
): Promise<Live<string>> {
  const requestContext = createRequestContext(new Request(TELEFUNC_URL, { method: 'POST' }))
  const context: Context = {}
  return restoreContext(context, async () => {
    await stampRequestStartFence() // request entry: stamped before the body reads anything
    const cell = await read()
    const result = serializeTelefunctionResult({
      ...TELEFUNC_ID,
      telefunctionReturn: cell,
      telefunctionAborted: false,
      context, // the fence rides the context serialization is handed, not the ambient one
      requestContext,
      abortSignal: requestContext.abortSignal,
      streamTransport: STREAM_TRANSPORT.BINARY_INLINE,
      useNodeStream: false,
      serverConfig: { extensionResponseTypes: [], log: { shieldErrors: { dev: false, prod: false } } },
    })
    // A plain Live carries no inline stream, so the body is text — the same response an adapter sends.
    const parsed = (await parseResponse(
      new Response((result as { body: string }).body),
      {
        ...TELEFUNC_ID,
        abortController: new AbortController(),
        channel: { transports: ['sse'] },
        requestCloseHandlers: [],
        extensionResponseTypes: [],
        headers: null,
        telefuncUrl: TELEFUNC_URL,
      },
      connectionKey, // a per-test connection, so no test inherits another's wire
    )) as { ret: Live<string> }
    return parsed.ret
  })
}

/** Mount a real query with the real adapter as its queryFn. An ACTIVE observer is required: invalidation
 *  only drives a refetch for a query something is watching. */
function mountQuery(queryFn: () => Promise<Live<string>>) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const observer = new QueryObserver(queryClient, {
    queryKey: ['todos'],
    queryFn: live(queryFn) as never,
    // Nothing but an invalidation may refetch this query, so a refetch is unambiguous evidence.
    staleTime: Number.POSITIVE_INFINITY,
  })
  const unsubscribe = observer.subscribe(() => {})
  return { queryClient, unsubscribe }
}

let previousAdapter: ReturnType<typeof getBroadcastAdapter>
let previousBufferLimit: number | undefined

beforeEach(() => {
  previousAdapter = getBroadcastAdapter()
  previousBufferLimit = serverConfig.channel.bufferLimit
  _resetBroadcastAdapterForTesting(new DefaultBroadcastAdapter())
  _resetTagHubsForTesting()
  clientConfig.fetch = inProcessFetch
  clientConfig.telefuncUrl = TELEFUNC_URL
  clientConfig.channel.transports = ['sse']
})

afterEach(() => {
  _resetBroadcastAdapterForTesting(previousAdapter)
  serverConfig.channel.bufferLimit = previousBufferLimit ?? 512 * 1024
})

describe('a live query whose pre-connect frames never survive the buffer', () => {
  it('still ends up holding the rows the write produced, not the ones it was born with', async () => {
    // A cap below one serialized invalidate frame: `BufferLane.push`'s oversized path clears the lane and
    // drops the frame, so the buffer has literally nothing to flush when the client attaches. The
    // catch-up invalidation is also the ONLY thing this Live ever emits — no data push exists that could
    // mark the client as behind on its own, so the repair is the only path left to the client.
    serverConfig.channel.bufferLimit = 8
    const connectionKey = crypto.randomUUID()

    let rows = 'rows-at-first-read'
    let calls = 0
    const queryFn = () =>
      callTelefunction(async () => {
        const firstCall = ++calls === 1
        const cell = new LiveCell(rows) // the read
        LiveCell.onInvalidate(TAG, cell) // these rows go stale when TAG is published
        if (firstCall) {
          // A write lands in the read→serialize window: the value the client is about to be handed is
          // already out of date as it goes onto the wire. This is what the fence exists to catch — and
          // only on the first call, so a refetch that never settles shows up as a failure below.
          rows = 'rows-after-the-write'
          await getTagHub().publish([TAG])
        }
        return cell
      }, connectionKey)

    const { queryClient, unsubscribe } = mountQuery(queryFn)
    await waitUntil(() => calls >= 2)

    // The client was handed `rows-at-first-read`, and the one frame that could have told it otherwise was
    // destroyed by the buffer. Holding the post-write rows means the refetch reached it anyway.
    expect(queryClient.getQueryData(['todos'])).toBe('rows-after-the-write')
    expect(calls).toBe(2)

    // And it settles: the second read had no write behind it, so nothing latches and nothing refetches.
    await delay(300)
    expect(calls).toBe(2)
    unsubscribe()
  })

  it('a pushed value destroyed by the buffer is not lost silently either', async () => {
    // The same hazard on the other frame kind. A Live's truth may travel in a data push, and a push that
    // dies in the buffer strands the client on its wire snapshot just as quietly.
    serverConfig.channel.bufferLimit = 64
    const connectionKey = crypto.randomUUID()

    let calls = 0
    let firstCell!: LiveCell<string>
    const queryFn = () =>
      callTelefunction(() => {
        // The refetch must be able to observe the pushed value, so the second read returns what the push
        // carried — otherwise a refetch would be indistinguishable from no refetch at all.
        const cell = new LiveCell(++calls === 1 ? 'snapshot' : 'pushed-value')
        if (calls === 1) firstCell = cell
        return cell
      }, connectionKey)

    const { queryClient, unsubscribe } = mountQuery(queryFn)
    await waitUntil(() => queryClient.getQueryData(['todos']) === 'snapshot')

    // The ONLY emission, and oversized for this cap: the lane is cleared and the frame dropped, so the
    // client is never told. Its `.data` stays on the snapshot it was born with.
    firstCell.set('pushed-value'.padEnd(200, '.'))

    await waitUntil(() => calls >= 2)
    expect(queryClient.getQueryData(['todos'])).toBe('pushed-value')
    unsubscribe()
  })
})
