// R3 — the streamRequest upload POST must not answer EOF while frames it carried are still
// queued behind an unfinished recv-chain turn.
//
// The loop deliberately does not await each dispatch (a slow turn must never stall the body), but
// it used to `void` the promises outright, so body-end and processing-end were unrelated events:
// the POST could resolve 200 with a frame still sitting in `recvChain`. R3 retains the dispatches
// and awaits them at EOF, which is the property the BATCH path already had via `drainDeferred`.
//
// ── The park, and why it is the only honest one ──────────────────────────────────────────────
// A test for this is worthless unless something is genuinely parked at EOF: with an empty chain
// the response resolves in a microtask either way, and the unfixed code passes identically. The
// only genuinely awaited seam reachable from `handleFrame` is `attach`'s `waitForChannelRegistration`
// (`mux.ts:318-327`), entered by a RECONCILE naming an `initial: true` channel that is not yet
// registered. Slow listeners park NOTHING — `_dispatchFrame` is `void` and `_onPeerMessage`
// fire-and-forgets listener promises, so the chain turn resolves regardless.
//
// The first test is the instrument check that the park is real (the response is still pending
// after the body has closed and the event loop has turned). Without it, "resolved only after
// release" would be satisfied by a response that resolved immediately.

import { afterEach, describe, expect, test } from 'vitest'

import { ServerChannel } from './server/channel.js'
import { getChannelMux } from './server/mux.js'
import { handleSseChannelRequest } from './server/sse.js'
import { encodeSseRequestMetadata } from './sse-request.js'
import { encodeU32 } from './frame.js'
import { encode } from './shared-ws.js'

const settle = () => new Promise((resolve) => setTimeout(resolve, 0))

const lengthPrefixed = (bytes: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> => {
  const out = new Uint8Array(4 + bytes.length) as Uint8Array<ArrayBuffer>
  out.set(encodeU32(bytes.length), 0)
  out.set(bytes, 4)
  return out
}

const registered: ServerChannel<number, never>[] = []
afterEach(() => {
  for (const channel of registered) channel._onPeerClose()
  registered.length = 0
})

function registerChannel(id: string): ServerChannel<number, never> {
  const channel = new ServerChannel<number, never>({ id })
  getChannelMux().registerChannel(channel)
  registered.push(channel)
  return channel
}

/** Open the SSE downstream so the connection exists and its `ready` gate is resolved. The
 *  stream-response POST carries the first reconcile, which registers channel `id` at ix 0. */
async function openDownstream(connId: string, id: string): Promise<void> {
  const reconcile = encode.reconcile({ open: [{ id, ix: 0, lastSeq: 0, initial: true }] })
  const body = new Blob([encodeSseRequestMetadata({ connId, streamResponse: true }), lengthPrefixed(reconcile)])
  const response = await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body }))
  expect(response?.statusCode).toBe(200)
  // `runStreamResponse` consumes the body asynchronously; `ready` resolves when it finishes.
  await settle()
}

/** Start the long-lived upload POST and hand back a writer for its body. */
function openStreamRequest(connId: string) {
  let controller!: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>
  const body = new ReadableStream<Uint8Array<ArrayBuffer>>({ start: (c) => (controller = c) })
  controller.enqueue(encodeSseRequestMetadata({ connId, streamRequest: true }))

  let resolved = false
  const response = handleSseChannelRequest(
    // `duplex: 'half'` is required for a streaming request body.
    new Request('http://test.local/_telefunc', { method: 'POST', body, duplex: 'half' } as RequestInit),
  ).then((r) => {
    resolved = true
    return r
  })

  return {
    push: (frame: Uint8Array<ArrayBuffer>) => controller.enqueue(lengthPrefixed(frame)),
    closeBody: () => controller.close(),
    response,
    hasResolved: () => resolved,
  }
}

describe('R3 — streamRequest EOF drains the recv chain', () => {
  test('the POST does not resolve while a frame it carried is still parked in the chain', async () => {
    const connId = crypto.randomUUID()
    const chA = registerChannel(`A-${connId}`)
    await openDownstream(connId, chA.id)

    const post = openStreamRequest(connId)
    // A reconcile naming an UNREGISTERED channel with `initial: true` → parks in
    // `waitForChannelRegistration` until that channel registers.
    const parkedId = `B-${connId}`
    post.push(
      encode.reconcile({
        open: [
          { id: chA.id, ix: 0, lastSeq: 0 },
          { id: parkedId, ix: 1, lastSeq: 0, initial: true },
        ],
      }),
    )
    post.closeBody()

    // INSTRUMENT CHECK: the body is finished, the event loop has turned several times, and the
    // response is still pending. This is what makes the assertion after the release meaningful.
    await settle()
    await settle()
    expect(post.hasResolved()).toBe(false)

    // Release the park — registration waiters fire synchronously.
    registerChannel(parkedId)

    expect((await post.response)?.statusCode).toBe(200)
    expect(post.hasResolved()).toBe(true)
  })

  // Control: with nothing parked, EOF resolves on its own. So the pending state above is caused by
  // the parked turn, not by a POST that never completes.
  test('control: with no parked frame the POST resolves at EOF', async () => {
    const connId = crypto.randomUUID()
    const chA = registerChannel(`A-${connId}`)
    await openDownstream(connId, chA.id)

    const post = openStreamRequest(connId)
    post.push(encode.text(0, JSON.stringify(1), 1))
    post.closeBody()

    expect((await post.response)?.statusCode).toBe(200)
  })

  // The batch path's own drain point is real TODAY and must not regress: `handleBatchPost` awaits
  // `drainDeferred`, so its response already implies every frame in the batch was dispatched.
  // Asserted with the same park so a regression there fails here rather than silently.
  test('the batch POST keeps its existing drain guarantee', async () => {
    const connId = crypto.randomUUID()
    const chA = registerChannel(`A-${connId}`)
    await openDownstream(connId, chA.id)

    const parkedId = `C-${connId}`
    const batch = new Blob([
      encodeSseRequestMetadata({ connId }),
      lengthPrefixed(
        encode.reconcile({
          open: [
            { id: chA.id, ix: 0, lastSeq: 0 },
            { id: parkedId, ix: 1, lastSeq: 0, initial: true },
          ],
        }),
      ),
    ])
    let resolved = false
    const response = handleSseChannelRequest(
      new Request('http://test.local/_telefunc', { method: 'POST', body: batch }),
    ).then((r) => {
      resolved = true
      return r
    })

    await settle()
    await settle()
    expect(resolved).toBe(false)

    registerChannel(parkedId)

    expect((await response)?.statusCode).toBe(200)
  })
})
