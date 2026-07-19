// T4 — the barrier through the SSE BATCH-POST path, which is the only place `deliverTo` meets a
// `connection.closed` guard.
//
// Both batch entry points suppressed their reconciled when the connection that carried the frames
// had closed. That is right for an ordinary reconcile — the answer is bound for that very wire, and
// sending it nowhere is the correct no-op. It is WRONG for a barrier commit: its answer is bound for
// the staged probe wire, and the old SSE connection closing is not an error but the expected end of
// the flow. Suppressing there leaves the server rotated and the client never told, which is
// precisely the unrecoverable shape the design forbids.
//
// ── Why this needs the real `sse.ts` and not the mux harness ─────────────────────────────────
// The guard lives in `handleBatchPost`, above the mux. The mux harness delivers frames straight to
// `onConnectionRawMessage` and never reaches it. So this file drives `handleSseChannelRequest` with
// hand-built Requests, the same seam `upgrade.stream-request-eof.spec.ts` established, and registers
// a probe connection directly on the module-global mux that `sse.ts` resolves.
//
// The close is real and racing, not simulated: a fresh stream-response POST reusing the same connId
// is exactly how `handleStreamResponsePost` closes an existing connection (a client reconnect), and
// it lands while the batch body is still open.

import { afterEach, describe, expect, test } from 'vitest'

import { ServerChannel } from './server/channel.js'
import { getChannelMux, type ServerTransport } from './server/mux.js'
import { handleSseChannelRequest } from './server/sse.js'
import { encodeSseRequestMetadata } from './sse-request.js'
import { encodeU32 } from './frame.js'
import { TAG, decode, encode, type DecodedFrame } from './shared-ws.js'

const settle = () => new Promise((resolve) => setTimeout(resolve, 0))

/** Wait for an OBSERVABLE CONDITION rather than for a fixed number of event-loop turns.
 *  Driving the real `sse.ts` means real `ReadableStream` reads, whose wakeups are not timer-driven
 *  and take an unbounded number of turns under CPU contention. A single `settle()` here was measured
 *  to pass alone and fail intermittently when the whole suite runs back to back — a flaky gate,
 *  which is worse than no gate. Polling on the condition makes the wait deterministic in outcome and
 *  fails loudly (rather than silently proceeding) if the condition never arrives. */
async function waitUntil(label: string, predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 1_000; i++) {
    if (predicate()) return
    await settle()
  }
  throw new Error(`waitUntil timed out: ${label}`)
}

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

/** A probe (WebSocket-shaped) connection on the same global mux `sse.ts` uses. `getConnId: null` is
 *  already the SSE-vs-WS discriminator the mux keys on, so no new production seam is needed. */
function openProbeWire() {
  const conn = {}
  const sent: DecodedFrame[] = []
  let sessionId: string | undefined
  let terminated = false
  const transport: ServerTransport<object> = {
    getSessionId: () => sessionId,
    setSessionId: (_c, id) => {
      sessionId = id
    },
    getConnId: () => null,
    sendNow: (_c, frame) => {
      sent.push(decode(frame))
    },
    terminateConnection: () => {
      terminated = true
    },
  }
  getChannelMux().onConnectionOpen(conn, transport)
  return {
    conn,
    sent,
    sessionId: () => sessionId,
    terminated: () => terminated,
    deliver: (frame: Uint8Array<ArrayBuffer>) => getChannelMux().onConnectionRawMessage(conn, frame),
  }
}

/** Opens the SSE downstream, carrying the first reconcile, and returns the session it minted. */
async function openDownstream(connId: string, channelId: string): Promise<string> {
  const reconcile = encode.reconcile({ open: [{ id: channelId, ix: 0, lastSeq: 0, initial: true }] })
  const body = new Blob([encodeSseRequestMetadata({ connId, streamResponse: true }), lengthPrefixed(reconcile)])
  const response = await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body }))
  expect(response?.statusCode).toBe(200)
  // `runStreamResponse` consumes the body asynchronously; the session appearing IS its completion.
  const live = () => getChannelMux().getConnectionByConnId(connId) as { sessionId: string | null } | undefined
  await waitUntil(`downstream ${connId} reconciled`, () => typeof live()?.sessionId === 'string')
  return live()!.sessionId as string
}

describe('barrier commit through a batch POST', () => {
  test('COMMITTED still reaches the probe wire when the old connection closes mid-batch', async () => {
    const connId = 'batch-barrier-1'
    const channelId = 'batch-barrier-ch-1'
    registerChannel(channelId)
    const s0 = await openDownstream(connId, channelId)

    const probe = openProbeWire()
    await probe.deliver(encode.prepare({ upgradeId: 'upg-1', sessionId: s0, open: [{ id: channelId, ix: 0 }] }))
    expect(probe.sent.filter((f) => f.tag === TAG.READY)).toHaveLength(1)

    // A batch POST whose body we hold open, so the close below lands mid-drain.
    let pushBody!: (chunk: Uint8Array<ArrayBuffer>) => void
    let endBody!: () => void
    const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
      start(controller) {
        pushBody = (chunk) => controller.enqueue(chunk)
        endBody = () => controller.close()
      },
    })
    const post = handleSseChannelRequest(
      new Request('http://test.local/_telefunc', { method: 'POST', body: stream, duplex: 'half' } as RequestInit),
    )
    pushBody(encodeSseRequestMetadata({ connId }))
    pushBody(
      lengthPrefixed(
        encode.reconcile({
          sessionId: s0,
          upgrade: true,
          barrier: true,
          upgradeId: 'upg-1',
          open: [{ id: channelId, ix: 0, lastSeq: 0 }],
        }),
      ),
    )
    // Let the barrier dispatch and commit. The reconcile has run — the probe wire now holds the new
    // session — but its COMMITTED is not sent until the batch POST decides to send it.
    await waitUntil('barrier committed on the probe wire', () => typeof probe.sessionId() === 'string')
    expect(probe.sessionId()).not.toBe(s0)
    expect(probe.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0) // ...not yet sent

    // The old connection dies before the POST resolves: a client reconnect on the same connId, which
    // is exactly what closes an existing connection in `handleStreamResponsePost`.
    await openDownstream(connId, channelId)
    endBody()
    const response = await post

    expect(response?.statusCode).toBe(200)
    // THE ASSERTION: the commit was delivered. Guarding on the closed OLD connection would drop it
    // here, leaving the server rotated and the client never told.
    const committed = probe.sent.filter((f) => f.tag === TAG.RECONCILED)
    expect(committed).toHaveLength(1)
    expect(committed[0]?.tag === TAG.RECONCILED && committed[0].payload.upgradeId).toBe('upg-1')
  })

  test('control: an ORDINARY reconcile in a batch POST is never routed to the probe wire', async () => {
    // The instrument that can disagree, at the same seam. Without it, the test above would be
    // satisfied by a batch path that sent EVERY reconciled to whatever probe happened to exist —
    // `deliverTo` being set is what routes a commit, and nothing else may.
    const connId = 'batch-barrier-2'
    const channelId = 'batch-barrier-ch-2'
    registerChannel(channelId)
    const s0 = await openDownstream(connId, channelId)
    const probe = openProbeWire()
    await probe.deliver(encode.prepare({ upgradeId: 'upg-2', sessionId: s0, open: [{ id: channelId, ix: 0 }] }))

    const body = new Blob([
      encodeSseRequestMetadata({ connId }),
      // Same wire, same channel, same session — everything the barrier had EXCEPT the flag.
      lengthPrefixed(encode.reconcile({ sessionId: s0, open: [{ id: channelId, ix: 0, lastSeq: 0 }] })),
    ])
    const response = await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body }))

    expect(response?.statusCode).toBe(200)
    expect(probe.sent.filter((f) => f.tag === TAG.RECONCILED)).toHaveLength(0)
    expect(probe.sessionId()).toBeUndefined() // the ordinary reconcile rotated ITS OWN wire, not this one
  })
})
