// An older SSE open held on the way while a newer wire held the same session and then lost it with nothing in it (the
// session retires): the older open's body, read after, takes nothing, while the server keeps the session's record. The
// page's next attempt takes its channel back with what it sent; with no next attempt the channel ends as a loss.
// Through `handleSseChannelRequest`; the page giving its older request up cancels that request's event stream, as a
// browser's abort does.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata, encodeU32 } from '../../wire/envelope.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import type { ReconcileOpenEntry } from '../../wire/payloads.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

const KEEP = 1_000
config.channel = { reconnectTimeout: KEEP, connectTtl: 2_000 }

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
const settle = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** An SSE open whose RECONCILE comes in `body`'s chunks: its event stream, read as the page reads it. */
async function open(body: BodyInit) {
  // A stream body is sent half duplex, which a `RequestInit` literal doesn't type.
  const init = { method: 'POST', body, duplex: 'half' }
  const answer = await handleSseChannelRequest(new Request(url, init), undefined, () => false)
  const stream = answer.body
  if (!(stream instanceof ReadableStream)) throw new Error('No event stream')
  const reader = stream.getReader()
  const events = new SseEvents()
  const reconciled: number[][] = []
  const texts: string[] = []
  let ended = false
  const reading = (async () => {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) return void (ended = true)
      events.push(value, (event) => {
        for (const each of isOneFrame(event) ? [event] : messageFrames(event)) {
          const frame = decode(each)
          if (frame.tag === TAG.RECONCILED) reconciled.push(frame.payload.open.map(({ ix }) => ix))
          if (frame.tag === TAG.TEXT) texts.push(frame.text)
        }
      })
    }
  })().catch(() => {})
  return {
    reconciled,
    texts,
    ended: () => ended,
    /** The page gives the request up: its event stream is cancelled. */
    giveUp: async () => {
      await reader.cancel()
      await reading
    },
  }
}

const reconcile = (sessionId: string, attempt: number, open: ReconcileOpenEntry[]) =>
  encode.reconcile({ sessionId, attempt, open })

/** The order: the older open (attempt 1, naming X) is held; attempt 2 holds the session with nothing in it and is
 *  lost; then the older body arrives. */
async function heldOlderAfterRetired(x: ServerChannel<string, string>, sessionId: string) {
  const body = new TransformStream<Uint8Array<ArrayBuffer>, Uint8Array<ArrayBuffer>>()
  const writer = body.writable.getWriter()
  const opening = open(body.readable)
  await writer.write(encodeSseRequestMetadata({ connId: crypto.randomUUID(), streamResponse: true }))
  const older = await opening
  const newer = await open(
    encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [reconcile(sessionId, 2, [])]),
  )
  await settle(20)
  await newer.giveUp()
  const stale = reconcile(sessionId, 1, [{ id: x.id, ix: 0, lastSeq: 0, initial: true }])
  await writer.write(encodeU32(stale.byteLength))
  await writer.write(stale)
  await writer.close()
  await settle(50)
  return older
}

test(
  "an older SSE open held while a newer wire held the session and retired it takes nothing; the page's next attempt takes X back with what it sent",
  { timeout: 10_000 },
  async () => {
    const sessionId = crypto.randomUUID()
    const x = new ServerChannel<string, string>()
    const ends: string[] = []
    x.onClose((err) => void ends.push(err ? err.constructor.name : 'clean'))
    getChannelMux().registerChannel(x, ATTACHABLE_NOW)
    const older = await heldOlderAfterRetired(x, sessionId)
    void x.send('sent after the older body').catch(() => {})
    await settle(20)
    const next = await open(
      encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [
        reconcile(sessionId, 3, [{ id: x.id, ix: 0, lastSeq: 0, initial: true }]),
      ]),
    )
    try {
      await settle(KEEP)
      expect({
        older: { reconciled: older.reconciled, ended: older.ended() },
        next: { reconciled: next.reconciled, texts: next.texts },
        ends: [...ends],
      }).toEqual({
        older: { reconciled: [], ended: true },
        next: { reconciled: [[0]], texts: [JSON.stringify('sent after the older body')] },
        ends: [],
      })
    } finally {
      x.abort()
      await older.giveUp()
      await next.giveUp()
    }
  },
)

test(
  'an older SSE open held while a newer wire held the session and retired it takes nothing; with no next attempt X ends as a loss',
  { timeout: 10_000 },
  async () => {
    const sessionId = crypto.randomUUID()
    const x = new ServerChannel<string, string>()
    const ends: string[] = []
    x.onClose((err) => void ends.push(err ? err.constructor.name : 'clean'))
    getChannelMux().registerChannel(x, ATTACHABLE_NOW)
    const older = await heldOlderAfterRetired(x, sessionId)
    const reconciledOnOlder = [...older.reconciled]
    // The page gave the older request up long before; its stream goes as the page's abort reaches the server.
    await older.giveUp()
    try {
      await settle(2_000 + KEEP * 2)
      expect({ reconciledOnOlder, ends: [...ends] }).toEqual({ reconciledOnOlder: [], ends: ['NetworkError'] })
    } finally {
      x.abort()
    }
  },
)
