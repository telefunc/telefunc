// An SSE open held on the way is read after the page gave its wire up. A channel it names may open on that wire; the page's next attempt takes it
// back with what it sent, and without one the channel ends as a loss. Through `handleSseChannelRequest`, as the
// server reads a page's requests.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

const KEEP = 1_000
config.channel = { reconnectTimeout: KEEP, connectTtl: 15_000 }

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`

/** A RECONCILE carrying the page's attempt number. */
const reconcile = (payload: Parameters<typeof encode.reconcile>[0], attempt: number) =>
  encode.reconcile(Object.assign({ attempt }, payload))

/** An SSE open whose event stream the test reads: the channels its RECONCILED attached, and the texts. */
async function open(reconcileFrame: Uint8Array<ArrayBuffer>) {
  const init = {
    method: 'POST',
    body: encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [reconcileFrame]),
    duplex: 'half',
  }
  const answer = await handleSseChannelRequest(new Request(url, init), undefined, () => false)
  const body = answer.body
  if (!(body instanceof ReadableStream)) throw new Error('No event stream')
  const attached: number[] = []
  const texts: string[] = []
  const events = new SseEvents()
  const reader = body.getReader()
  const reading = (async () => {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) return
      events.push(value, (event) => {
        for (const each of isOneFrame(event) ? [event] : messageFrames(event)) {
          const frame = decode(each)
          if (frame.tag === TAG.RECONCILED) for (const { ix } of frame.payload.open) attached.push(ix)
          if (frame.tag === TAG.TEXT) texts.push(frame.text)
        }
      })
    }
  })().catch(() => {})
  return {
    attached,
    texts,
    /** The page gives the wire up: its request is aborted, which cancels the stream at the server. */
    cancel: async () => {
      await reader.cancel()
      await reading
    },
  }
}

const settle = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

test(
  "a held open naming a session the server has no record of opens X on a wire no page reads; the page's next attempt takes X back with what it sent",
  { timeout: 15_000 },
  async () => {
    const pageId = crypto.randomUUID()
    const x = new ServerChannel<string, string>()
    const ends: string[] = []
    x.onClose((err) => void ends.push(err ? err.message : 'clean'))
    const held = await open(reconcile({ sessionId: pageId, open: [{ id: x.id, ix: 0, lastSeq: 0, initial: true }] }, 1))
    getChannelMux().registerChannel(x)
    await Promise.race([x.opened, settle(1_000)])
    void x.send('sent while no page read')
    await settle(20)
    await held.cancel()
    const next = await open(reconcile({ sessionId: pageId, open: [{ id: x.id, ix: 0, lastSeq: 0, initial: true }] }, 3))
    try {
      await settle(KEEP * 2)
      expect({ openedOnHeld: held.attached.includes(0), ends: [...ends], replayed: next.texts }).toEqual({
        openedOnHeld: true,
        ends: [],
        replayed: [JSON.stringify('sent while no page read')],
      })
    } finally {
      x.abort()
      await next.cancel()
    }
  },
)

test(
  'a held open naming a session the server has no record of opens X on a wire no page reads; with no next attempt X ends as a loss',
  { timeout: 15_000 },
  async () => {
    const pageId = crypto.randomUUID()
    const x = new ServerChannel<string, string>()
    const ends: string[] = []
    x.onClose((err) => void ends.push(err ? err.constructor.name : 'clean'))
    const held = await open(reconcile({ sessionId: pageId, open: [{ id: x.id, ix: 0, lastSeq: 0, initial: true }] }, 1))
    getChannelMux().registerChannel(x)
    await Promise.race([x.opened, settle(1_000)])
    await settle(20)
    await held.cancel()
    await settle(KEEP * 3)
    try {
      expect({ openedOnHeld: held.attached.includes(0), ends: [...ends] }).toEqual({
        openedOnHeld: true,
        ends: ['NetworkError'],
      })
    } finally {
      x.abort()
    }
  },
)
