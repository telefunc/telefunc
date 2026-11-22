// The ends the server decides reach the page in the order it decided them: a channel's end that kept the wire, still
// queued when the session's end closes it, goes ahead of the wire's last word, on SSE and on a WebSocket. The data
// queued with it goes with the wire.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { CHANNEL_TRANSPORT, WIRE_VERSION } from '../../constants.js'
import { encode, isOneFrame, messageFrames } from '../../wire/frame.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { TAG } from '../../wire/tags.js'
import { END_CAUSE } from '../endCause.js'
import { getChannelMux, type ServerTransport } from './ChannelMux.js'
import { ServerChannel, ATTACHABLE_NOW } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

config.channel = { reconnectTimeout: 1_000, connectTtl: 15_000 }

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`

/** An SSE wire opened for `channel`, its event stream not read yet. */
async function sseWire(channel: ServerChannel) {
  const connId = crypto.randomUUID()
  const body = encodeSseRequest({ connId, streamResponse: true }, [
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }],
    }),
  ])
  const init = { method: 'POST', body, duplex: 'half' }
  const answer = await handleSseChannelRequest(new Request(url, init), undefined, () => false)
  if (!answer || !(answer.body instanceof ReadableStream)) throw new Error('No event stream')
  return { connId, events: answer.body }
}

/** The tags of the events the stream carries, to its end. */
async function tagsOf(events: ReadableStream<Uint8Array>): Promise<number[]> {
  const tags: number[] = []
  const parser = new SseEvents()
  for await (const chunk of events) parser.push(chunk, (raw) => void tags.push(raw[0] ?? -1))
  return tags
}

/** A WebSocket wire the test holds, which records each frame its transport is handed and its close, in order. */
function webSocketWire() {
  const mux = getChannelMux()
  const wire = {}
  const handed: (number | 'close')[] = []
  const transport: ServerTransport<typeof wire> = {
    getConnId: () => null,
    sendNow: (_w, message) => {
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) handed.push(frame[0] ?? -1)
    },
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    bufferedAmount: () => 0,
    close: () => void handed.push('close'),
  }
  mux.onWireOpen(wire, transport)
  return { wire, handed }
}

test("a channel's end queued on the event stream when the session ends goes ahead of the stream's END", async () => {
  const mux = getChannelMux()
  const channel = new ServerChannel()
  mux.registerChannel(channel, ATTACHABLE_NOW)
  // The stream isn't read: what the server sends waits for it.
  const { connId, events } = await sseWire(channel)
  await channel.opened
  const wire = mux.getWireByConnId(connId)
  if (!wire) throw new Error('No SSE wire')
  mux.endFault(wire, { scope: 'channel', ix: 0, cause: 'frameRefused', wire: 'kept' })
  mux.endFault(wire, { scope: 'session', cause: 'unreadable' })
  expect((await channel.closed)?.message).toBe(END_CAUSE.frameRefused.error().message)
  const tags = await tagsOf(events)
  expect(tags.slice(-2)).toEqual([TAG.ERROR, TAG.END])
})

test("a channel's end held behind a RECONCILED owed on a WebSocket when the session ends goes to the socket ahead of its close", async () => {
  const mux = getChannelMux()
  const channel = new ServerChannel()
  mux.registerChannel(channel, ATTACHABLE_NOW)
  const { wire, handed } = webSocketWire()
  // The RECONCILE is taken and its RECONCILED owed: what the wire sends meanwhile is held behind it.
  await mux.dispatchWireMessage(
    wire,
    encode.reconcile({
      sessionId: crypto.randomUUID(),
      attempt: 1,
      open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }],
    }),
  )
  mux.endFault(wire, { scope: 'channel', ix: 0, cause: 'frameRefused', wire: 'kept' })
  mux.endFault(wire, { scope: 'session', cause: 'unreadable' })
  expect((await channel.closed)?.message).toBe(END_CAUSE.frameRefused.error().message)
  expect(handed.slice(-2)).toEqual([TAG.ERROR, 'close'])
})
