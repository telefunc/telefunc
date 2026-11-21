import { expect, test } from 'vitest'
import { handleSseChannelRequest } from './sse.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { WIRE_VERSION } from '../../constants.js'
import { frameRefusal, refusalContentType } from '../../refusal.js'
import { END_CAUSE } from '../endCause.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { SseEvents } from '../../wire/SseEvents.js'

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
const post = (body: BodyInit) =>
  handleSseChannelRequest(new Request(url, { method: 'POST', body }), undefined, () => false)

/** A wire-level frame of a tag no version has: it names no channel. */
const unreadable = new Uint8Array([0x0f, 0, 0])

test("a frame of no channel the server cannot read is refused with the POST's `frames` refusal of the session, and ends its channels with the connection's sentence, each told on the event stream before it ends", async () => {
  const channel = new ServerChannel<string, string>({ id: crypto.randomUUID() })
  getChannelMux().registerChannel(channel)
  const connId = crypto.randomUUID()
  const opened = await post(
    encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
    ]),
  )
  await channel.opened

  const refused = await post(encodeSseRequest({ connId }, [unreadable]))

  const refusal = frameRefusal('unreadable', undefined)
  expect({ statusCode: refused.statusCode, contentType: refused.contentType, body: refused.body }).toEqual({
    statusCode: 400,
    contentType: refusalContentType(refusal),
    body: refusal.reason,
  })
  const events: Uint8Array<ArrayBuffer>[] = []
  const decoder = new SseEvents()
  if (!(opened.body instanceof ReadableStream)) throw new Error('Expected the event stream as a web stream')
  const reader = opened.body.getReader()
  for (let read = await reader.read(); !read.done; read = await reader.read()) {
    decoder.push(read.value, (event) => events.push(event))
  }
  const last = events.at(-1)
  expect(last && decode(last)).toEqual({ tag: TAG.ERROR, index: 0, seq: 1, reason: END_CAUSE.unreadable.code })
  await expect(channel.closed).resolves.toHaveProperty('message', END_CAUSE.unreadable.error().message)
})
