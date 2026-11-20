import { expect, test } from 'vitest'
import { handleSseChannelRequest } from './sse.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { WIRE_VERSION } from '../../constants.js'
import { FRAME_REFUSAL, frameRefusal, refusalContentType } from '../../refusal.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { SseEvents } from '../../wire/SseEvents.js'

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
const post = (body: BodyInit) =>
  handleSseChannelRequest(new Request(url, { method: 'POST', body }), undefined, () => false)

/** A wire-level frame of a tag no version has: it names no channel. */
const unreadable = new Uint8Array([0x0f, 0, 0])

test("a frame of no channel the server cannot read is refused with the POST's `frames` refusal, ends the page's event stream with a REJECTED event, and ends its channels with the reason", async () => {
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

  const refusal = frameRefusal('refused', undefined)
  expect({ statusCode: refused.statusCode, contentType: refused.contentType, body: refused.body }).toEqual({
    statusCode: 400,
    contentType: refusalContentType(refusal),
    body: refusal.reason,
  })
  const events: Uint8Array[] = []
  const decoder = new SseEvents()
  if (!(opened.body instanceof ReadableStream)) throw new Error('Expected the event stream as a web stream')
  const reader = opened.body.getReader()
  for (let read = await reader.read(); !read.done; read = await reader.read()) {
    decoder.push(read.value, (event) => events.push(event))
  }
  expect(events.at(-1)).toEqual(encode.rejected())
  await expect(channel.closed).resolves.toHaveProperty(
    'message',
    `Channel closed: the server refused what the page sent on it: ${FRAME_REFUSAL.refused.reason}`,
  )
})
