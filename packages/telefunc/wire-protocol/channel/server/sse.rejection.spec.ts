import { expect, test } from 'vitest'
import { handleSseChannelRequest } from './sse.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { STATUS_CODE_REJECTED, WIRE_REJECTED_REASON, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { SseEvents } from '../../wire/SseEvents.js'

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
const post = (body: BodyInit) =>
  handleSseChannelRequest(new Request(url, { method: 'POST', body }), undefined, () => false)

test("a frame the server cannot read is refused with its POST's status and reason, ends the page's event stream with a REJECTED event, and ends its channels with it", async () => {
  const channel = new ServerChannel<string, string>({ id: crypto.randomUUID() })
  getChannelMux().registerChannel(channel)
  const connId = crypto.randomUUID()
  const opened = await post(
    encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
    ]),
  )
  await channel.opened

  const refused = await post(encodeSseRequest({ connId }, [new Uint8Array([0xff, 0, 0])]))

  expect({ statusCode: refused.statusCode, body: refused.body }).toEqual({
    statusCode: STATUS_CODE_REJECTED,
    body: WIRE_REJECTED_REASON,
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
    expect.stringContaining('the server refused a frame it could not read'),
  )
})
