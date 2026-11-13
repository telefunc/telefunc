import { expect, test } from 'vitest'

import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest, encodeSseRequestMetadata, encodeU32 } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { handleSseChannelRequest } from './sse.js'

function post(body: BodyInit) {
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  return handleSseChannelRequest(new Request(url, { method: 'POST', body, duplex: 'half' } as RequestInit))
}

test('a batch POST whose wire closed before its frames ran is refused', async () => {
  const connId = crypto.randomUUID()
  const opened = await post(encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]))
  expect(opened.statusCode).toBe(200)

  // The batch's metadata arrives at once, its frame only once the handler reads for it.
  const reading = createDeferred<ReadableStreamDefaultController<Uint8Array>>()
  let reads = 0
  const body = new ReadableStream<Uint8Array>(
    {
      pull: (controller) => {
        if (++reads === 1) controller.enqueue(encodeSseRequestMetadata({ connId }))
        else reading.resolve(controller)
      },
    },
    { highWaterMark: 0 },
  )
  const answer = post(body)
  const controller = await reading.promise

  // The page's event stream is cancelled, closing the wire, while the batch's frame is on its way.
  await (opened.body as ReadableStream).cancel()
  const ping = encode.ping([])
  controller.enqueue(encodeU32(ping.byteLength))
  controller.enqueue(ping)
  controller.close()

  expect((await answer).statusCode).toBe(400)
})
