// A page that floods PINGs and reads nothing, on the Web stream path (Deno, Bun, workerd, the Web adapters): once its
// session ends at its share of `config.channel.maxMemory`, the copies its closed event stream still holds stay in the
// ledger till the stream lets go of them. Through `handleSseChannelRequest`, as the server reads a page's requests.

import { expect, test } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { CHANNEL_TRANSPORT, MAX_CHANNELS_PER_CONNECTION, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { pingDeadline } from '../../wire/pingDeadline.js'
import { handleSseChannelRequest } from './sse.js'
import { noUpgrades } from '../../../node/server/runTelefunc/takesUpgrades.js'

const PING_INTERVAL = 1_000
config.channel = { maxMemory: '16m', pingInterval: PING_INTERVAL }
/** What the ledger holds, for every holder together. */
const ledger = getGlobalObject<{ total: number }>('channelMemory.ts', { total: 0 })

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
const post = (body: Blob) =>
  handleSseChannelRequest(new Request(url, { method: 'POST', body }), undefined, () => false, noUpgrades)

const ended = Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, (_, i) => ({ ix: i + 1, lastSeq: 0 }))
const widePing = () => encode.ping(ended)

test('Web SSE: what a closed event stream holds stays in the ledger till the stream lets go of it', async () => {
  const connId = crypto.randomUUID()
  const reconcile = encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] })
  const { body } = await post(encodeSseRequest({ connId, streamResponse: true }, [reconcile]))
  if (!(body instanceof ReadableStream)) throw new Error('No event stream')
  let status = 200
  for (let round = 0; round < 300 && status === 200; round++)
    status = (await post(encodeSseRequest({ connId }, Array.from({ length: 10 }, widePing)))).statusCode
  expect(status).toBe(400)
  await new Promise((resolve) => setTimeout(resolve, 50))
  const afterEnd = ledger.total
  let unread = 0
  const reader = body.getReader()
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    unread += value.byteLength
  }
  expect(unread).toBeGreaterThan(2 ** 20)
  expect(afterEnd).toBeGreaterThanOrEqual(unread)
  // The stream lets go of what it holds a ping deadline after its end.
  await new Promise((resolve) => setTimeout(resolve, pingDeadline(PING_INTERVAL, CHANNEL_TRANSPORT.SSE) + 500))
  expect(ledger.total).toBe(0)
}, 60_000)
