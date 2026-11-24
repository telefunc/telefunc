// What a live SSE wire keeps of the requests it answered: nothing, however many there were. A page on the batch path
// (no streaming upload) sends every frame and every PING this way, for as long as its wire lives.

import { queryObjects, setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { expect, test } from 'vitest'
import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { encode } from '../../wire/frame.js'
import { handleSseChannelRequest } from './sse.js'
import { noUpgrades } from '../../../node/server/runTelefunc/takesUpgrades.js'

setFlagsFromString('--expose-gc')
const collect: unknown = runInNewContext('gc')

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
function post(body: BodyInit) {
  return handleSseChannelRequest(new Request(url, { method: 'POST', body }), undefined, () => false, noUpgrades)
}

/** The Promises alive once garbage is collected. */
async function livePromises(): Promise<number> {
  if (typeof collect !== 'function') throw new Error('no gc')
  for (let i = 0; i < 3; i++) {
    collect()
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  return queryObjects(Promise, { format: 'count' })
}

test('2,000 batch POSTs answered on one live SSE wire leave nothing alive behind them', async () => {
  const connId = crypto.randomUUID()
  const opened = await post(
    encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }),
    ]),
  )
  if (!(opened.body instanceof ReadableStream)) throw new Error('No event stream')
  const events = opened.body.getReader()
  void (async () => {
    while (!(await events.read()).done);
  })().catch(() => {})
  const answered: number[] = []
  for (let i = 0; i < 100; i++) answered.push((await post(encodeSseRequest({ connId }, []))).statusCode)
  const before = await livePromises()
  const POSTS = 2_000
  for (let i = 0; i < POSTS; i++) answered.push((await post(encodeSseRequest({ connId }, []))).statusCode)
  const after = await livePromises()
  await events.cancel()
  expect({
    answered: answered.every((statusCode) => statusCode === 200),
    perPost: (after - before) / POSTS < 0.05,
  }).toEqual({
    answered: true,
    perPost: true,
  })
})
