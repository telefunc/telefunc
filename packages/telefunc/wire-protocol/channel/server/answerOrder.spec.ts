import { expect, test } from 'vitest'

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { createDeferred } from '../../../utils/createDeferred.js'
import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, isSequencedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { pairPublishes } from '../client/pairPublishes.js'
import { SseEvents } from '../client/SseEvents.js'
import { getChannelMux } from './ChannelMux.js'
import { publishBody } from './PublishBody.js'
import { ServerBroadcast } from './ServerBroadcast.js'
import { handleSseChannelRequest } from './sse.js'

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
const post = (body: BodyInit) => handleSseChannelRequest(new Request(url, { method: 'POST', body }))

/** What the page reads of `frames`: each publish and answer, as `publish:<text>` and `answer:<text>`, with its seq,
 *  less each one at or below the newest seq it read, which the page drops. */
function reads(frames: Uint8Array[]): { label: string; seq: number }[] {
  const pair = pairPublishes()
  const out: { label: string; seq: number }[] = []
  for (const frame of frames) {
    const decoded = pair(decode(frame))
    if (decoded === null || !isSequencedFrame(decoded) || decoded.seq <= (out.at(-1)?.seq ?? 0)) continue
    if (decoded.tag === TAG.PUBLISH) out.push({ label: `publish:${parse(decoded.text)}`, seq: decoded.seq })
    if (decoded.tag === TAG.ACK_RES) out.push({ label: `answer:${parse(decoded.text)}`, seq: decoded.seq })
  }
  return out
}

/** The page attached to `broadcast` at index 0 over a new event stream: first, or again, having read nothing of it. */
async function attach(broadcast: ServerBroadcast<string>, { initial }: { initial: boolean }) {
  const connId = crypto.randomUUID()
  const entry = { id: broadcast._end.id, ix: 0, lastSeq: 0, want: { version: 1, text: true, binary: false } }
  const opened = await post(
    encodeSseRequest({ connId, streamResponse: true }, [
      encode.reconcile({ open: [initial ? { ...entry, initial: true } : entry] }),
    ]),
  )
  if (!(opened.body instanceof ReadableStream)) throw new Error('The attach was answered without an event stream')
  const reader = opened.body.getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  return {
    /** The page sends the channel `text` with an ack, as its frame `seq`. */
    send: (text: string, seq: number) =>
      post(encodeSseRequest({ connId }, [encode.textAckReq(0, stringify(text), seq)])),
    /** Reads the event stream until the page read an answer, and returns what it read. */
    async readToAnswer() {
      while (!reads(frames).some(({ label }) => label.startsWith('answer:'))) {
        const read = await reader.read()
        if (read.done) break
        events.push(read.value, (frame) => frames.push(frame))
      }
      return reads(frames)
    },
    /** The page's event stream ends, as a dropped connection's does. */
    drop: () => reader.cancel(),
  }
}

/** A broadcast whose end, asked `join`, sends a publish and then answers once `answer` resolves, as Room's stub delivers
 *  a join's `joined` before its answer (`room/src/server/RoomHandle.ts`). */
function joiningBroadcast(answer: Promise<void>) {
  const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  getChannelMux().registerChannel(broadcast._end)
  const asked = createDeferred<void>()
  broadcast._end.listen(async () => {
    broadcast._end.deliver(publishBody(stringify('joined'), { seq: 0, timestamp: 0 }, null, false))
    asked.resolve()
    await answer
    return 'member'
  })
  return { broadcast, asked: asked.promise }
}

// A page acts on its join's answer only once it read that join's `joined` (room/src/client/ClientRoomHandle.ts).
test('a publish an end sends before it answers reaches a page that is attached before the answer', async () => {
  const { broadcast } = joiningBroadcast(Promise.resolve())
  const page = await attach(broadcast, { initial: true })
  await page.send('join', 1)
  expect((await page.readToAnswer()).map(({ label }) => label)).toEqual(['publish:joined', 'answer:member'])
  await page.drop()
})

test('a publish an end sends before it answers, while its page is away, reaches the page before the answer once it is back', async () => {
  const answer = createDeferred<void>()
  const { broadcast, asked } = joiningBroadcast(answer.promise)
  const away = await attach(broadcast, { initial: true })
  await away.send('join', 1)
  await asked
  await away.drop()
  answer.resolve()
  await new Promise((resolve) => setTimeout(resolve))
  const back = await attach(broadcast, { initial: false })
  expect((await back.readToAnswer()).map(({ label }) => label)).toEqual(['publish:joined', 'answer:member'])
  await back.drop()
})
