import { expect, test } from 'vitest'

import { createServer } from 'node:http'
import { once } from 'node:events'
import { Telefunc } from '../../../serve/node.js'
import { ClientBroadcast } from '../client/ClientBroadcast.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { createDeferred } from '../../../utils/createDeferred.js'
import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, isSequencedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { pairPublishes } from '../client/pairPublishes.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { noUpgrades } from '../../../node/server/runTelefunc/takesUpgrades.js'
import { getChannelMux } from './ChannelMux.js'
import { publishBody } from './PublishBody.js'
import { ServerBroadcast } from './ServerBroadcast.js'
import { ATTACHABLE_NOW } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
const post = (body: BodyInit) =>
  handleSseChannelRequest(new Request(url, { method: 'POST', body }), undefined, () => false, noUpgrades)

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
      encode.reconcile({
        sessionId: crypto.randomUUID(),
        attempt: 1,
        open: [initial ? { ...entry, initial: true } : entry],
      }),
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
  getChannelMux().registerChannel(broadcast._end, ATTACHABLE_NOW)
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

// An add-on's own protocol, as Room's joins, meta writes and messages: what the page sends and what the server answers
// keep their bytes.
test("an add-on's request and publish on a page's broadcast, and their answers, keep the bytes in them", async () => {
  const server = createServer((_req, res) => res.writeHead(404).end())
  new Telefunc().installWebSocket(server)
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('No port')
  const telefuncUrl = `http://127.0.0.1:${address.port}/_telefunc`
  const published: unknown[] = []
  const end = new ServerBroadcastEnd()
  end.setOwner({
    _onPagePublish(serialized) {
      published.push(parse(serialized))
      return { thumbnail: new Uint8Array([11]) }
    },
    _onPagePublishBinary() {},
  })
  getChannelMux().registerChannel(end, ATTACHABLE_NOW)
  const page = new ClientBroadcast({
    channelId: end.id,
    key: 'room',
    connection: { telefuncUrl, transports: ['ws'], idleTimeout: 0 },
  })
  try {
    await Promise.all([end.opened, page.opened])
    const received: unknown[] = []
    end.listen((request) => {
      received.push(request)
      return { meta: { avatar: new Uint8Array([9, 10]) } }
    })
    const answer = await page._sendRequest({ meta: { avatar: new Uint8Array([7, 8]) } })
    expect(received).toEqual([{ meta: { avatar: new Uint8Array([7, 8]) } }])
    expect(answer).toEqual({ meta: { avatar: new Uint8Array([9, 10]) } })
    expect(await page._publishUnreported({ data: new Uint8Array([12]) })).toEqual({ thumbnail: new Uint8Array([11]) })
    expect(published).toEqual([{ data: new Uint8Array([12]) }])
  } finally {
    await end.close({ timeout: 100 })
    server.close()
  }
})
