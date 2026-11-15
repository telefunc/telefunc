import { expect, test } from 'vitest'

import { parse } from '@brillout/json-serializer/parse'
import { WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ServerHeadroom } from '../../flowControl/server/ServerHeadroom.js'
import { pairPublishes } from '../client/pairPublishes.js'
import { SseEvents } from '../client/SseEvents.js'
import { getChannelMux } from './ChannelMux.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { handleSseChannelRequest } from './sse.js'

/** Three publishes, each larger than what a wire sends before its page says it took some. */
const PUBLISHES = ['p1', 'p2', 'p3'].map((name) => name + 'x'.repeat(100_000))

/** What the page reads of `frames`, in order: each publish as `<channel index>:<name>`, and each ABORT. */
function reads(frames: Iterable<Uint8Array>): string[] {
  const pair = pairPublishes()
  const out: string[] = []
  for (const frame of frames) {
    const decoded = pair(decode(frame) as DecodedFrame)
    if (decoded?.tag === TAG.PUBLISH) out.push(`${decoded.index}:${(parse(decoded.text) as string).slice(0, 2)}`)
    if (decoded?.tag === TAG.ABORT) out.push(`${decoded.index}:ABORT`)
  }
  return out
}

/** Two subscribers of one key on one page, at channel indexes 0 and 1. */
function subscribers() {
  const key = crypto.randomUUID()
  return { key, aborted: new ServerBroadcast<string>({ key }), other: new ServerBroadcast<string>({ key }) }
}

test('on SSE, abort() drops its publishes that wait for the wire, and the other channel gets all of its own', async () => {
  const { key, aborted, other } = subscribers()
  const mux = getChannelMux()
  mux.registerChannel(aborted)
  mux.registerChannel(other)
  const broadcast = { text: true, binary: false }
  const reconcile = encode.reconcile({
    open: [
      { id: aborted.id, ix: 0, lastSeq: 0, initial: true, broadcast },
      { id: other.id, ix: 1, lastSeq: 0, initial: true, broadcast },
    ],
  })
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const body = encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [reconcile])
  const opened = await handleSseChannelRequest(new Request(url, { method: 'POST', body }))
  await Promise.all([aborted.opened, other.opened])

  // The page reads nothing yet, so the publishes wait for its event stream.
  await Promise.all(PUBLISHES.map((data) => Broadcast.publish(key, data)))
  aborted.abort()

  const reader = (opened.body as ReadableStream<Uint8Array>).getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  while (!reads(frames).includes('1:p3')) events.push((await reader.read()).value!, (frame) => frames.push(frame))
  await reader.cancel()
  expect(reads(frames)).toEqual(['0:ABORT', '1:p1', '1:p2', '1:p3'])
})

test('on a WebSocket, abort() drops its publishes that wait for the wire, and the other channel keeps its own in order', async () => {
  const { key, aborted, other } = subscribers()
  // A socket the page takes nothing from: past the first frame, what the wire sends waits for headroom.
  const headroom = new ServerHeadroom({ socket: { send() {}, bufferedAmount: 0 }, pingInterval: 5_000 })
  const sender: PeerSender = {
    queue: headroom,
    send(frame: Uint8Array<ArrayBuffer>) {
      headroom.send(frame, performance.now())
    },
    carry() {},
    bufferedAmount: () => 0,
    behindAfter: () => Number.POSITIVE_INFINITY,
    deliveredBefore: () => 0,
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
  }
  for (const [index, channel] of [aborted, other].entries()) {
    channel._registerChannel()
    const peer = new IndexedPeer({ sender, index, replay: channel._replayBuffer! })
    channel._attachPeer(peer, { broadcast: { text: true, binary: false } })
  }

  await Promise.all(PUBLISHES.map((data) => Broadcast.publish(key, data)))
  // p1 started on channel 0's head, which the ABORT can't pass.
  expect(reads(headroom.sender.waitingFrames)).toEqual(['1:p1', '0:p2', '1:p2', '0:p3', '1:p3'])
  aborted.abort()
  expect(reads(headroom.sender.waitingFrames)).toEqual(['1:p1', '1:p2', '1:p3'])
  other.abort()
})
