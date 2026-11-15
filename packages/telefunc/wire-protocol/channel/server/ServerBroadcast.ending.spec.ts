import { expect, test } from 'vitest'

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { config, getChannelConfig } from '../../../node/server/serverConfig.js'
import { CREDIT_WINDOW_INITIAL_BYTES, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { ERROR_REASON, decode, encode, type ChannelFrame, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ServerHeadroom } from '../../flowControl/server/ServerHeadroom.js'
import { pairPublishes } from '../client/pairPublishes.js'
import { SseEvents } from '../client/SseEvents.js'
import { getChannelMux } from './ChannelMux.js'
import { hold } from './channelMemory.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { handleSseChannelRequest } from './sse.js'

// The least it may be, the credit window a channel starts with: a test can bring one channel to hold the most it may.
config.channel = { maxPendingBytes: CREDIT_WINDOW_INITIAL_BYTES }

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
  return { key, ended: new ServerBroadcast<string>({ key }), other: new ServerBroadcast<string>({ key }) }
}

test('on SSE, abort() drops its publishes that wait for the wire, and the other channel gets all of its own', async () => {
  const { key, ended, other } = subscribers()
  const mux = getChannelMux()
  mux.registerChannel(ended)
  mux.registerChannel(other)
  const broadcast = { text: true, binary: false }
  const reconcile = encode.reconcile({
    open: [
      { id: ended.id, ix: 0, lastSeq: 0, initial: true, broadcast },
      { id: other.id, ix: 1, lastSeq: 0, initial: true, broadcast },
    ],
  })
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const body = encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [reconcile])
  const opened = await handleSseChannelRequest(new Request(url, { method: 'POST', body }))
  await Promise.all([ended.opened, other.opened])

  // The page reads nothing yet, so the publishes wait for its event stream.
  await Promise.all(PUBLISHES.map((data) => Broadcast.publish(key, data)))
  ended.abort()

  const reader = (opened.body as ReadableStream<Uint8Array>).getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  while (!reads(frames).includes('1:p3')) events.push((await reader.read()).value!, (frame) => frames.push(frame))
  await reader.cancel()
  expect(reads(frames)).toEqual(['0:ABORT', '1:p1', '1:p2', '1:p3'])
})

/** The page sends `frame` on channel 0, its first. */
function fromPage(channel: ServerBroadcast<string>, frame: Uint8Array<ArrayBuffer>): void {
  channel._dispatchFrame(decode(frame) as ChannelFrame)
}

/** Each way channel 0 ends that its page reads nothing of it after: what ends it passes its waiting publishes. */
const ENDINGS: Record<string, (channel: ServerBroadcast<string>) => void> = {
  'abort()': (channel) => channel.abort(),
  "the page's close": (channel) => fromPage(channel, encode.close(0, 1_000, 1)),
  "the page's abort": (channel) => fromPage(channel, encode.abort(0, stringify(undefined), 1)),
  'the page losing its replay': (channel) => fromPage(channel, encode.error(0, ERROR_REASON.LOST, 1)),
  'too many answers to hold': (channel) => {
    // As thousands of the page's ack requests waiting for their answers do.
    hold(channel, getChannelConfig().maxPendingBytes - channel.heldBytes)
    fromPage(channel, encode.publishAckReq(0, stringify('publish'), 1))
  },
}

for (const [ending, end] of Object.entries(ENDINGS)) {
  test(`on a WebSocket, ${ending} drops its publishes that wait for the wire, and the other channel keeps its own in order`, async () => {
    const { key, ended, other } = subscribers()
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
    for (const [index, channel] of [ended, other].entries()) {
      channel._registerChannel()
      const peer = new IndexedPeer({ sender, index, replay: channel._replayBuffer! })
      channel._attachPeer(peer, { broadcast: { text: true, binary: false } })
    }

    await Promise.all(PUBLISHES.map((data) => Broadcast.publish(key, data)))
    // p1 started on channel 0's head, which nothing of channel 0 passes.
    expect(reads(headroom.sender.waitingFrames)).toEqual(['1:p1', '0:p2', '1:p2', '0:p3', '1:p3'])
    end(ended)
    expect(ended.isClosed).toBe(true)
    expect(reads(headroom.sender.waitingFrames)).toEqual(['1:p1', '1:p2', '1:p3'])
    other.abort()
  })
}
