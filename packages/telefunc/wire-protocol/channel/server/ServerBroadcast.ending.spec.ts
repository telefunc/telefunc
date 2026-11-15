import { expect, test } from 'vitest'

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { config, getChannelConfig } from '../../../node/server/serverConfig.js'
import { CREDIT_WINDOW_INITIAL_BYTES, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import {
  ERROR_REASON,
  decode,
  encode,
  isOneFrame,
  isSequencedFrame,
  messageFrames,
  type ChannelFrame,
  type DecodedFrame,
} from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ClientPieceReceiver } from '../../flowControl/client/ClientPieceReceiver.js'
import { ServerHeadroom } from '../../flowControl/server/ServerHeadroom.js'
import { pairPublishes } from '../client/pairPublishes.js'
import { SseEvents } from '../client/SseEvents.js'
import { getChannelMux } from './ChannelMux.js'
import { hold } from './channelMemory.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { handleSseChannelRequest } from './sse.js'

// The least it may be, the credit window a channel starts with: a test can bring one channel to hold the most it may. The
// server may hold 32 MiB in all, far more than these tests leave held.
config.channel = { maxPendingBytes: CREDIT_WINDOW_INITIAL_BYTES, maxMemory: '32m' }

/** Three publishes, each larger than what a wire sends before its page says it took some. */
const PUBLISHES = ['p1', 'p2', 'p3'].map((name) => name + 'x'.repeat(100_000))

/** What the page reads of `frames`, in order: each publish as `<channel index>:<name>`, each ABORT and ERROR. As the
 *  page does, it drops a frame at or below the newest seq it has of that channel. */
function reads(frames: Iterable<Uint8Array>): string[] {
  const pair = pairPublishes()
  const newestSeq = new Map<number, number>()
  const out: string[] = []
  for (const frame of frames) {
    const decoded = pair(decode(frame) as DecodedFrame)
    if (decoded === null || !isSequencedFrame(decoded) || decoded.seq <= (newestSeq.get(decoded.index) ?? 0)) continue
    newestSeq.set(decoded.index, decoded.seq)
    if (decoded.tag === TAG.PUBLISH) out.push(`${decoded.index}:${(parse(decoded.text) as string).slice(0, 2)}`)
    if (decoded.tag === TAG.ABORT) out.push(`${decoded.index}:ABORT`)
    if (decoded.tag === TAG.ERROR) out.push(`${decoded.index}:ERROR`)
  }
  return out
}

/** Two subscribers of one key on one page, at channel indexes 0 and 1. */
function subscribers() {
  const key = crypto.randomUUID()
  return { key, ended: new ServerBroadcast<string>({ key }), other: new ServerBroadcast<string>({ key }) }
}

/** Subscribes the page to `channels`, at indexes 0, 1, …, over its event stream, which it doesn't read yet. */
async function overSse(channels: ServerBroadcast<string>[]) {
  const mux = getChannelMux()
  for (const channel of channels) mux.registerChannel(channel)
  const broadcast = { text: true, binary: false }
  const open = channels.map((channel, ix) => ({ id: channel.id, ix, lastSeq: 0, initial: true as const, broadcast }))
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const body = encodeSseRequest({ connId: crypto.randomUUID(), streamResponse: true }, [encode.reconcile({ open })])
  const opened = await handleSseChannelRequest(new Request(url, { method: 'POST', body }))
  await Promise.all(channels.map((channel) => channel.opened))
  const reader = (opened.body as ReadableStream<Uint8Array>).getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  return {
    /** Reads the event stream until the page has read `last`, and returns all it read. */
    async readUntil(last: string): Promise<string[]> {
      while (!reads(frames).includes(last)) events.push((await reader.read()).value!, (frame) => frames.push(frame))
      return reads(frames)
    },
    /** Ends the page's event stream. */
    close: () => reader.cancel(),
  }
}

/** Subscribes the page to `channels`, at indexes 0, 1, …, over a WebSocket it doesn't read yet: past the first frame,
 *  what the wire sends waits for headroom. */
function overWebSocket(channels: ServerBroadcast<string>[]) {
  const arrived: Uint8Array[] = []
  const receiver = new ClientPieceReceiver({ owner: { sendAck() {}, carryAck() {} } })
  // The frames of each message, as the page's WebSocket puts them back together.
  const socket = {
    send(message: Uint8Array<ArrayBuffer>) {
      for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) {
        const decoded = decode(frame)
        if (decoded.tag === TAG.ANNOUNCE) receiver.announce(decoded.bytes, performance.now(), decoded.floor)
        else if (decoded.tag === TAG.PIECE) {
          const whole = receiver.add(decoded.total, decoded.piece, performance.now())
          if (whole !== null) arrived.push(whole)
        } else arrived.push(frame)
      }
    },
    bufferedAmount: 0,
  }
  const headroom = new ServerHeadroom({ socket, pingInterval: 5_000 })
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
  for (const [index, channel] of channels.entries()) {
    channel._registerChannel()
    const peer = new IndexedPeer({ sender, index, replay: channel._replayBuffer! })
    channel._attachPeer(peer, { broadcast: { text: true, binary: false } })
  }
  return {
    headroom,
    /** The page reads all that waits for it, saying it took each byte it got, and returns all it read. */
    async readAll(): Promise<string[]> {
      while (headroom.sender.unhandedBytes > 0) {
        headroom.acknowledged({ bytes: headroom.sender.transmittedBytes, heldMs: 0, spanUs: 1_000 })
        await new Promise((resolve) => setTimeout(resolve))
      }
      return reads(arrived)
    },
  }
}

test('on SSE, abort() drops its publishes that wait for the wire, and the other channel gets all of its own', async () => {
  const { key, ended, other } = subscribers()
  const page = await overSse([ended, other])

  // The page reads nothing yet, so the publishes wait for its event stream.
  await Promise.all(PUBLISHES.map((data) => Broadcast.publish(key, data)))
  ended.abort()

  expect(await page.readUntil('1:p3')).toEqual(['0:ABORT', '1:p1', '1:p2', '1:p3'])
  await page.close()
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
    const { headroom } = overWebSocket([ended, other])

    await Promise.all(PUBLISHES.map((data) => Broadcast.publish(key, data)))
    // p1 started on channel 0's head, which nothing of channel 0 passes.
    expect(reads(headroom.sender.waitingFrames)).toEqual(['1:p1', '0:p2', '1:p2', '0:p3', '1:p3'])
    end(ended)
    expect(ended.isClosed).toBe(true)
    // Of channel 0, an error still waits, behind the p1 its wire started.
    expect(reads(headroom.sender.waitingFrames).filter((read) => read !== '0:ERROR')).toEqual(['1:p1', '1:p2', '1:p3'])
    other.abort()
  })
}

/** Each way a subscriber that may miss no publish falls behind, by what it publishes and what its page then reads. */
const OVERFLOWS: Record<string, { publishes: string[]; pageReads: string[] }> = {
  // p1 left: from p2, the oldest waiting, all go.
  'more than a channel may hold waits for its page': {
    publishes: ['p1', 'p2', 'p3', 'p4'].map((name) => name + 'x'.repeat(600_000)),
    pageReads: ['0:p1', '0:ERROR'],
  },
  // p4 never reaches the channel, and nothing else goes to make room for it.
  'a publish is larger than all the server may hold': {
    publishes: [...['p1', 'p2', 'p3'].map((name) => name + 'x'.repeat(600_000)), 'p4' + 'x'.repeat(33 * 1024 * 1024)],
    pageReads: ['0:p1', '0:p2', '0:p3', '0:ERROR'],
  },
}

for (const [overflow, { publishes, pageReads }] of Object.entries(OVERFLOWS)) {
  test(`on SSE, a reliable subscriber where ${overflow} reads every publish before the gap, then the error`, async () => {
    const key = crypto.randomUUID()
    const channel = new ServerBroadcast<string>({ key })
    const page = await overSse([channel])
    // The page starts reading: p1 leaves the server's queue, as it does on a WebSocket.
    await Broadcast.publish(key, publishes[0]!)
    await page.readUntil('0:p1')
    await Promise.all(publishes.slice(1).map((data) => Broadcast.publish(key, data)))
    expect(channel.isClosed).toBe(true)
    expect(await page.readUntil('0:ERROR')).toEqual(pageReads)
    await page.close()
  })

  test(`on a WebSocket, a reliable subscriber where ${overflow} reads every publish before the gap, then the error`, async () => {
    const key = crypto.randomUUID()
    const channel = new ServerBroadcast<string>({ key })
    const page = overWebSocket([channel])
    await Promise.all(publishes.map((data) => Broadcast.publish(key, data)))
    expect(channel.isClosed).toBe(true)
    expect(await page.readAll()).toEqual(pageReads)
  })
}
