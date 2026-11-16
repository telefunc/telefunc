import { expect, test } from 'vitest'

import { parse } from '@brillout/json-serializer/parse'
import { config } from '../../../node/server/serverConfig.js'
import { CREDIT_WINDOW_INITIAL_BYTES, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode, isOneFrame, isSequencedFrame, messageFrames, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ClientPieceReceiver } from '../../flowControl/client/ClientPieceReceiver.js'
import { ServerHeadroom } from '../../flowControl/server/ServerHeadroom.js'
import { pairPublishes } from '../client/pairPublishes.js'
import { SseEvents } from '../client/SseEvents.js'
import { getChannelMux } from './ChannelMux.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

// The least it may be, the credit window a channel starts with: a test can bring one channel to hold the most it may. The
// server may hold 32 MiB in all, far more than these tests leave held.
config.channel = { maxPendingBytes: CREDIT_WINDOW_INITIAL_BYTES, maxMemory: '32m' }

/** Three messages, each larger than what a wire sends before its page says it took some. */
const MESSAGES = ['p1', 'p2', 'p3'].map((name) => name + 'x'.repeat(100_000))

/** A message as `<channel index>:<name>`, or a CLOSE, with its channel's seq. */
type Sent = { index: number; seq: number; label: string }

/** What `frames` carry, in order. */
function sent(frames: Iterable<Uint8Array>): Sent[] {
  const pair = pairPublishes()
  const out: Sent[] = []
  for (const frame of frames) {
    const decoded = pair(decode(frame) as DecodedFrame)
    if (decoded === null || !isSequencedFrame(decoded)) continue
    const { index, seq } = decoded
    if (decoded.tag === TAG.PUBLISH || decoded.tag === TAG.TEXT)
      out.push({ index, seq, label: `${index}:${(parse(decoded.text) as string).slice(0, 2)}` })
    if (decoded.tag === TAG.CLOSE) out.push({ index, seq, label: `${index}:CLOSE` })
  }
  return out
}

/** What the page reads of `frames`, in order: what they carry, less each frame at or below the newest seq it has of
 *  that channel, which the page drops. */
function reads(frames: Iterable<Uint8Array>): string[] {
  const newestSeq = new Map<number, number>()
  const out: string[] = []
  for (const { index, seq, label } of sent(frames)) {
    if (seq <= (newestSeq.get(index) ?? 0)) continue
    newestSeq.set(index, seq)
    out.push(label)
  }
  return out
}

/** The page's subscriptions to `channel`: to a broadcast's text, and none to a plain channel, which takes none. */
function subscribed(channel: ServerChannel) {
  return channel instanceof ServerBroadcastEnd ? { broadcast: { text: true, binary: false } } : {}
}

/** Subscribes the page to `channels`, at indexes 0, 1, …, over its event stream, which it doesn't read yet. */
async function overSse(channels: ServerChannel[]) {
  const mux = getChannelMux()
  for (const channel of channels) mux.registerChannel(channel)
  const open = channels.map((channel, ix) => ({
    id: channel.id,
    ix,
    lastSeq: 0,
    initial: true as const,
    ...subscribed(channel),
  }))
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const connId = crypto.randomUUID()
  const body = encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open })])
  const opened = await handleSseChannelRequest(new Request(url, { method: 'POST', body }))
  await Promise.all(channels.map((channel) => channel.opened))
  const reader = (opened.body as ReadableStream<Uint8Array>).getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  return {
    /** Reads the event stream until the page has read `last`, and returns the frames it carried. */
    async readUntil(last: string): Promise<Uint8Array[]> {
      while (!reads(frames).includes(last)) events.push((await reader.read()).value!, (frame) => frames.push(frame))
      return frames
    },
    /** Ends the page's event stream. */
    close: () => reader.cancel(),
  }
}

/** Attaches the page to `channels`, at indexes 0, 1, …, subscribed to the broadcasts, over a WebSocket it doesn't read
 *  yet: past the first frame, what the wire sends waits for headroom. */
function overWebSocket(channels: ServerChannel[]) {
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
    terminate() {},
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
  }
  for (const [index, channel] of channels.entries()) {
    channel._registerChannel()
    const peer = new IndexedPeer({ sender, index, replay: channel._replayBuffer! })
    channel._attachPeer(peer, subscribed(channel))
  }
  return {
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

test('on a WebSocket, once the page read a publish, its close goes ahead of what of another channel waits', async () => {
  const key = crypto.randomUUID()
  const channel = new ServerBroadcast<string>({ key })._end
  const other = new ServerChannel()
  const page = overWebSocket([channel, other])
  void Broadcast.publish(key, 'p0')
  expect(await page.readAll()).toEqual(['0:p0'])
  for (const message of MESSAGES) void other.send(message)
  void channel.close()
  // Of the other channel, p1 started and its rest waits.
  expect(await page.readAll()).toEqual(['0:p0', '0:CLOSE', '1:p1', '1:p2', '1:p3'])
  other.abort()
})

test('on SSE, once the page read a publish, its close goes ahead of what of another channel waits', async () => {
  const key = crypto.randomUUID()
  const channel = new ServerBroadcast<string>({ key })._end
  const other = new ServerChannel()
  const page = await overSse([channel, other])
  void Broadcast.publish(key, 'p0')
  await page.readUntil('0:p0')
  for (const message of MESSAGES) void other.send(message)
  void channel.close()
  // Of the other channel, p1's event started, and nothing passes an event once it started.
  expect(reads(await page.readUntil('1:p3'))).toEqual(['0:p0', '1:p1', '0:CLOSE', '1:p2', '1:p3'])
  await page.close()
})

test("on SSE, a frame that goes between a publish's head and its body leaves that channel's close behind the body", async () => {
  const key = crypto.randomUUID()
  const channel = new ServerBroadcast<string>({ key })._end
  const other = new ServerChannel()
  const page = await overSse([channel, other])
  // The page reads nothing yet, so both wait for its event stream.
  void Broadcast.publish(key, 'p0')
  void Broadcast.publish(key, 'p1')
  // Each event the page reads lets the next go: once p0 is read, p1's head goes, and its body waits.
  await page.readUntil('0:p0')
  void other.send('s1')
  void channel.close()
  expect(reads(await page.readUntil('0:CLOSE'))).toEqual(['0:p0', '1:s1', '0:p1', '0:CLOSE'])
  await page.close()
})
