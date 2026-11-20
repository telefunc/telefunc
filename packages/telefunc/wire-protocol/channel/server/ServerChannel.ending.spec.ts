import { expect, test, vi } from 'vitest'

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { config, getChannelConfig } from '../../../node/server/serverConfig.js'
import { Abort } from '../../../shared/Abort.js'
import { CHANNEL_PUMP_TAG_DATA, CREDIT_WINDOW_INITIAL_BYTES, WIRE_VERSION } from '../../constants.js'
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
import { getBroadcastAdapter } from '../../broadcast/broadcastAdapter.js'
import { pairPublishes } from '../client/pairPublishes.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { ChannelStreamSource } from '../../values/ChannelStreamSource.js'
import { ChannelOverflowError } from '../channelErrors.js'
import { getChannelMux } from './ChannelMux.js'
import { admit, hold, letGo, queueMemory, releaseHolder, type MemoryHolder } from './channelMemory.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { ServerChannel } from './ServerChannel.js'
import { handleSseChannelRequest } from './sse.js'

// The least it may be, the credit window a channel starts with: a test can bring one channel to hold the most it may. The
// server may hold 32 MiB in all, far more than these tests leave held.
config.channel = { maxPendingBytes: CREDIT_WINDOW_INITIAL_BYTES, maxMemory: '32m' }

/** Three messages, each larger than what a wire sends before its page says it took some. */
const MESSAGES = ['p1', 'p2', 'p3'].map((name) => name + 'x'.repeat(100_000))

/** A message as `<channel index>:<name>`, a CLOSE, an ABORT or an ERROR, with its channel's seq. */
type Sent = { index: number; seq: number; label: string }

/** What `frames` carry, in order. */
function sent(frames: Iterable<Uint8Array>): Sent[] {
  const pair = pairPublishes()
  const out: Sent[] = []
  for (const frame of frames) {
    const decoded = pair(decode(frame) as DecodedFrame)
    if (decoded === null || !isSequencedFrame(decoded)) continue
    const { index, seq } = decoded
    if (
      decoded.tag === TAG.PUBLISH ||
      decoded.tag === TAG.TEXT ||
      decoded.tag === TAG.TEXT_ACK_REQ ||
      decoded.tag === TAG.ACK_RES
    )
      out.push({ index, seq, label: `${index}:${(parse(decoded.text) as string).slice(0, 2)}` })
    if (decoded.tag === TAG.PUBLISH_BINARY || decoded.tag === TAG.BINARY)
      out.push({ index, seq, label: `${index}:${new TextDecoder().decode(decoded.data.subarray(0, 2))}` })
    if (decoded.tag === TAG.CLOSE) out.push({ index, seq, label: `${index}:CLOSE` })
    if (decoded.tag === TAG.ABORT) out.push({ index, seq, label: `${index}:ABORT` })
    if (decoded.tag === TAG.ERROR) out.push({ index, seq, label: `${index}:ERROR` })
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

/** Two subscribers of one key on one page, at channel indexes 0 and 1. */
function subscribers() {
  const key = crypto.randomUUID()
  return { key, ended: new ServerBroadcast<string>({ key }), other: new ServerBroadcast<string>({ key }) }
}

/** Subscribes the page to `channels`, at indexes 0, 1, …, over its event stream, which it doesn't read yet. */
async function overSse(channels: ServerChannel[]) {
  const mux = getChannelMux()
  for (const channel of channels) mux.registerChannel(channel)
  const want = { version: 1, text: true, binary: false }
  const open = channels.map((channel, ix) => ({ id: channel.id, ix, lastSeq: 0, initial: true as const, want }))
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const connId = crypto.randomUUID()
  const body = encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open })])
  const opened = await handleSseChannelRequest(new Request(url, { method: 'POST', body }))
  await Promise.all(channels.map((channel) => channel.opened))
  const reader = (opened.body as ReadableStream<Uint8Array>).getReader()
  const events = new SseEvents()
  const frames: Uint8Array[] = []
  return {
    /** Reads the event stream until the page has read `last`, or the stream ends, and returns the frames it carried. */
    async readUntil(last: string): Promise<Uint8Array[]> {
      while (!reads(frames).includes(last)) {
        const read = await reader.read()
        if (read.done) break
        events.push(read.value, (frame) => frames.push(frame))
      }
      return frames
    },
    /** Reads `bytes` of the event stream, whole events or not. */
    async readBytes(bytes: number): Promise<void> {
      for (let read = 0; read < bytes; ) {
        const chunk = await reader.read()
        if (chunk.done) return
        read += chunk.value.byteLength
        events.push(chunk.value, (frame) => frames.push(frame))
      }
    },
    /** Reads the event stream to its end, and returns the frames it carried. */
    async readToEnd(): Promise<Uint8Array[]> {
      for (let read = await reader.read(); !read.done; read = await reader.read())
        events.push(read.value, (frame) => frames.push(frame))
      return frames
    },
    /** The page leaves for good, as a closed tab does. */
    leave: () =>
      handleSseChannelRequest(new Request(url, { method: 'POST', body: encodeSseRequest({ connId, close: true }) })),
    /** Ends the page's event stream. */
    close: () => reader.cancel(),
  }
}

/** What this server's channels hold, on the ledger every channel shares. */
const ledgerTotal = () => (globalThis as any)._telefunc['channelMemory.ts'].total as number

/** Holds what one-to-one channels would, which no publish can make them let go of, leaving `room` below the shedding
 *  target; returns what lets it go. */
function leaveRoom(room: number): () => void {
  const filler: MemoryHolder = { heldBytes: 0 }
  hold(filler, (getChannelConfig().maxMemory * 7) / 8 - ledgerTotal() - room)
  return () => {
    letGo(filler, filler.heldBytes)
    releaseHolder(filler)
  }
}

/** Attaches the page to `channels`, at indexes 0, 1, …, subscribed to the broadcasts, over a WebSocket it doesn't read
 *  yet: past the first frame, what the wire sends waits for headroom. `requests[index]` opened that channel's wire. */
function overWebSocket(channels: ServerChannel[], requests: Request[] = []) {
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
  /** Attaches `channel` at `index` as the mux does: what the page lacks after `lastSeq` goes first. */
  const attach = (channel: ServerChannel, index: number, lastSeq = 0, request?: Request) => {
    const replay = channel._replayBuffer!
    channel._onPageAttach(lastSeq, request)
    for (const frame of replay.getAfter(lastSeq)) sender.send(frame)
    const peer = new IndexedPeer({ sender, index, replay })
    channel._attachPeer(peer, { want: { version: 1, text: true, binary: true } })
    return peer
  }
  const peers = channels.map((channel, index) => {
    channel._registerChannel()
    return attach(channel, index, 0, requests[index])
  })
  return {
    headroom,
    peers,
    attach,
    /** Each frame the page got, whole. */
    arrived,
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
  await Promise.all(MESSAGES.map((data) => Broadcast.publish(key, data)))
  ended.abort()

  // What the wire carries: the page itself drops what of channel 0 follows its ABORT.
  const frames = await page.readUntil('1:p3')
  expect(sent(frames).map(({ label }) => label)).toEqual(['0:ABORT', '1:p1', '1:p2', '1:p3'])
  await page.close()
})

test('on SSE, a page that left for good is sent nothing of what waited for it', async () => {
  const key = crypto.randomUUID()
  const channel = new ServerBroadcast<string>({ key })
  const page = await overSse([channel])
  // The page reads nothing yet, so the publishes wait for its event stream.
  await Promise.all(MESSAGES.map((data) => Broadcast.publish(key, data)))
  await page.leave()
  expect(reads(await page.readToEnd())).toEqual([])
})

test("on SSE, after the page's abort nothing of that channel waits for the stream, its sends that pass another channel's event too", async () => {
  const ended = new ServerChannel()
  const other = new ServerChannel()
  const page = await overSse([ended, other])
  void other.send('p0')
  await page.readUntil('1:p0')
  for (const message of MESSAGES) void other.send(message)
  // Of the other channel, p1's event started: channel 0's 1 kB sends wait to pass it once it ends.
  for (const name of ['s1', 's2', 's3']) void ended.send(name + 'x'.repeat(1_000))
  fromPage(ended, encode.abort(0, stringify(undefined), 1))
  expect(ended.isClosed).toBe(true)
  expect(reads(await page.readUntil('1:p3'))).toEqual(['1:p0', '1:p1', '1:p2', '1:p3'])
  other.abort()
  await page.close()
})

/** The page sends `frame` on channel 0, its first. */
function fromPage(channel: ServerChannel, frame: Uint8Array<ArrayBuffer>): void {
  channel._dispatchFrame(decode(frame) as ChannelFrame)
}

/** Each way channel 0 ends while its wire has started p1 and not the rest. Its ABORT and its answer to the page's close
 *  go ahead of p1, the too-many-answers ERROR waits behind it, and after the page's own abort or lost replay the server
 *  sends no frame that ends it. */
const ENDINGS: Record<string, (channel: ServerChannel) => void> = {
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
  test(`on a WebSocket, after ${ending} its publishes the wire had not started wait no more, and the other channel's still wait in order`, async () => {
    const { key, ended, other } = subscribers()
    const { headroom } = overWebSocket([ended, other])

    await Promise.all(MESSAGES.map((data) => Broadcast.publish(key, data)))
    // Of channel 0, p1 started, so only p2 and p3 wait.
    expect(reads(headroom.sender.waitingFrames)).toEqual(['1:p1', '0:p2', '1:p2', '0:p3', '1:p3'])
    end(ended)
    expect(ended.isClosed).toBe(true)
    // Of channel 0, only the too-many-answers ERROR may still wait, behind the p1 its wire started.
    expect(reads(headroom.sender.waitingFrames).filter((read) => read !== '0:ERROR')).toEqual(['1:p1', '1:p2', '1:p3'])
    other.abort()
  })
}

/** Each way channel 0 starts to close and has not ended: the page's close waits for the channel's own publish. */
const CLOSING: Record<string, (channel: ServerBroadcast<string>) => void> = {
  'close()': (channel) => void channel.close(),
  "the page's close while a publish of its own is on its way": (channel) => {
    void channel.publish('p0')
    fromPage(channel, encode.close(0, 1_000, 1))
  },
}

for (const [closing, begin] of Object.entries(CLOSING)) {
  test(`on a WebSocket, after ${closing} a publish to its key leaves nothing more of it waiting for the wire or held`, async () => {
    const key = crypto.randomUUID()
    const channel = new ServerBroadcast<string>({ key })
    const other = new ServerChannel()
    const { headroom } = overWebSocket([channel, other])
    // The wire is busy with another channel's message: what comes after it waits.
    void other.send(MESSAGES[0])
    begin(channel)
    expect(channel._phase).toBe('closing')
    const left = () => ({
      waiting: sent(headroom.sender.waitingFrames).filter(({ index }) => index === 0),
      held: channel.heldBytes,
    })
    const before = left()
    const publishing = Broadcast.publish(key, MESSAGES[0]!)
    expect(left()).toEqual(before)
    await publishing
    other.abort()
  })
}

/** Each kind of publish to `key`, of `data` as text or as its bytes. */
const PUBLISHES: Record<string, (key: string, data: string) => unknown> = {
  text: (key, data) => Broadcast.publish(key, data),
  binary: (key, data) => Broadcast.publishBinary(key, new TextEncoder().encode(data)),
}

/** A subscriber that began to close, near the server's budget: each publish waiting for its wire, and what its page
 *  reads after a publish that would take room the closing subscriber holds. A reliable one may miss nothing published
 *  before its close; a lossy one's close goes behind the newest. */
const NEAR_THE_BUDGET: Record<string, { lossy: boolean; after: number; pageReads: string[] }> = {
  reliable: { lossy: false, after: 600_000, pageReads: ['0:p1', '0:p2', '0:p3', '0:CLOSE'] },
  lossy: { lossy: true, after: 900_000, pageReads: ['0:p1', '0:p3', '0:CLOSE'] },
}

for (const [kind, publish] of Object.entries(PUBLISHES)) {
  for (const [subscriber, { lossy, after, pageReads }] of Object.entries(NEAR_THE_BUDGET)) {
    test(`on a WebSocket, a ${kind} publish after a ${subscriber} subscriber's close() near the budget costs it nothing it was owed`, async () => {
      const key = crypto.randomUUID()
      const channel = new ServerBroadcast<string>({ key, lossy })
      const page = overWebSocket([channel])
      for (const name of ['p1', 'p2', 'p3']) await publish(key, name + 'x'.repeat(600_000))
      const closing = channel.close()
      const release = leaveRoom(after / 2)
      try {
        await publish(key, 'p4' + 'x'.repeat(after))
        expect(await page.readAll()).toEqual(pageReads)
        fromPage(channel, encode.closeAck(0, 1))
        expect(await closing).toBe(0)
      } finally {
        release()
      }
    })
  }

  test(`on a WebSocket, a ${kind} publish near the budget for an open lossy subscriber replaces nothing a closing one owes`, async () => {
    const key = crypto.randomUUID()
    const channel = new ServerBroadcast<string>({ key, lossy: true })
    const open = new ServerBroadcast<string>({ key, lossy: true })
    const page = overWebSocket([channel, open])
    for (const name of ['p1', 'p2', 'p3']) await publish(key, name + 'x'.repeat(600_000))
    const closing = channel.close()
    const release = leaveRoom(450_000)
    try {
      // Its body is admitted for the open one, which lets go of what it replaces there.
      await publish(key, 'p4' + 'x'.repeat(900_000))
      const pageReads = await page.readAll()
      expect(pageReads.filter((read) => read.startsWith('0:'))).toEqual(['0:p1', '0:p3', '0:CLOSE'])
      expect(pageReads.filter((read) => read.startsWith('1:'))).toEqual(['1:p4'])
      fromPage(channel, encode.closeAck(0, 1))
      expect(await closing).toBe(0)
    } finally {
      release()
      open.abort()
    }
  })
}

for (const [kind, publish] of Object.entries(PUBLISHES)) {
  test(`on a WebSocket, a closing subscriber cut for another object's publish reads the error at the next ${kind} publish of its key`, async () => {
    // Each object writes only to the pages of the wires its requests opened, as a Durable Object does.
    const adapter = getBroadcastAdapter()
    adapter.ownerOf = (request) => request?.url ?? 'server'
    const [key, otherKey] = [crypto.randomUUID(), crypto.randomUUID()]
    const channel = new ServerBroadcast<string>({ key })
    const other = new ServerBroadcast<string>({ key: otherKey })
    const page = overWebSocket([channel, other], [new Request('http://one.test/'), new Request('http://two.test/')])
    for (const name of ['p1', 'p2', 'p3']) await publish(key, name + 'x'.repeat(600_000))
    const closing = channel.close()
    const release = leaveRoom(300_000)
    try {
      // The other object's publish cuts channel 0, which only its own object can end.
      await publish(otherKey, 'q1' + 'x'.repeat(600_000))
      await publish(key, 'p4')
      expect((await page.readAll()).filter((read) => read.startsWith('0:'))).toEqual(['0:p1', '0:ERROR'])
      expect(await closing).toBe(1)
    } finally {
      release()
      other.abort()
      delete adapter.ownerOf
    }
  })
}

test("on a WebSocket, a closing subscriber cut for another object's publish reads the error when its close runs out", async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  const adapter = getBroadcastAdapter()
  adapter.ownerOf = (request) => request?.url ?? 'server'
  const [key, otherKey] = [crypto.randomUUID(), crypto.randomUUID()]
  const channel = new ServerBroadcast<string>({ key })
  const other = new ServerBroadcast<string>({ key: otherKey })
  const page = overWebSocket([channel, other], [new Request('http://one.test/'), new Request('http://two.test/')])
  for (const name of ['p1', 'p2', 'p3']) await Broadcast.publish(key, name + 'x'.repeat(600_000))
  const closing = channel.close({ timeout: 1_000 })
  const release = leaveRoom(300_000)
  try {
    // The other object's publish cuts channel 0, its CLOSE too, so no answer comes before the close runs out.
    await Broadcast.publish(otherKey, 'q1' + 'x'.repeat(600_000))
    await vi.advanceTimersByTimeAsync(1_000)
    expect(await closing).toBe(1)
    vi.useRealTimers()
    expect((await page.readAll()).filter((read) => read.startsWith('0:'))).toEqual(['0:p1', '0:ERROR'])
  } finally {
    vi.useRealTimers()
    release()
    other.abort()
    delete adapter.ownerOf
  }
})

for (const attach of ['before', 'after'] as const) {
  test(`on a WebSocket, a subscriber cut for another object's publish while its page is away and its close waits reads the error alone, its page back ${attach} the close runs out`, async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
    const adapter = getBroadcastAdapter()
    adapter.ownerOf = (request) => request?.url ?? 'server'
    const [key, otherKey] = [crypto.randomUUID(), crypto.randomUUID()]
    const channel = new ServerBroadcast<string>({ key })
    const other = new ServerBroadcast<string>({ key: otherKey })
    const request = new Request('http://one.test/')
    const page = overWebSocket([channel, other], [request, new Request('http://two.test/')])
    for (const name of ['p1', 'p2', 'p3']) await Broadcast.publish(key, name + 'x'.repeat(600_000))
    channel._onPeerDisconnect(page.peers[0]!, 60_000)
    // Its page is away, so the CLOSE waits for it to attach again.
    const closing = channel.close({ timeout: 1_000 })
    const release = leaveRoom(300_000)
    try {
      await Broadcast.publish(otherKey, 'q1' + 'x'.repeat(600_000))
      const back = overWebSocket([])
      // The page has p1.
      if (attach === 'before') back.attach(channel, 0, 1, request)
      await vi.advanceTimersByTimeAsync(1_000)
      expect(await closing).toBe(1)
      vi.useRealTimers()
      if (attach === 'after') back.attach(channel, 0, 1, request)
      expect(await back.readAll()).toEqual(['0:ERROR'])
    } finally {
      vi.useRealTimers()
      release()
      other.abort()
      delete adapter.ownerOf
    }
  })
}

test('on SSE, a subscriber cut to make room ends after the call that cut it, which the room it let go of admits', async () => {
  const subscriber = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  const page = await overSse([subscriber])
  let closedWith: Error | undefined
  subscriber.onClose((err) => {
    closedWith = err
  })
  // The page reads nothing yet, so the publishes wait for its event stream.
  await Promise.all(MESSAGES.map((data) => subscriber.publish(data)))
  const release = leaveRoom(0)
  try {
    expect(admit({ heldBytes: 0 }, 1_000)).toBe(true)
    expect(closedWith).toBeUndefined()
    // Less than one of its publishes: those its page lacks went.
    expect(subscriber.heldBytes).toBeLessThan(100_000)
    await new Promise((resolve) => setTimeout(resolve))
    expect(closedWith).toBeInstanceOf(ChannelOverflowError)
  } finally {
    release()
    await page.leave()
    await page.close()
  }
})

/** Each way the app ends a channel. */
const APP_ENDS: Record<string, (channel: ServerChannel) => void> = {
  closes: (channel) => void channel.close(),
  aborts: (channel) => channel.abort(),
}

for (const [ends, end] of Object.entries(APP_ENDS)) {
  test(`on a WebSocket, a subscriber cut to make room that the app ${ends} before it ended reads the error, as its onClose does`, async () => {
    const key = crypto.randomUUID()
    const subscriber = new ServerBroadcast<string>({ key })
    const page = overWebSocket([subscriber])
    for (const name of ['p1', 'p2', 'p3']) await Broadcast.publish(key, name + 'x'.repeat(600_000))
    let closedWith: Error | undefined
    subscriber.onClose((err) => {
      closedWith = err
    })
    const release = leaveRoom(0)
    try {
      admit({ heldBytes: 0 }, 1_000)
      end(subscriber)
      expect(await page.readAll()).toEqual(['0:p1', '0:ERROR'])
      expect(closedWith).toBeInstanceOf(ChannelOverflowError)
    } finally {
      release()
    }
  })
}

/** Each way an application's `onClose` of one channel ends another, and the last frame that other's page then reads. A
 *  graceful close comes behind what waits for the page; an abort drops what of that may go unsent. */
const ENDED_BY_A_CLOSE_CALLBACK: Record<
  string,
  { end: (channel: ServerChannel) => void; last: string; drops: boolean }
> = {
  closes: { end: (channel) => void channel.close(), last: 'CLOSE', drops: false },
  aborts: { end: (channel) => channel.abort(), last: 'ABORT', drops: true },
}

for (const [ends, { end, last, drops }] of Object.entries(ENDED_BY_A_CLOSE_CALLBACK)) {
  test(`on a WebSocket, a subscriber that another's onClose ${ends} once its publish was admitted reads the ${last} behind it, and the subscriber after it gets the publish`, async () => {
    const [cutKey, key] = [crypto.randomUUID(), crypto.randomUUID()]
    const cut = new ServerBroadcast<string>({ key: cutKey })
    const channel = new ServerBroadcast<string>({ key })
    const after = new ServerBroadcast<string>({ key })
    const page = overWebSocket([cut, channel, after])
    for (const name of ['p1', 'p2', 'p3']) await Broadcast.publish(cutKey, name + 'x'.repeat(600_000))
    cut.onClose(() => end(channel))
    const release = leaveRoom(300_000)
    try {
      // Admitting it for channel 1 cuts channel 0, whose onClose ends channel 1 once the publish was delivered.
      await Broadcast.publish(key, 'p4' + 'x'.repeat(600_000))
      const pageReads = await page.readAll()
      expect(pageReads.filter((read) => read.startsWith('1:'))).toEqual(drops ? [`1:${last}`] : ['1:p4', `1:${last}`])
      expect(pageReads.filter((read) => read.startsWith('2:'))).toEqual(['2:p4'])
    } finally {
      release()
      for (const subscriber of [cut, channel, after]) subscriber.abort()
    }
  })
}

/** Each send that asks the ledger for room, whether an abort may drop it unsent, and how its promise settles once the
 *  page left: an acknowledged one the page never answered rejects. */
const SENDS: Record<
  string,
  { send: (channel: ServerChannel) => Promise<unknown>; droppable: boolean; settles: 'resolved' | 'rejected' }
> = {
  'send()': { send: (channel) => channel.send('s1' + 'x'.repeat(1_000)), droppable: false, settles: 'resolved' },
  'send() with ack': {
    send: (channel) => channel.send('s1' + 'x'.repeat(1_000), { ack: true }),
    droppable: false,
    settles: 'rejected',
  },
  'sendBinary()': {
    send: (channel) => channel.sendBinary(new TextEncoder().encode('s1' + 'x'.repeat(1_000))),
    droppable: false,
    settles: 'resolved',
  },
  'a lossy send()': {
    send: (channel) => channel.send('s1' + 'x'.repeat(1_000), { lossy: true }),
    droppable: true,
    settles: 'resolved',
  },
}

for (const [ends, { end, last, drops }] of Object.entries(ENDED_BY_A_CLOSE_CALLBACK)) {
  for (const [name, { send, droppable, settles }] of Object.entries(SENDS)) {
    test(`on SSE, ${name} that cuts a subscriber whose onClose ${ends} its channel goes, the ${last} behind it, and nothing stays pending or held`, async () => {
      const channel = new ServerChannel()
      const subscriber = new ServerBroadcast<string>({ key: crypto.randomUUID() })
      const page = await overSse([channel, subscriber])
      subscriber.onClose(() => end(channel))
      // The page reads nothing yet, so the publishes wait for its event stream.
      await Promise.all(MESSAGES.map((data) => subscriber.publish(data)))
      // No room is left: the send's admission cuts the subscriber, whose onClose ends the channel after the send.
      const release = leaveRoom(0)
      try {
        const settled = send(channel).then(
          () => 'resolved',
          () => 'rejected',
        )
        expect(channel.isClosed).toBe(false)
        const frames = await page.readUntil(`0:${last}`)
        expect(reads(frames).filter((read) => read.startsWith('0:'))).toEqual(
          drops && droppable ? [`0:${last}`] : ['0:s1', `0:${last}`],
        )
        await page.leave()
        expect(await settled).toBe(settles)
        expect(channel.heldBytes).toBe(0)
      } finally {
        release()
        await page.close()
      }
    })
  }
}

test('on SSE, an acknowledged send made before a response carried its channel rejects where carrying it cuts a subscriber whose onClose aborts the channel', async () => {
  const subscriber = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  const page = await overSse([subscriber])
  // The page reads nothing yet, so the publishes wait for its event stream.
  await Promise.all(MESSAGES.map((data) => subscriber.publish(data)))
  const channel = new ServerChannel()
  let rejectedWith: unknown
  channel.send('s1' + 'x'.repeat(1_000), { ack: true }).catch((err: unknown) => {
    rejectedWith = err
  })
  subscriber.onClose(() => channel.abort())
  const release = leaveRoom(0)
  try {
    // Admitted again with all this server's channels, the send cuts the subscriber, whose onClose aborts the channel.
    getChannelMux().registerChannel(channel)
    await new Promise((resolve) => setTimeout(resolve))
    expect(rejectedWith).toBeInstanceOf(Abort)
  } finally {
    release()
    await page.leave()
    await page.close()
  }
})

test("on SSE, the answer to the page's acknowledged send that cuts a subscriber whose onClose aborts its channel goes, the ABORT behind it", async () => {
  const channel = new ServerChannel()
  const subscriber = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  const page = await overSse([channel, subscriber])
  subscriber.onClose(() => channel.abort())
  channel.listen(() => 'a1' + 'x'.repeat(100_000))
  // The page reads nothing yet, so the publishes wait for its event stream.
  await Promise.all(MESSAGES.map((data) => subscriber.publish(data)))
  // Room for the request, not for its answer: the answer's admission cuts the subscriber, whose onClose aborts the channel.
  const release = leaveRoom(50_000)
  try {
    fromPage(channel, encode.textAckReq(0, stringify('q1'), 1))
    const frames = await page.readUntil('0:ABORT')
    expect(reads(frames).filter((read) => read.startsWith('0:'))).toEqual(['0:a1', '0:ABORT'])
    await page.leave()
    expect(channel.heldBytes).toBe(0)
  } finally {
    release()
    await page.close()
  }
})

/** Each small frame of channel 0 that follows a publish its page has read, which nothing of channel 0 is then ahead of:
 *  its CLOSE, and a 1 kB send after a lossy one, which goes as a publish. */
type AfterAPublish = {
  create: (key: string) => ServerChannel
  publish: (channel: ServerChannel, key: string) => void
  small: (channel: ServerChannel) => void
  reads: [string, string]
}
const AFTER_A_PUBLISH: Record<string, AfterAPublish> = {
  'its close': {
    create: (key) => new ServerBroadcast<string>({ key }),
    publish: (_, key) => void Broadcast.publish(key, 'p0'),
    small: (channel) => void channel.close(),
    reads: ['0:p0', '0:CLOSE'],
  },
  'a 1 kB send after a lossy one': {
    create: () => new ServerChannel(),
    publish: (channel) => void channel.send('l0', { lossy: true }),
    small: (channel) => void channel.send('r1' + 'x'.repeat(1_000)),
    reads: ['0:l0', '0:r1'],
  },
}

for (const [
  name,
  {
    create,
    publish,
    small,
    reads: [published, sent],
  },
] of Object.entries(AFTER_A_PUBLISH)) {
  test(`on a WebSocket, once the page read a publish, ${name} goes ahead of what of another channel waits`, async () => {
    const key = crypto.randomUUID()
    const channel = create(key)
    const other = new ServerChannel()
    const page = overWebSocket([channel, other])
    publish(channel, key)
    expect(await page.readAll()).toEqual([published])
    for (const message of MESSAGES) void other.send(message)
    small(channel)
    // Of the other channel, p1 started and its rest waits.
    expect(await page.readAll()).toEqual([published, sent, '1:p1', '1:p2', '1:p3'])
    other.abort()
  })

  test(`on SSE, once the page read a publish, ${name} goes ahead of what of another channel waits`, async () => {
    const key = crypto.randomUUID()
    const channel = create(key)
    const other = new ServerChannel()
    const page = await overSse([channel, other])
    publish(channel, key)
    await page.readUntil(published)
    for (const message of MESSAGES) void other.send(message)
    small(channel)
    // Of the other channel, p1's event started, and nothing passes an event once it started.
    expect(reads(await page.readUntil('1:p3'))).toEqual([published, '1:p1', sent, '1:p2', '1:p3'])
    await page.close()
  })
}

test("on SSE, a frame that goes between a publish's head and its body leaves that channel's close behind the body", async () => {
  const key = crypto.randomUUID()
  const channel = new ServerBroadcast<string>({ key })
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

/** Each way channel 0 ends while its page is away, after what it sends then: the page that comes back reads what it is
 *  owed of that, then the end. */
const WHILE_AWAY: Record<
  string,
  {
    create: (key: string) => ServerChannel
    away: (channel: ServerChannel, key: string) => unknown
    pageReads: string[]
  }
> = {
  'close()': {
    create: () => new ServerChannel(),
    away: (channel) => {
      void channel.send('p1')
      void channel.close()
    },
    pageReads: ['0:p1', '0:CLOSE'],
  },
  'abort()': {
    create: () => new ServerChannel(),
    away: (channel) => {
      void channel.send('p1')
      channel.abort()
    },
    pageReads: ['0:p1', '0:ABORT'],
  },
  'abort() after lossy sends': {
    create: () => new ServerChannel(),
    away: (channel) => {
      void channel.send('p1')
      void channel.send('l1', { lossy: true })
      void channel.send('l2', { lossy: true })
      channel.abort()
    },
    pageReads: ['0:p1', '0:ABORT'],
  },
  'close() after lossy sends': {
    create: () => new ServerChannel(),
    away: (channel) => {
      void channel.send('p1')
      void channel.send('l1', { lossy: true })
      void channel.send('l2', { lossy: true })
      void channel.close()
    },
    pageReads: ['0:p1', '0:l2', '0:CLOSE'],
  },
  'abort() after a lossy send that a reliable one follows': {
    create: () => new ServerChannel(),
    away: (channel) => {
      void channel.send('l1', { lossy: true })
      void channel.send('p1')
      channel.abort()
    },
    pageReads: ['0:p1', '0:ABORT'],
  },
  'an overflow': {
    create: (key) => new ServerBroadcast<string>({ key }),
    away: (_, key) =>
      Promise.all(['p1', 'p2', 'p3', 'p4'].map((name) => Broadcast.publish(key, name + 'x'.repeat(600_000)))),
    // No wire had any of them: from p1 on, all go.
    pageReads: ['0:ERROR'],
  },
}

for (const [ending, { create, away, pageReads }] of Object.entries(WHILE_AWAY)) {
  test(`on a WebSocket, after ${ending} while its page is away, the page that comes back reads what it is owed, then the end`, async () => {
    const key = crypto.randomUUID()
    const channel = create(key)
    const [peer] = overWebSocket([channel]).peers
    channel._onPeerDisconnect(peer!, 60_000)
    await away(channel, key)
    const back = overWebSocket([])
    back.attach(channel, 0)
    expect(await back.readAll()).toEqual(pageReads)
  })
}

test('on a WebSocket, abort() drops each lossy send its wire has not started, also one a reliable send follows', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  void channel.send('p0' + 'x'.repeat(100_000))
  void channel.send('l1', { lossy: true })
  void channel.send('p1')
  channel.abort()
  expect(await page.readAll()).toEqual(['0:p0', '0:p1', '0:ABORT'])
})

test("on a WebSocket, a lossy send that waits when the page closes goes unsent while the close's callbacks run", async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  let onCloseDone = () => {}
  channel.onClose(() => new Promise<void>((resolve) => (onCloseDone = resolve)))
  // The page granted 100 messages, so the lossy send after them waits for more.
  for (let i = 0; i < 100; i++) void channel.send(`p${i}`)
  void channel.send('l1', { lossy: true })
  fromPage(channel, encode.close(0, 1_000, 1))
  // More credit, while the close waits for its callback.
  fromPage(channel, encode.msgWindow(0, 200))
  expect(await page.readAll()).not.toContain('0:l1')
  onCloseDone()
  await channel.closed
})

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
    // The page starts reading: p1 leaves the server's queue, as it does on a WebSocket. The others come while its event
    // is under way, so they wait: once it ended, the stream would ask for the next.
    await Broadcast.publish(key, publishes[0]!)
    await page.readBytes(100_000)
    await Promise.all(publishes.slice(1).map((data) => Broadcast.publish(key, data)))
    expect(channel.isClosed).toBe(true)
    expect(reads(await page.readUntil('0:ERROR'))).toEqual(pageReads)
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

test('on a WebSocket, a reliable subscriber that fell behind lets go of each message its page confirms before the error', async () => {
  const key = crypto.randomUUID()
  const channel = new ServerBroadcast<string>({ key })
  overWebSocket([channel])
  const [first, ...rest] = OVERFLOWS['a publish is larger than all the server may hold']!.publishes
  await Broadcast.publish(key, first!)
  const oneMessage = channel.heldBytes
  await Promise.all(rest.map((data) => Broadcast.publish(key, data)))
  expect(channel.isClosed).toBe(true)
  const held = channel.heldBytes
  // The page read p1 and p2, of the same size, and says so.
  fromPage(channel, encode.window(0, CREDIT_WINDOW_INITIAL_BYTES, 2))
  expect(channel.heldBytes).toBe(held - 2 * oneMessage)
})

/** Each way the page leaves channel 0, which then reads nothing more of it. */
const PAGE_GONE: Record<string, (channel: ServerChannel) => void> = {
  "the page's abort": ENDINGS["the page's abort"]!,
  'the page losing its replay': ENDINGS['the page losing its replay']!,
}

for (const [ending, end] of Object.entries(PAGE_GONE)) {
  test(`on a WebSocket, after ${ending} nothing of that channel waits for the wire, its sends too, and the other channel's arrive in order`, async () => {
    const ended = new ServerChannel()
    const other = new ServerChannel()
    const page = overWebSocket([ended, other])
    for (const message of MESSAGES) {
      void ended.send(message)
      void other.send(message)
    }
    expect(reads(page.headroom.sender.waitingFrames)).toEqual(['1:p1', '0:p2', '1:p2', '0:p3', '1:p3'])
    end(ended)
    expect(ended.isClosed).toBe(true)
    expect(reads(page.headroom.sender.waitingFrames)).toEqual(['1:p1', '1:p2', '1:p3'])
    // Channel 0's p1 had started.
    expect(await page.readAll()).toEqual(['0:p1', '1:p1', '1:p2', '1:p3'])
    other.abort()
  })
}

test('on a WebSocket, abort() comes behind the sends made before it', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  for (const message of MESSAGES) void channel.send(message)
  channel.abort()
  expect(await page.readAll()).toEqual(['0:p1', '0:p2', '0:p3', '0:ABORT'])
})

/** The page's messages of 768 KiB with seqs 1 to 3, each starting with `firstByte(seq)`, of which a channel may hold
 *  two: the third passes its 2 MiB. */
function sendThreeFromPage(channel: ServerChannel, firstByte = (seq: number) => seq): void {
  for (let seq = 1; seq <= 3; seq++) {
    fromPage(channel, encode.binary(0, new Uint8Array(768 * 1024).fill(firstByte(seq), 0, 1), seq))
  }
}

test("on a WebSocket, a page's message its channel can't hold while a listener works on the ones before closes the channel with ChannelOverflowError, and reaches no listener", async () => {
  const held = ledgerTotal()
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  let closedWith: unknown
  channel.onClose((err) => {
    closedWith = err
  })
  const got: (number | undefined)[] = []
  let finish = () => {}
  const working = new Promise<void>((resolve) => {
    finish = resolve
  })
  channel.listenBinary((data) => {
    got.push(data.at(0))
    return working
  })
  sendThreeFromPage(channel)
  expect(closedWith).toBeInstanceOf(ChannelOverflowError)
  expect(ledgerTotal() - held).toBeLessThanOrEqual(getChannelConfig().maxPendingBytes)
  expect(await page.readAll()).toEqual(['0:ERROR'])
  expect(page.arrived.map((frame) => decode(frame))).toContainEqual(
    expect.objectContaining({ tag: TAG.ERROR, reason: ERROR_REASON.OVERFLOW }),
  )
  expect(got).toEqual([1, 2])
  // The listener is done, and the page confirms the ERROR.
  finish()
  await new Promise((resolve) => setTimeout(resolve))
  fromPage(channel, encode.window(0, CREDIT_WINDOW_INITIAL_BYTES, 1))
  expect(ledgerTotal()).toBe(held)
})

test("on a WebSocket, a stream argument's reader reads the chunks queued before a chunk its channel can't hold, then ChannelOverflowError", async () => {
  const held = ledgerTotal()
  const channel = new ServerChannel()
  overWebSocket([channel])
  const source = ChannelStreamSource.create(channel, { memory: queueMemory() })
  sendThreeFromPage(channel, () => CHANNEL_PUMP_TAG_DATA)
  const first = await source.readNextChunk()
  const second = await source.readNextChunk()
  const third = await source.readNextChunk().catch((err: unknown) => err)
  expect([first?.byteLength, second?.byteLength]).toEqual([768 * 1024 - 1, 768 * 1024 - 1])
  expect(third).toBeInstanceOf(ChannelOverflowError)
  fromPage(channel, encode.window(0, CREDIT_WINDOW_INITIAL_BYTES, 1))
  expect(ledgerTotal()).toBe(held)
})

// A channel whose data its page never confirms while the page keeps coming back: dated at a detach, judged at the next
// detach once the page attached after the date, as the page judges its own (decisions/516-sse-carrier.md, msg41 D1).

/** The cause a channel ends with once its page hasn't confirmed its data for `reconnectTimeout`. */
const unconfirmedFor = (reconnectTimeout: number) =>
  `Channel closed: the page hasn't confirmed what the server sent on it for over ${reconnectTimeout / 1_000} s of reconnects`

/** A channel of `create`'s over a WebSocket whose page took `sent()` and never confirms it: its wire goes at 0, its page
 *  comes back at 5 s having nothing, and that wire goes at `secondLossAt`. Returns how it ended, and what a page that
 *  comes back after that reads. */
async function overdue(
  create: (key: string) => ServerChannel,
  sent: (channel: ServerChannel, key: string) => Promise<unknown>,
  secondLossAt: number,
) {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  const key = crypto.randomUUID()
  const channel = create(key)
  const ended: (Error | undefined)[] = []
  channel.onClose((err) => void ended.push(err))
  const [peer] = overWebSocket([channel]).peers
  await sent(channel, key)
  channel._onPeerDisconnect(defined(peer), 10_000)
  await vi.advanceTimersByTimeAsync(5_000)
  const again = overWebSocket([]).attach(channel, 0)
  await vi.advanceTimersByTimeAsync(secondLossAt - 5_000)
  channel._onPeerDisconnect(again, 10_000)
  vi.useRealTimers()
  const back = overWebSocket([])
  back.attach(channel, 0)
  return { ended, reads: await back.readAll() }
}

function defined<T>(value: T | undefined): T {
  if (value === undefined) throw new Error('Expected a value')
  return value
}

const OVERDUE: Record<
  string,
  { create: (key: string) => ServerChannel; sent: (channel: ServerChannel, key: string) => Promise<unknown> }
> = {
  'a channel': { create: () => new ServerChannel(), sent: async (channel) => void channel.send('p1') },
  'a broadcast subscriber': {
    create: (key) => new ServerBroadcast<string>({ key }),
    sent: async (_, key) => void (await Broadcast.publish(key, 'p1')),
  },
  'a lossy broadcast subscriber': {
    create: (key) => new ServerBroadcast<string>({ key, lossy: true }),
    sent: async (_, key) => void (await Broadcast.publish(key, 'p1')),
  },
}

for (const [what, { create, sent }] of Object.entries(OVERDUE)) {
  test(`${what} whose page never confirms what it sent ends at the first detach past reconnectTimeout after the page came back: both ends read the cause, the page nothing else`, async () => {
    const { ended, reads } = await overdue(create, sent, 11_000)
    expect(ended).toHaveLength(1)
    expect(ended[0]?.message).toBe(unconfirmedFor(10_000))
    expect(reads).toEqual(['0:ERROR'])
  })
}

test('a channel whose page lost its wire again inside reconnectTimeout goes on: its data goes again', async () => {
  const { create, sent } = defined(OVERDUE['a channel'])
  const { ended, reads } = await overdue(create, sent, 9_000)
  expect(ended).toEqual([])
  expect(reads).toEqual(['0:p1'])
})

test("a channel whose page confirmed something between its losses isn't judged on the date before: its data gets through between resets", async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  try {
    const channel = new ServerChannel()
    const ended: (Error | undefined)[] = []
    channel.onClose((err) => void ended.push(err))
    const [peer] = overWebSocket([channel]).peers
    void channel.send('p1')
    void channel.send('p2')
    channel._onPeerDisconnect(defined(peer), 10_000)
    await vi.advanceTimersByTimeAsync(5_000)
    // The page has p1 now: what it hasn't confirmed is dated from its next loss.
    const again = overWebSocket([]).attach(channel, 0, 1)
    await vi.advanceTimersByTimeAsync(6_000)
    channel._onPeerDisconnect(again, 10_000)
    await vi.advanceTimersByTimeAsync(5_000)
    const third = overWebSocket([]).attach(channel, 0, 1)
    await vi.advanceTimersByTimeAsync(4_000)
    channel._onPeerDisconnect(third, 10_000)
    expect(ended).toEqual([])
  } finally {
    vi.useRealTimers()
  }
})

test('a channel is judged from its first loss without a confirmation since, not from its newest', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  try {
    const channel = new ServerChannel()
    const ended: (Error | undefined)[] = []
    channel.onClose((err) => void ended.push(err))
    const [peer] = overWebSocket([channel]).peers
    void channel.send('p1')
    // Losses at 0, 6 and 11 s, the page back between them having nothing: only the last is past 10 s from the first.
    channel._onPeerDisconnect(defined(peer), 10_000)
    await vi.advanceTimersByTimeAsync(1_000)
    const second = overWebSocket([]).attach(channel, 0)
    await vi.advanceTimersByTimeAsync(5_000)
    channel._onPeerDisconnect(second, 10_000)
    await vi.advanceTimersByTimeAsync(1_000)
    const third = overWebSocket([]).attach(channel, 0)
    await vi.advanceTimersByTimeAsync(4_000)
    channel._onPeerDisconnect(third, 10_000)
    expect(ended.map((err) => err?.message)).toEqual([unconfirmedFor(10_000)])
  } finally {
    vi.useRealTimers()
  }
})

test("a page simply away isn't judged by the date: the grace period ends it, as before", async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  try {
    const channel = new ServerChannel()
    const ended: (Error | undefined)[] = []
    channel.onClose((err) => void ended.push(err))
    const [peer] = overWebSocket([channel]).peers
    void channel.send('p1')
    channel._onPeerDisconnect(defined(peer), 10_000)
    await vi.advanceTimersByTimeAsync(10_000)
    expect(ended.map((err) => err?.message)).toEqual([
      'Channel timed out: client did not reconnect within grace period',
    ])
  } finally {
    vi.useRealTimers()
  }
})

for (const ending of ['close', 'abort'] as const) {
  test(`a channel the app ends with ${ending}() while its unconfirmed data holds a date leaves nothing pending`, async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
    try {
      const channel = new ServerChannel()
      const [peer] = overWebSocket([channel]).peers
      void channel.send('p1')
      channel._onPeerDisconnect(defined(peer), 10_000)
      await vi.advanceTimersByTimeAsync(5_000)
      const again = overWebSocket([]).attach(channel, 0)
      if (ending === 'close') void channel.close({ timeout: 1_000 })
      else channel.abort()
      channel._onPeerDisconnect(again, 10_000)
      await vi.advanceTimersByTimeAsync(10_000)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      vi.useRealTimers()
    }
  })
}
