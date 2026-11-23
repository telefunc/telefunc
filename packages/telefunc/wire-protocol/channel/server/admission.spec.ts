import { afterEach, expect, onTestFinished, test, vi } from 'vitest'

import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { config, getChannelConfig } from '../../../node/server/serverConfig.js'
import { CREDIT_WINDOW_INITIAL_BYTES } from '../../constants.js'
import {
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
import { ChannelOverflowError } from '../channelErrors.js'
import {
  cancelWait,
  hold,
  letGo,
  queueMemory,
  releaseHolder,
  type MemoryHolder,
  type RoomWait,
} from './channelMemory.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { ServerChannel } from './ServerChannel.js'

// One channel may hold 2 MiB, the credit window it starts with, and the server 32 MiB in all.
config.channel = { maxPendingBytes: CREDIT_WINDOW_INITIAL_BYTES, maxMemory: '32m' }

type Ledger = { total: number; reserved: number; holders: Set<MemoryHolder>; waiting: Iterable<RoomWait> }
/** The ledger every channel of this server shares. */
const ledger = () => (globalThis as unknown as { _telefunc: Record<string, Ledger> })._telefunc['channelMemory.ts']!

/** The room `maxMemory` keeps for sends waiting to be held: the most one channel may hold, within the top eighth. */
const WAITING_ROOM = Math.min(getChannelConfig().maxPendingBytes, getChannelConfig().maxMemory / 8)
/** The most bytes held: `maxMemory` less that room. */
const HELD_AT_MOST = getChannelConfig().maxMemory - WAITING_ROOM

// A collection the test can force, as `node --expose-gc` gives.
setFlagsFromString('--expose-gc')
const gc: unknown = runInNewContext('gc')
/** Collects what nothing reaches any more, across a few turns. */
async function collect() {
  if (typeof gc !== 'function') throw new Error('no gc')
  for (let i = 0; i < 8; i++) {
    await new Promise((resolve) => setTimeout(resolve, 0))
    gc()
  }
}

// Each row starts on an empty ledger: what a row's channels still hold or wait with as it ends goes.
afterEach(() => {
  const { holders, waiting } = ledger()
  for (const wait of waiting) cancelWait(wait)
  // A channel's publish bodies go first, as other holders share them (`releaseHolder`).
  for (const holder of holders) if (holder instanceof ServerChannel) holder._replayBuffer?.dispose()
  for (const holder of [...holders]) releaseHolder(holder)
})

/** Holds what one-to-one channels would, leaving `room` below the most bytes held; returns what lets it go. */
function leaveRoomUnderMax(room: number): () => void {
  const filler: MemoryHolder = { heldBytes: 0 }
  hold(filler, HELD_AT_MOST - ledger().total - room)
  return () => {
    letGo(filler, filler.heldBytes)
    releaseHolder(filler)
  }
}

/** As `leaveRoomUnderMax`, but `room` below the shedding target, the top eighth of `maxMemory`. */
function leaveRoom(room: number): () => void {
  const filler: MemoryHolder = { heldBytes: 0 }
  hold(filler, (getChannelConfig().maxMemory * 7) / 8 - ledger().total - room)
  return () => {
    letGo(filler, filler.heldBytes)
    releaseHolder(filler)
  }
}

/** Fills the ledger to `total` with `count` holders, each admitted as a page's input queue is. */
function fillTo(total: number, count: number): () => void {
  const holders = Array.from({ length: count }, () => queueMemory())
  const remaining = total - ledger().total
  const each = Math.floor(remaining / count)
  for (const [i, holder] of holders.entries())
    expect(holder.tryHold(each + (i === 0 ? remaining % count : 0))).toBe(true)
  return () => {
    for (const holder of holders) holder.release()
  }
}

/** `count` holders of `bytes` each; returns what lets them go. */
function smallHolders(count: number, bytes: number): () => void {
  const holders = Array.from({ length: count }, (): MemoryHolder => ({ heldBytes: 0 }))
  for (const holder of holders) hold(holder, bytes)
  return () => {
    for (const holder of holders) releaseHolder(holder)
  }
}

/** How `promise` has settled `ms` after the call. */
const settledIn = async (promise: Promise<unknown>, ms = 50) => {
  const outcome = promise.then(
    () => 'resolved',
    (error: unknown) => (error instanceof ChannelOverflowError ? 'overflow' : `rejected: ${String(error)}`),
  )
  return Promise.race([outcome, new Promise<string>((resolve) => setTimeout(() => resolve('pending'), ms))])
}

/** What the page reads of `frames`, in order, as `<channel index>:<the message's first two characters>`,
 *  `<channel index>:ABORT` or `<channel index>:CLOSE`, less each frame at or below the newest seq it has of that channel. */
function reads(frames: Iterable<Uint8Array>): string[] {
  const pair = pairPublishes()
  const newestSeq = new Map<number, number>()
  const out: string[] = []
  for (const frame of frames) {
    const decoded = pair(decode(frame) as DecodedFrame)
    if (decoded === null || !isSequencedFrame(decoded)) continue
    const { index, seq } = decoded
    if (seq <= (newestSeq.get(index) ?? 0)) continue
    newestSeq.set(index, seq)
    if (decoded.tag === TAG.PUBLISH || decoded.tag === TAG.TEXT || decoded.tag === TAG.TEXT_ACK_REQ)
      out.push(`${index}:${(parse(decoded.text) as string).slice(0, 2)}`)
    if (decoded.tag === TAG.PUBLISH_BINARY || decoded.tag === TAG.BINARY || decoded.tag === TAG.BINARY_ACK_REQ)
      out.push(`${index}:${new TextDecoder().decode(decoded.data.subarray(0, 2))}`)
    if (decoded.tag === TAG.ABORT) out.push(`${index}:ABORT`)
    if (decoded.tag === TAG.CLOSE) out.push(`${index}:CLOSE`)
  }
  return out
}

/** The page sends `frame` on `channel`. */
function fromPage(channel: ServerChannel, frame: Uint8Array<ArrayBuffer>): void {
  channel._dispatchFrame(decode(frame) as ChannelFrame)
}

/** The seq of the last frame `channel` sent its page. */
function sentThrough(channel: ServerChannel): number {
  return channel._replayBuffer?.seq ?? 0
}

/** The page says it has all `channel`, at `index`, sent it, and that the channel may send `limit` bytes in all. */
function confirm(channel: ServerChannel, index = 0, limit = 1): void {
  fromPage(channel, encode.window(index, limit, sentThrough(channel)))
}

/** Attaches the page to `channels`, at indexes 0, 1, …, over a WebSocket it doesn't read yet: past the first frame,
 *  what the wire sends waits for headroom. */
function overWebSocket(channels: ServerChannel[]) {
  const arrived: Uint8Array[] = []
  const receiver = new ClientPieceReceiver({ owner: { sendAck() {}, carryAck() {} } })
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
      headroom.send(frame)
    },
    carry() {},
    bufferedAmount: () => 0,
    behindAfter: () => Number.POSITIVE_INFINITY,
    deliveredBefore: () => 0,
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
  }
  /** Attaches `channel` at `index` as the mux does, the page having its frames through `lastSeq`: what it lacks goes
   *  first. */
  const attach = (channel: ServerChannel, index: number, lastSeq = 0) => {
    onTestFinished(() => {
      if (channel._phase === 'ended') channel._release()
    })
    const replay = channel._replayBuffer!
    channel._onPageAttach(lastSeq, undefined)
    for (const frame of replay.getAfter(lastSeq)) sender.send(frame)
    const peer = new IndexedPeer({ sender, index, replay })
    channel._attachPeer(peer, { want: { version: 1, text: true, binary: true } })
    return peer
  }
  const peers = channels.map((channel, index) => {
    channel._registerChannel()
    return attach(channel, index)
  })
  return {
    headroom,
    peers,
    attach,
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

test('on a WebSocket, channels that each await every send near maxMemory wait for their pages instead of being refused', async () => {
  // 48 channels of 24 × 32 KiB each hold 36 MiB unconfirmed, past the 32 MiB the server may hold.
  const channels = Array.from({ length: 48 }, () => new ServerChannel())
  overWebSocket(channels)
  let refused = 0
  let done = 0
  const sending = channels.map(async (channel) => {
    for (let n = 0; n < 24; n++) await channel.send('x'.repeat(32 * 1024)).catch(() => void refused++)
    done++
  })
  await new Promise((resolve) => setTimeout(resolve, 20))
  expect({ refused, overMax: ledger().total > getChannelConfig().maxMemory }).toEqual({ refused: 0, overMax: false })
  // The pages say they have all that was sent, again and again, until every send went.
  for (let round = 0; round < 200 && done < channels.length; round++) {
    for (const [index, channel] of channels.entries()) confirm(channel, index)
    await new Promise((resolve) => setTimeout(resolve))
  }
  await Promise.all(sending)
  expect(refused).toBe(0)
  for (const channel of channels) channel.abort()
})

test("on a WebSocket, a send its channel holds once its page confirms the one before waits for that, and an aborted subscriber's page still reads all it was sent", async () => {
  const key = crypto.randomUUID()
  const aborted = new ServerBroadcast<string>({ key })
  const live = new ServerChannel()
  const page = overWebSocket([aborted])
  overWebSocket([live])
  await new Promise((resolve) => setTimeout(resolve))
  for (const name of ['p1', 'p2']) await Broadcast.publish(key, name + 'x'.repeat(700_000))
  void live.send('q1' + 'x'.repeat(1_500_000))
  aborted.abort()
  const release = leaveRoom(0)
  try {
    // 1.5 MB and 1 MB are past the 2 MiB one channel may hold: q2 waits for q1's confirmation.
    expect(await settledIn(live.send('q2' + 'x'.repeat(1_000_000)), 5)).toBe('pending')
    release()
    expect((await page.readAll()).filter((read) => read.startsWith('0:'))).toEqual(['0:p1', '0:p2', '0:ABORT'])
  } finally {
    release()
    live.abort()
  }
})

test("on a WebSocket, a send that finds the ledger full waits, and goes once another channel's page confirms what it holds", async () => {
  // 15 channels each hold 2 MiB, the most one may: 30 MiB, all the ledger holds below the room it keeps for waiting sends.
  const confirming = new ServerChannel()
  const full = [confirming, ...Array.from({ length: 14 }, () => new ServerChannel())]
  const late = new ServerChannel()
  overWebSocket([...full, late])
  for (const channel of full) void channel.sendBinary(new Uint8Array(2 * 1024 * 1024 - 7 - 512 - 8))
  const leftover = leaveRoomUnderMax(0)
  const first = late.send('x'.repeat(1024))
  const before = await settledIn(first, 5)
  leftover()
  confirm(confirming)
  expect({ before, after: await settledIn(first) }).toEqual({ before: 'pending', after: 'resolved' })
  for (const channel of [...full, late]) channel.abort()
})

test("on a WebSocket, a send within credit that isn't awaited goes, and another channel's awaited send near the cap goes once the first's page confirms it", async () => {
  const release = smallHolders(41, 4_096)
  const room = leaveRoomUnderMax(1024 * 1024)
  const [fired, awaited] = [new ServerChannel(), new ServerChannel()]
  overWebSocket([fired, awaited])
  try {
    const firedSend = fired.sendBinary(new Uint8Array(900 * 1024))
    const awaitedSend = awaited.sendBinary(new Uint8Array(256 * 1024))
    const firedBefore = await settledIn(firedSend, 5)
    confirm(fired)
    expect({ firedBefore, awaitedAfter: await settledIn(awaitedSend) }).toEqual({
      firedBefore: 'resolved',
      awaitedAfter: 'resolved',
    })
  } finally {
    room()
    release()
    fired.abort()
    awaited.abort()
  }
})

test('on a WebSocket, a send made while its channel has one waiting for room is refused, and the waiting one arrives in order', async () => {
  const room = leaveRoomUnderMax(64 * 1024)
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  try {
    const first = channel.send('a1' + 'x'.repeat(100 * 1024))
    const second = await settledIn(channel.send('a2'), 5)
    room()
    const firstOutcome = await settledIn(first)
    await settledIn(channel.send('a3'))
    expect({ second, firstOutcome, pageReads: await page.readAll() }).toEqual({
      second: 'overflow',
      firstOutcome: 'resolved',
      pageReads: ['0:a1', '0:a3'],
    })
  } finally {
    room()
    channel.abort()
  }
})

test('on a WebSocket, what the ledger holds and what the sends waiting for room will hold stay within maxMemory', async () => {
  const channel = new ServerChannel()
  overWebSocket([channel])
  const free = leaveRoomUnderMax(0)
  try {
    const sending = channel.sendBinary(new Uint8Array(128 * 1024))
    expect(await settledIn(sending, 5)).toBe('pending')
    const { total, waiting } = ledger()
    const waits = [...waiting].reduce((bytes, wait) => bytes + wait.bytes, 0)
    expect(total + waits).toBeLessThanOrEqual(getChannelConfig().maxMemory)
  } finally {
    channel.abort()
    free()
  }
})

test("on a WebSocket, a send past what its channel may hold unconfirmed waits until its page confirms the channel's earlier one", async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  try {
    await channel.sendBinary(new Uint8Array(900 * 1024))
    const sending = channel.sendBinary(new Uint8Array(1_200 * 1024))
    const before = await settledIn(sending, 5)
    confirm(channel, 0, 2 * CREDIT_WINDOW_INITIAL_BYTES)
    await page.readAll()
    expect({ before, after: await settledIn(sending) }).toEqual({ before: 'pending', after: 'resolved' })
  } finally {
    channel.abort()
  }
})

test('on a WebSocket, a send that waits for room sends the bytes it was given, though the caller then writes into them', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  try {
    await channel.sendBinary(new TextEncoder().encode('a1' + 'x'.repeat(900 * 1024)))
    const data = new TextEncoder().encode('a2' + 'x'.repeat(1_200 * 1024))
    const sending = channel.sendBinary(data)
    data.fill(0x7a)
    expect(await settledIn(sending, 5)).toBe('pending')
    confirm(channel, 0, 2 * CREDIT_WINDOW_INITIAL_BYTES)
    expect(await page.readAll()).toEqual(['0:a1', '0:a2'])
    expect(await settledIn(sending)).toBe('resolved')
  } finally {
    channel.abort()
  }
})

test('on a WebSocket, a send that waits on its share of the room goes once a holder of no channel of its own lets go', async () => {
  const filler = smallHolders(41, 4096)
  const channel = new ServerChannel()
  overWebSocket([channel])
  await channel.sendBinary(new Uint8Array(32 * 1024))
  const free = leaveRoomUnderMax(1024 * 1024)
  try {
    const sending = channel.sendBinary(new Uint8Array(900 * 1024))
    const before = await settledIn(sending, 5)
    free()
    expect({ before, after: await settledIn(sending) }).toEqual({ before: 'pending', after: 'resolved' })
  } finally {
    channel.abort()
    free()
    filler()
  }
})

test("on a WebSocket, when the page goes, a send waiting for room and one waiting for credit both reject with the page's error", async () => {
  const [roomWaiter, creditWaiter] = [new ServerChannel(), new ServerChannel()]
  overWebSocket([roomWaiter, creditWaiter])
  const free = leaveRoomUnderMax(1536 * 1024)
  try {
    // Past the page's start credit, under the room left: it is held, and waits for credit.
    const credit = creditWaiter.sendBinary(new Uint8Array(1280 * 1024 - 512))
    // Past the room left: it waits for room, and holds nothing.
    const room = roomWaiter.sendBinary(new Uint8Array(1700 * 1024))
    expect(await Promise.all([settledIn(credit, 5), settledIn(room, 5)])).toEqual(['pending', 'pending'])
    expect({ credit: creditWaiter.heldBytes > 0, room: roomWaiter.heldBytes }).toEqual({ credit: true, room: 0 })
    const gone = new Error('page gone')
    creditWaiter._onPeerGone(gone)
    roomWaiter._onPeerGone(gone)
    expect(await Promise.allSettled([credit, room])).toEqual([
      { status: 'rejected', reason: gone },
      { status: 'rejected', reason: gone },
    ])
  } finally {
    roomWaiter.abort()
    creditWaiter.abort()
    free()
  }
})

test("on a WebSocket, a send that fits goes while another channel's send waits on its share of the room", async () => {
  const filler = smallHolders(41, 4096)
  const [older, later] = [new ServerChannel(), new ServerChannel()]
  overWebSocket([older, later])
  await older.sendBinary(new Uint8Array(32 * 1024))
  const free = leaveRoomUnderMax(1024 * 1024)
  try {
    const first = older.sendBinary(new Uint8Array(900 * 1024))
    const second = later.send('fits within the 1 MiB left')
    expect(await Promise.all([settledIn(first, 10), settledIn(second, 10)])).toEqual(['pending', 'resolved'])
  } finally {
    older.abort()
    later.abort()
    free()
    filler()
  }
})

test("on a WebSocket, with the ledger near full of input queues, a send that waits on its share of the room leaves another channel's send that fits its room", async () => {
  const [older, later] = [new ServerChannel(), new ServerChannel()]
  overWebSocket([older, later])
  await older.sendBinary(new Uint8Array(32 * 1024))
  const free = fillTo(HELD_AT_MOST - 1024 * 1024, 42)
  try {
    const waiting = older.sendBinary(new Uint8Array(900 * 1024))
    const fitting = later.sendBinary(new Uint8Array(512 * 1024))
    expect(await Promise.all([settledIn(waiting, 30), settledIn(fitting, 30)])).toEqual(['pending', 'resolved'])
  } finally {
    older._onPeerGone(new Error('fixture closed'))
    later._onPeerGone(new Error('fixture closed'))
    free()
  }
})

for (const ending of ['close', 'abort'] as const)
  test(`on a WebSocket, ${ending === 'close' ? 'a close' : 'an abort'} while a send waits at the most bytes held keeps the ledger within maxMemory`, async () => {
    const channel = new ServerChannel()
    overWebSocket([channel])
    const free = fillTo(HELD_AT_MOST, 15)
    try {
      const forRoom = channel.sendBinary(new Uint8Array(2 * 1024 * 1024 - 519))
      expect(await settledIn(forRoom, 5)).toBe('pending')
      expect({ total: ledger().total, reserved: ledger().reserved }).toEqual({
        total: HELD_AT_MOST,
        reserved: 2 * 1024 * 1024,
      })
      if (ending === 'close') void channel.close()
      else channel.abort('x'.repeat(256 * 1024))
      expect(ledger().total + ledger().reserved).toBeLessThanOrEqual(getChannelConfig().maxMemory)
    } finally {
      channel._onPeerGone(new Error('fixture closed'))
      free()
    }
  })

test('on a WebSocket, a lossy send behind a send waiting for its channel to be confirmed either is refused or reaches the page', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  try {
    await channel.sendBinary(new Uint8Array(900 * 1024))
    const waiting = channel.send('a2' + 'x'.repeat(1_200 * 1024))
    expect(await settledIn(waiting, 5)).toBe('pending')
    const lossy = await settledIn(channel.send('l3' + 'x'.repeat(900 * 1024), { lossy: true }), 5)
    confirm(channel)
    const read = await page.readAll()
    expect(lossy === 'overflow' || read.includes('0:l3')).toBe(true)
  } finally {
    channel._onPeerGone(new Error('fixture closed'))
  }
})

test("on a WebSocket, the server's end rejects a send waiting for room once, and gives back the room it waited for", async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  const free = fillTo(HELD_AT_MOST, 15)
  try {
    const waiting = channel.send('a1')
    const outcomes: unknown[] = []
    void waiting.then(
      () => outcomes.push('resolved'),
      (error: unknown) => outcomes.push(error),
    )
    expect(await settledIn(waiting, 5)).toBe('pending')
    const ended = new Error('server stopped')
    page.headroom.close()
    channel._end(ended)
    await expect(waiting).rejects.toBe(ended)
    free()
    await new Promise((resolve) => setTimeout(resolve))
    expect(outcomes).toEqual([ended])
    expect({ total: ledger().total, reserved: ledger().reserved }).toEqual({ total: 0, reserved: 0 })
  } finally {
    channel._onPeerGone(new Error('fixture closed'))
  }
})

/** A channel made with a send past what it may hold waiting for room, then dropped before any telefunction returned it. */
function droppedWithWaitingSend(): WeakRef<ServerChannel> {
  const channel = new ServerChannel()
  // 1.5 MB twice is past the 2 MiB one channel may hold: the second waits for room, holding a copy of its bytes.
  void channel.sendBinary(new Uint8Array(1_500_000))
  void channel.sendBinary(new Uint8Array(1_500_000))
  return new WeakRef(channel)
}

test('a channel dropped before its telefunction returned it, with a send waiting for room, leaves the ledger nothing', async () => {
  const dropped = droppedWithWaitingSend()
  await new Promise((resolve) => setTimeout(resolve))
  expect({ reserved: ledger().reserved, waiting: [...ledger().waiting].length }).toEqual({ reserved: 0, waiting: 0 })
  await collect()
  expect(dropped.deref()).toBeUndefined()
})

test('on a WebSocket, a send past what its channel may hold, made before its telefunction returned it, goes once its page confirms the one before', async () => {
  const channel = new ServerChannel()
  const first = channel.sendBinary(new Uint8Array(1_500_000))
  const second = channel.sendBinary(new Uint8Array(1_500_000))
  const page = overWebSocket([channel])
  try {
    const before = await settledIn(second, 5)
    await page.readAll()
    confirm(channel, 0, 2 * CREDIT_WINDOW_INITIAL_BYTES)
    expect({ before, first: await settledIn(first), second: await settledIn(second) }).toEqual({
      before: 'pending',
      first: 'resolved',
      second: 'resolved',
    })
  } finally {
    channel.abort()
  }
})

test('on a WebSocket, a send waiting for room, made before its telefunction returned the channel, is refused then if other sends hold the room', async () => {
  const channel = new ServerChannel()
  void channel.sendBinary(new Uint8Array(1_500_000))
  const second = channel.sendBinary(new Uint8Array(1_500_000))
  const other = new ServerChannel()
  overWebSocket([other])
  fillTo(HELD_AT_MOST, 15)
  try {
    expect(await settledIn(other.sendBinary(new Uint8Array(WAITING_ROOM - 519)), 5)).toBe('pending')
    const reserved = ledger().reserved
    channel._registerChannel()
    expect({ second: await settledIn(second, 5), reserved: ledger().reserved }).toEqual({
      second: 'overflow',
      reserved,
    })
  } finally {
    channel.abort()
    other.abort()
  }
})

test('on a WebSocket, a binary send refused while another of its channel waits for room copies none of its bytes', async () => {
  const channel = new ServerChannel()
  overWebSocket([channel])
  const free = fillTo(HELD_AT_MOST, 15)
  try {
    expect(await settledIn(channel.send('a1'), 5)).toBe('pending')
    const data = new Uint8Array(1024 * 1024)
    const nativeBytes = globalThis.Uint8Array
    let copied = 0
    globalThis.Uint8Array = new Proxy(nativeBytes, {
      construct(target, args, newTarget) {
        if (args[0] === data) copied += data.byteLength
        return Reflect.construct(target, args, newTarget)
      },
    })
    let refused: Promise<void>
    try {
      refused = channel.sendBinary(data)
    } finally {
      globalThis.Uint8Array = nativeBytes
    }
    expect({ outcome: await settledIn(refused, 5), copied }).toEqual({ outcome: 'overflow', copied: 0 })
  } finally {
    channel._onPeerGone(new Error('fixture closed'))
    free()
  }
})

test("on a WebSocket, an acknowledged send that waited for its channel's earlier one to be confirmed settles with the page's answer", async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  try {
    await channel.sendBinary(new Uint8Array(900 * 1024))
    const result = channel.sendBinary(new Uint8Array(1200 * 1024), { ack: true })
    expect(await settledIn(result, 5)).toBe('pending')
    fromPage(channel, encode.window(0, CREDIT_WINDOW_INITIAL_BYTES, sentThrough(channel)))
    await page.readAll()
    const ackSeq = sentThrough(channel)
    expect(ackSeq).toBe(2)
    const beforeAnswer = await settledIn(result, 5)
    fromPage(channel, encode.ackRes(0, 1, ackSeq, stringify('accepted')))
    expect({ beforeAnswer, answer: await result }).toEqual({ beforeAnswer: 'pending', answer: 'accepted' })
  } finally {
    channel._onPeerGone(new Error('fixture closed'))
  }
})

/** What the warning at a channel's first refused `send()` says refused it, for each of its causes. */
const REFUSALS: Record<string, { cause: string; refused: () => Promise<{ sent: Promise<unknown> }> }> = {
  'past the most one channel holds': {
    cause: `it would hold more than ${CREDIT_WINDOW_INITIAL_BYTES} bytes, the most for one channel`,
    refused: async () => {
      const channel = new ServerChannel()
      overWebSocket([channel])
      return { sent: channel.sendBinary(new Uint8Array(CREDIT_WINDOW_INITIAL_BYTES)) }
    },
  },
  'while another send of its channel waits for room': {
    cause: 'another `send()` of this channel already waits for room',
    refused: async () => {
      const channel = new ServerChannel()
      overWebSocket([channel])
      fillTo(HELD_AT_MOST, 15)
      expect(await settledIn(channel.send('a1'), 5)).toBe('pending')
      return { sent: channel.send('a2') }
    },
  },
  'once the sends waiting for room hold all of it': {
    cause: `what this server's channels hold and the sends waiting for room fill \`config.channel.maxMemory\`, ${getChannelConfig().maxMemory} bytes`,
    refused: async () => {
      const [waiting, refused] = [new ServerChannel(), new ServerChannel()]
      overWebSocket([waiting, refused])
      fillTo(HELD_AT_MOST, 15)
      expect(await settledIn(waiting.sendBinary(new Uint8Array(WAITING_ROOM - 519)), 5)).toBe('pending')
      return { sent: refused.send('b1') }
    },
  },
}
for (const [when, { cause, refused }] of Object.entries(REFUSALS)) {
  test(`on a WebSocket, the warning at a send() refused ${when} says so`, async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    try {
      const { sent } = await refused()
      expect(await settledIn(sent, 5)).toBe('overflow')
      const warnings = warn.mock.calls
        .map((args) => String(args[0]))
        .filter((text) => text.includes('refused a `send()`'))
      expect(warnings).toHaveLength(1)
      expect(warnings[0]).toContain(`refused a \`send()\`: ${cause}, so that message isn't delivered`)
    } finally {
      warn.mockRestore()
    }
  })
}

test('on a WebSocket, a lossy send made while a send of its channel waits for room reaches the page after it', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  const free = leaveRoomUnderMax(64 * 1024)
  try {
    const first = channel.send('a1' + 'x'.repeat(100 * 1024))
    expect(await settledIn(first, 5)).toBe('pending')
    void channel.send('l2', { lossy: true })
    free()
    expect(await settledIn(first)).toBe('resolved')
    expect(await page.readAll()).toEqual(['0:a1', '0:l2'])
  } finally {
    channel.abort()
    free()
  }
})

for (const ending of ['close', 'abort'] as const)
  test(`on a WebSocket, ${ending}() while a send waits for room sends it ahead of the ${ending === 'close' ? 'CLOSE' : 'ABORT'}`, async () => {
    const channel = new ServerChannel()
    const page = overWebSocket([channel])
    const free = leaveRoomUnderMax(64 * 1024)
    try {
      const sending = channel.send('a1' + 'x'.repeat(100 * 1024))
      expect(await settledIn(sending, 5)).toBe('pending')
      if (ending === 'close') void channel.close()
      else channel.abort()
      expect(await settledIn(sending, 5)).toBe('resolved')
      expect(await page.readAll()).toEqual(['0:a1', ending === 'close' ? '0:CLOSE' : '0:ABORT'])
    } finally {
      channel.abort()
      free()
    }
  })

test('on a WebSocket, abort() while an acknowledged send waits for room sends its request ahead of the ABORT, and the send rejects with the Abort', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  const free = fillTo(HELD_AT_MOST, 15)
  try {
    const waiting = channel.sendBinary(new TextEncoder().encode('a1' + 'x'.repeat(100 * 1024)), { ack: true })
    expect(await settledIn(waiting, 5)).toBe('pending')
    channel.abort()
    await expect(waiting).rejects.toMatchObject({ isAbort: true })
    expect(await page.readAll()).toEqual(['0:a1', '0:ABORT'])
  } finally {
    free()
  }
})

test('on a WebSocket, close() delivers the lossy sends on both sides of a send that waits for room, in the order they were made', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  try {
    await channel.send('l1', { lossy: true })
    const free = fillTo(HELD_AT_MOST, 16)
    try {
      const reliable = channel.send('a2')
      expect(await settledIn(reliable, 5)).toBe('pending')
      const lossy = await settledIn(channel.send('l3', { lossy: true }), 5)
      void channel.close()
      expect({ lossy, reliable: await settledIn(reliable), read: await page.readAll() }).toEqual({
        lossy: 'resolved',
        reliable: 'resolved',
        read: ['0:l1', '0:a2', '0:l3', '0:CLOSE'],
      })
    } finally {
      free()
    }
  } finally {
    channel._onPeerGone(new Error('fixture closed'))
  }
})

test("on a WebSocket, a lossy send too large to go with the send of its channel that waits for room is refused, and that one goes once its page confirms the channel's earlier one", async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  try {
    await channel.sendBinary(new Uint8Array(900 * 1024))
    const reliable = channel.send('a2' + 'x'.repeat(1_200 * 1024))
    expect(await settledIn(reliable, 5)).toBe('pending')
    const lossy = await settledIn(channel.sendBinary(new Uint8Array(29 * 1024 * 1024), { lossy: true }), 5)
    confirm(channel, 0, 2 * CREDIT_WINDOW_INITIAL_BYTES)
    await page.readAll()
    expect({ lossy, reliable: await settledIn(reliable) }).toEqual({ lossy: 'overflow', reliable: 'resolved' })
  } finally {
    channel._onPeerGone(new Error('fixture closed'))
  }
})

for (const wakesDetached of [false, true])
  test(`on a WebSocket, a send that waits for room ${wakesDetached ? 'and finds room while its page is away' : 'while its page is away'} reaches the page back in order, with what was sent before and after it`, async () => {
    const channel = new ServerChannel()
    const original = overWebSocket([channel])
    try {
      await channel.send('a0')
      const free = fillTo(HELD_AT_MOST, 16)
      const waiting = channel.send('a1')
      expect(await settledIn(waiting, 5)).toBe('pending')
      await channel.send('l2', { lossy: true })
      const [peer] = original.peers
      if (peer === undefined) throw new Error('no peer')
      channel._onPeerDisconnect(peer, 500)
      original.headroom.close()
      if (wakesDetached) {
        free()
        await Promise.resolve()
        expect(await settledIn(waiting, 5)).toBe('pending')
      }
      const back = overWebSocket([])
      back.attach(channel, 0)
      if (!wakesDetached) free()
      await waiting
      await channel.send('a3')
      expect(await back.readAll()).toEqual(['0:a0', '0:a1', '0:l2', '0:a3'])
      back.headroom.close()
    } finally {
      channel._onPeerGone(new Error('fixture closed'))
    }
  })

test('on a WebSocket, a send waiting for room goes with the lossy send behind it once there is room for the two', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  // Room for the two, about 165 kB, and not for the lossy one twice.
  const free = leaveRoomUnderMax(200_000)
  const blocker: MemoryHolder = { heldBytes: 0 }
  hold(blocker, 150_000)
  try {
    const waiting = channel.send('a1' + 'x'.repeat(100 * 1024))
    expect(await settledIn(waiting, 5)).toBe('pending')
    void channel.send('l2' + 'x'.repeat(60 * 1024), { lossy: true })
    letGo(blocker, blocker.heldBytes)
    releaseHolder(blocker)
    expect(await settledIn(waiting)).toBe('resolved')
    expect(await page.readAll()).toEqual(['0:a1', '0:l2'])
  } finally {
    channel.abort()
    free()
  }
})

test('on a WebSocket, a binary lossy send made while a send of its channel waits for room sends the bytes it was given, though the caller then writes into them', async () => {
  const channel = new ServerChannel()
  const page = overWebSocket([channel])
  const free = leaveRoomUnderMax(64 * 1024)
  try {
    const first = channel.send('a1' + 'x'.repeat(100 * 1024))
    expect(await settledIn(first, 5)).toBe('pending')
    const data = new TextEncoder().encode('l2')
    void channel.sendBinary(data, { lossy: true })
    data.fill(0x7a)
    free()
    expect(await settledIn(first)).toBe('resolved')
    expect(await page.readAll()).toEqual(['0:a1', '0:l2'])
  } finally {
    channel.abort()
    free()
  }
})
