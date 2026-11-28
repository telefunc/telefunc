import { ATTACHABLE_NOW, ServerChannel } from './ServerChannel.js'
import { afterEach, describe, expect, it, onTestFinished, vi } from 'vitest'
import { config, getChannelConfig } from '../../../node/server/serverConfig.js'
import { getChannelMux } from './ChannelMux.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { ServerHeadroom } from '../../flowControl/server/ServerHeadroom.js'
import { decodeChannelFrame } from '../../wire/decode.fixture.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { hold, releaseHolder } from './channelMemory.js'
import { ChannelOverflowError } from '../channelErrors.js'
import { binaryData, binaryPayload } from '../broadcastPayload.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { IndexedPeer } from './IndexedPeer.js'
import { ServerReplay } from './ServerReplay.js'
import { PublishBody, publishBody } from './PublishBody.js'
import { disposeBackend } from '../../backend/install.js'
import { ACK_STATUS, decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'

// A file of its own: `config.channel` is set before the first channel exists.
const MAX_PENDING_BYTES = 2 * 1024 * 1024
config.channel = { maxPendingBytes: MAX_PENDING_BYTES, maxMemory: '512m' }

afterEach(() => disposeBackend())

// A lossy publish a plain one of its stream followed is never replaced, so it counts toward the bound as a plain one does:
// where the bound can't be kept, the page fell behind and its channel ends.
it('holds no more than maxPendingBytes for a page that is away, lossy publishes a plain one followed included', () => {
  const end = awayEnd()
  let seq = 0
  for (let group = 0; group < 120; group++) {
    for (let i = 0; i < 5; i++) {
      end.deliver(publishBody(new Uint8Array(32 * 1024), { seq: ++seq, timestamp: seq }, 'cursor', true))
    }
    end.deliver(publishBody('"plain"', { seq: ++seq, timestamp: seq }, 'cursor', false))
  }

  expect(end.heldBytes).toBeLessThanOrEqual(MAX_PENDING_BYTES)
  expect(end._phase).toBe('ended')
})

/** An end whose page attached once and is away. */
function awayEnd(): ServerBroadcastEnd {
  const end = new ServerBroadcastEnd()
  end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
  end._registerChannel(ATTACHABLE_NOW)
  const peer = new IndexedPeer({
    sender: {
      send() {},
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => -1,
      deliveredBefore: () => 0,
      terminate() {},
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      sendPublish(index, seq, body) {
        this.send(encode.publishTo(index, seq))
        this.send(body)
      },
      waitingPublishes: () => Number.POSITIVE_INFINITY,
      dropWaitingPublishes() {},
      dropUngiven: () => 0,
      dropWaitingFrom() {},
    },
    index: 7,
    replay: replayOf(end),
  })
  end._attachPeer(peer)
  end._onPeerDisconnect(peer, 60_000)
  return end
}

// Random mixed publishes of two lossy streams and of none, against the rule written out: a page gets each plain publish,
// and a lossy one unless a newer lossy one of its stream came, with no plain one of the stream between, while the page
// was behind or away. A page that keeps up hands each publish to its wire at once, where it waits for the page's
// confirmation.
const PAGES = {
  'keeps up': { behindAfter: Number.POSITIVE_INFINITY, waiting: 0, goesAway: false },
  'keeps up, then goes away': { behindAfter: Number.POSITIVE_INFINITY, waiting: 0, goesAway: true },
  'is behind': { behindAfter: -1, waiting: Number.POSITIVE_INFINITY, goesAway: false },
  'is behind, then goes away': { behindAfter: -1, waiting: Number.POSITIVE_INFINITY, goesAway: true },
  'is away': { behindAfter: -1, waiting: Number.POSITIVE_INFINITY, goesAway: false },
}
const STREAMS = [null, 'a', 'b'] as const

describe.each(Object.entries(PAGES))('to a page that %s', (page, { behindAfter, waiting, goesAway }) => {
  it.each(Array.from({ length: 300 }, (_, seed) => seed + 1))('delivers what the rule says, seed %i', (seed) => {
    const random = mulberry32(seed)
    const publishes = Array.from({ length: 1 + Math.floor(random() * 30) }, (_, i) => {
      const stream = STREAMS[Math.floor(random() * STREAMS.length)] ?? null
      return { id: i + 1, stream, lossy: stream !== null && random() < 0.6 }
    })
    const { end, peer } = pageEnd(behindAfter, waiting)
    const awayAt = page === 'is away' ? 0 : goesAway ? Math.floor(random() * publishes.length) : publishes.length
    const replacingAt = behindAfter < 0 ? 0 : awayAt
    for (const [i, { id, stream, lossy }] of publishes.entries()) {
      if (i === awayAt) end._onPeerDisconnect(peer, 60_000)
      end.deliver(publishBody(String(id), { seq: id, timestamp: id }, stream, lossy))
    }
    void end.close()

    // Whether a newer lossy publish of `stream` than the one at `i` replaced it.
    const replaced = (stream: string | null, i: number) => {
      for (const [j, later] of publishes.entries()) {
        if (j <= i || later.stream !== stream) continue
        if (!later.lossy) return false
        if (j >= replacingAt) return true
      }
      return false
    }
    const owed = publishes.filter(({ stream, lossy }, i) => !lossy || !replaced(stream, i)).map(({ id }) => id)
    const read = replayedIds(end)
    const streamOf = new Map(publishes.map(({ id, stream }) => [id, stream]))
    // Within a stream in the order they were delivered; across streams a held lossy one may go after a later plain one.
    for (const stream of STREAMS) {
      const ofStream = (ids: number[]) => ids.filter((id) => streamOf.get(id) === stream)
      expect(ofStream(read)).toEqual(ofStream(owed))
    }
    const plain = publishes.filter(({ lossy }) => !lossy).map(({ id }) => id)
    expect(read.filter((id) => plain.includes(id))).toEqual(plain)
    end._release()
  })
})

/** An attached page's end whose page is behind once a publish waits longer than `behindAfter` for its confirmation, with
 *  `waiting` of its newest publishes not yet taken by its wire. */
function pageEnd(behindAfter: number, waiting: number, broadcast?: ServerBroadcast<string>) {
  const end = broadcast?._end ?? new ServerBroadcastEnd()
  if (!broadcast) end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
  const peer = attachPage(end, behindAfter, waiting)
  return { end, peer }
}

function attachPage(end: ServerChannel, behindAfter: number, waiting = 0): IndexedPeer {
  end._registerChannel(ATTACHABLE_NOW)
  const peer = new IndexedPeer({
    sender: {
      send() {},
      carry() {},
      bufferedAmount: () => 0,
      behindAfter: () => behindAfter,
      deliveredBefore: () => 0,
      terminate() {},
      sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
      sendPublish(index, seq, body) {
        this.send(encode.publishTo(index, seq))
        this.send(body)
      },
      waitingPublishes: () => waiting,
      dropWaitingPublishes() {},
      dropUngiven: () => 0,
      dropWaitingFrom() {},
    },
    index: 7,
    replay: replayOf(end),
  })
  end._attachPeer(peer, { want: { version: 1, text: true, binary: true } })
  return peer
}

/** The ids of the publishes a reconnect of `end`'s page would get, in order. */
function replayedIds(end: ServerBroadcastEnd): number[] {
  return replayOf(end)
    .getAfter(0)
    .map((frame) => decode(frame))
    .flatMap((decoded) => (decoded.tag === TAG.PUBLISH ? [Number(decoded.text)] : []))
}

function replayOf(end: ServerChannel): ServerReplay {
  const replay = end._replayBuffer
  if (replay === null) throw new Error('The end has no replay')
  return replay
}

/** A seeded random number in [0, 1). */
function mulberry32(seed: number): () => number {
  let state = seed
  return () => {
    state = (state + 0x6d2b79f5) | 0
    let t = Math.imul(state ^ (state >>> 15), 1 | state)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

it('a call without a session token applies no want to any channel, though the broadcast came from a call without one', () => {
  const mux = getChannelMux()
  const end = new ServerBroadcastEnd()
  let wants = 0
  end.setOwner({
    _onPagePublish() {},
    _onPagePublishBinary() {},
    _onPageWantParts() {
      wants++
    },
  })
  const call = new Request('https://example.test/_telefunc', { method: 'POST' })
  end._carriedBy = call
  mux.registerChannel(end, ATTACHABLE_NOW)
  expect(
    mux.applyCallWants(call, [{ channel: end.id, version: 1, text: false, binary: false, parts: {} }]),
  ).toBeUndefined()
  expect(wants).toBe(0)
  end.abort()
})

it('partial confirmation and dropping a stale lossy publish keep the newest route and timing', () => {
  vi.useFakeTimers({ toFake: ['performance'] })
  const holder = { heldBytes: 0 }
  const replay = new ServerReplay(holder)
  const bodies = [1, 3, 4, 5, 6].map(
    (seq) => new PublishBody(encode.publishBinary(Uint8Array.of(seq), { seq, timestamp: 1 }), 'cursor', true),
  )
  const push = (index: number, body: PublishBody | undefined) => {
    if (body === undefined) throw new Error('missing test publish')
    replay.pushPublish(replay.nextSeq(), index, body)
  }
  try {
    push(7, bodies[0])
    vi.advanceTimersByTime(50)
    replay.acknowledge(1)
    expect(replay.leastConfirmMs).toBe(50)

    const textSeq = replay.nextSeq()
    replay.push(textSeq, encode.text(7, '"plain"', textSeq))
    push(300, bodies[1])
    vi.advanceTimersByTime(1)
    push(301, bodies[2])
    vi.advanceTimersByTime(1)
    push(302, bodies[3])
    vi.advanceTimersByTime(1)
    push(303, bodies[4])
    vi.advanceTimersByTime(53)

    replay.acknowledge(4, 3)
    // The extra 24-byte publish and 14-byte text frame took 2 ms beyond the baseline after subtracting the hold.
    expect(replay.confirmRate).toBe(19)
    // The 24-byte baseline publish took about 1.26 ms at that rate.
    expect(replay.leastConfirmMs).toBeCloseTo(48.74)
    replay.dropStaleLossy(2)
    const [route, body] = replay.getAfter(4)
    if (route === undefined) throw new Error('missing route')
    expect(decode(route)).toMatchObject({ tag: TAG.PUBLISH_TO, index: 303, seq: 6 })
    expect(body).toBe(bodies[4]?.frame)
    expect(bodies.map((publish) => publish.refs)).toEqual([0, 0, 0, 0, 1])
    expect(replay.unconfirmedSince).toBe(53)

    replay.acknowledge(6)
    expect(replay.isEmpty).toBe(true)
    expect(holder.heldBytes).toBe(0)
  } finally {
    replay.dispose()
    vi.useRealTimers()
  }
})

const ledger = getGlobalObject('channelMemory.ts', { total: 0, maxMemory: 0, reserved: 0 })

function useSmallMemory(reserved = 0) {
  const { maxMemory: previousMax, reserved: previousReserved } = ledger
  ledger.maxMemory = 2 * 1024 * 1024
  ledger.reserved = reserved
  vi.useFakeTimers({ toFake: ['performance'] })
  onTestFinished(() => {
    ledger.maxMemory = previousMax
    ledger.reserved = previousReserved
    vi.useRealTimers()
  })
}

it('sends a lossy publish that fits while a small reliable publish is unconfirmed', async () => {
  useSmallMemory()
  const key = crypto.randomUUID()
  const broadcast = new ServerBroadcast<string>({ key })
  const { end } = pageEnd(600, 0, broadcast)
  try {
    await Broadcast.publishBinary(key, new Uint8Array(10))
    await Broadcast.publishBinary(key, new Uint8Array(1152 * 1024), { lossy: true })
    const sizes = replayOf(end)
      .getAfter(0)
      .map(decode)
      .flatMap((frame) => (frame.tag === TAG.PUBLISH_BINARY ? [binaryData(frame.data).byteLength] : []))
    expect(sizes).toEqual([10, 1152 * 1024])
  } finally {
    end.abort()
    end._release()
  }
})

it.each(['measured', 'unmeasured'] as const)(
  'with a %s link on a 2 MiB server, a page stays open and gets the latest lossy publish',
  async (link) => {
    useSmallMemory(link === 'measured' ? 512 * 1024 : 0)
    const key = crypto.randomUUID()
    const broadcast = new ServerBroadcast<string>({ key })
    const { end } = pageEnd(600, 0, broadcast)
    const replay = replayOf(end)
    const confirm = () => end._dispatchFrame(decodeChannelFrame(encode.window(7, 0, replay.seq)))
    const frames = () => replay.getAfter(0).map(decode)
    const delivered = () =>
      frames().flatMap((frame) => (frame.tag === TAG.PUBLISH_BINARY ? [binaryData(frame.data)[0]] : []))
    try {
      if (link === 'measured') {
        await Broadcast.publishBinary(key, new Uint8Array(16 * 1024), { lossy: true })
        vi.advanceTimersByTime(2)
        confirm()
        await Broadcast.publishBinary(key, new Uint8Array(256 * 1024), { lossy: true })
        await Broadcast.publishBinary(key, new Uint8Array(256 * 1024), { lossy: true })
        vi.advanceTimersByTime(4)
        confirm()
        expect(replay.confirmRate).toBeGreaterThan(100_000)
      }

      const payload = new Uint8Array(256 * 1024)
      for (let id = 1; id <= 20; id++) {
        payload[0] = id
        if (link === 'measured') await Broadcast.publishBinary(key, payload, { lossy: true })
        else await end._onPeerPublishBinaryAckReqMessage(binaryPayload(payload, true), id)
      }
      expect(end._phase).toBe('open')
      expect(delivered()).not.toContain(20)
      if (link === 'unmeasured') {
        expect(replay.confirmRate).toBe(0)
        const receipts = frames().filter((frame) => frame.tag === TAG.ACK_RES && frame.status === ACK_STATUS.OK)
        expect(receipts).toHaveLength(20)
      }

      vi.advanceTimersByTime(4)
      confirm()
      expect(delivered().at(-1)).toBe(20)
      if (link === 'measured') expect(delivered()).toEqual([20])
      expect(end._phase).toBe('open')
    } finally {
      if (end._phase === 'open') end.abort()
      end._release()
    }
  },
)

it('a 1:1 channel with wire credit can replace a lossy send held for the 2 MiB server budget', async () => {
  useSmallMemory()
  const end = new ServerChannel()
  const peer = attachPage(end, 600)
  const replay = replayOf(end)
  const confirm = () => end._dispatchFrame(decodeChannelFrame(encode.window(peer.index, 16 * 1024 * 1024, replay.seq)))
  try {
    confirm()
    await end.sendBinary(new Uint8Array(16 * 1024), { lossy: true })
    vi.advanceTimersByTime(2)
    confirm()
    await end.sendBinary(new Uint8Array(256 * 1024), { lossy: true })
    await end.sendBinary(new Uint8Array(256 * 1024), { lossy: true })
    vi.advanceTimersByTime(4)
    confirm()
    expect(replay.confirmRate).toBeGreaterThan(100_000)

    const payload = new Uint8Array(256 * 1024)
    for (let id = 1; id <= 20; id++) {
      payload[0] = id
      await end.sendBinary(payload, { lossy: true })
    }
    payload[0] = 0
    vi.advanceTimersByTime(4)
    confirm()
    const delivered = replay
      .getAfter(0)
      .map(decode)
      .flatMap((frame) => (frame.tag === TAG.BINARY ? [frame.data[0]] : []))
    expect(delivered).toEqual([20])
    expect(end._phase).toBe('open')
  } finally {
    if (end._phase === 'open') end.abort()
    end._release()
  }
})

for (const lossy of [false, true]) {
  it(`a broadcast whose ${lossy ? 'lossy' : 'plain'} publish doesn't fit under the budget closes, and the server holds no more than it`, async () => {
    const key = crypto.randomUUID()
    const broadcast = new ServerBroadcast<string>({ key })
    const { end } = pageEnd(Infinity, 2, broadcast)
    const filler = { heldBytes: 0 }
    const { maxMemory } = getChannelConfig()
    hold(filler, maxMemory - ledger.total - 300_000)
    try {
      expect(await Broadcast.publish(key, 'a'.repeat(400_000), { lossy })).toMatchObject({ key })
      expect(ledger.total).toBeLessThanOrEqual(maxMemory)
      expect(await broadcast.closed).toBeInstanceOf(ChannelOverflowError)
    } finally {
      releaseHolder(filler)
      end._release()
    }
  })
}

for (const kind of ['text', 'binary'] as const) {
  for (const publishes of ['plain', 'lossy'] as const) {
    it(`on a WebSocket, a ${publishes} ${kind} publish after a subscriber's close() near the budget costs it nothing it was owed`, async () => {
      const key = crypto.randomUUID()
      const broadcast = new ServerBroadcast<string>({ key })
      const end = broadcast._end
      end._registerChannel(ATTACHABLE_NOW)
      onTestFinished(() => {
        if (!end.isClosed) end.abort()
        end._release()
      })
      const headroom = new ServerHeadroom({ socket: { send() {}, bufferedAmount: 0 }, pingInterval: 5_000 })
      onTestFinished(() => headroom.close())
      const replay = replayOf(end)
      const peer = new IndexedPeer({
        sender: {
          send: (frame: Uint8Array<ArrayBuffer>) => headroom.send(frame),
          sendPublish(index, seq, body) {
            this.send(encode.publishTo(index, seq))
            this.send(body)
          },
          carry() {},
          behindAfter: () => Infinity,
          deliveredBefore: () => 0,
          bufferedAmount: () => 0,
          sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
          terminate() {},
          waitingPublishes: (index) => headroom.waitingPublishes(index),
          dropWaitingPublishes: (index, seqs) => headroom.dropWaitingPublishes(index, seqs),
          dropWaitingFrom: (index, mode) => headroom.dropWaitingFrom(index, mode),
          dropUngiven: (index) => headroom.dropUngiven(index),
        },
        index: 0,
        replay,
      })
      end._attachPeer(peer, { want: { version: 1, text: kind === 'text', binary: kind === 'binary' } })
      const lossy = publishes === 'lossy'
      const publish = (name: string, bytes: number) => {
        const value = name + 'x'.repeat(bytes)
        return kind === 'text'
          ? Broadcast.publish(key, value, { lossy })
          : Broadcast.publishBinary(key, new TextEncoder().encode(value), { lossy })
      }
      for (const name of ['p1', 'p2', 'p3']) await publish(name, 600_000)
      const closing = broadcast.close()
      const after = lossy ? 900_000 : 600_000
      const filler = { heldBytes: 0 }
      hold(filler, (getChannelConfig().maxMemory * 7) / 8 - ledger.total - after / 2)
      try {
        await publish('p4', after)
        const owed = replay.getAfter(0).map(decode)
        const delivered = owed.flatMap((frame) =>
          frame.tag === TAG.PUBLISH || frame.tag === TAG.PUBLISH_BINARY ? [frame.info.seq] : [],
        )
        expect(delivered).toEqual(lossy ? [1, 3] : [1, 2, 3])
        expect(owed.at(-1)?.tag).toBe(TAG.CLOSE)
        end._dispatchFrame(decodeChannelFrame(encode.closeAck(0, replay.seq, 1)))
        end._dispatchFrame(decodeChannelFrame(encode.closeDone(0, replay.seq, 2)))
        await expect(closing).resolves.toBeNull()
      } finally {
        releaseHolder(filler)
      }
    })
  }
}
