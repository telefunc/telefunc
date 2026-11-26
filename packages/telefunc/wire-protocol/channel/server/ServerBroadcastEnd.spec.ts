import { ATTACHABLE_NOW } from './ServerChannel.js'
import { afterEach, describe, expect, it } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { IndexedPeer } from './IndexedPeer.js'
import type { ServerReplay } from './ServerReplay.js'
import { publishBody } from './PublishBody.js'
import { disposeBackend } from '../../backend/install.js'
import { decode } from '../../wire/frame.js'
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
      queue: {
        waitingPublishes: () => Number.POSITIVE_INFINITY,
        dropWaitingPublishes() {},
        dropUngiven: () => 0,
        dropWaitingFrom() {},
      },
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
function pageEnd(behindAfter: number, waiting: number) {
  const end = new ServerBroadcastEnd()
  end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
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
      queue: {
        waitingPublishes: () => waiting,
        dropWaitingPublishes() {},
        dropUngiven: () => 0,
        dropWaitingFrom() {},
      },
    },
    index: 7,
    replay: replayOf(end),
  })
  end._attachPeer(peer)
  return { end, peer }
}

/** The ids of the publishes a reconnect of `end`'s page would get, in order. */
function replayedIds(end: ServerBroadcastEnd): number[] {
  return replayOf(end)
    .getAfter(0)
    .map((frame) => decode(frame))
    .flatMap((decoded) => (decoded.tag === TAG.PUBLISH ? [Number(decoded.text)] : []))
}

function replayOf(end: ServerBroadcastEnd): ServerReplay {
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
