import { afterEach, beforeEach, expect, test, vi } from 'vitest'

import { hold, letGo, releaseHolder, type MemoryHolder } from './channelMemory.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { LossyDelivery } from './LossyDelivery.js'
import { PublishBody } from './PublishBody.js'
import { ServerReplay } from './ServerReplay.js'

beforeEach(() => vi.useFakeTimers({ toFake: ['performance'] }))
afterEach(() => vi.useRealTimers())

/** A lossy page on a wire that holds nothing back: what the delivery hands goes into `sent` and the replay. */
function lossyPage() {
  const holder: MemoryHolder = { heldBytes: 0 }
  const replay = new ServerReplay(holder)
  const sender: PeerSender = {
    queue: { waitingPublishes: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {}, dropUngiven: () => 0 },
    behindAfter: (roundTrip) => 600 + roundTrip,
    send() {},
    carry() {},
    bufferedAmount: () => 0,
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    deliveredBefore: () => 0,
    terminate() {},
  }
  const peer = new IndexedPeer({ sender, index: 0, replay })
  const sent: PublishBody[] = []
  const delivery = new LossyDelivery<PublishBody>({
    peer: () => peer,
    replay: () => replay,
    hand: (body) => {
      sent.push(body)
      peer.sendPublish(body)
    },
    hold: (body) => hold(holder, body),
    letGo: (body) => letGo(holder, body),
    dropUnhanded() {},
    outOfCredit: () => false,
    bytes: (body) => body.frame.byteLength,
  })
  return { replay, delivery, sent, release: () => releaseHolder(holder) }
}

test('a report measures when its page took the value, excluding time held at the page', () => {
  const { replay, delivery, release } = lossyPage()
  try {
    delivery.send('cursor', new PublishBody(new Uint8Array(4096), 'cursor', true))
    vi.advanceTimersByTime(1000)
    replay.acknowledge(1, 600)
    expect(replay.leastConfirmMs).toBe(400)
  } finally {
    release()
  }
})

test('a confirmation of all its page was handed, at a rate that takes far longer than a page that keeps up for what waits, lets only the newest of that go', () => {
  const { replay, delivery, sent, release } = lossyPage()
  try {
    // A value alone, confirmed in the round trip: 100 ms.
    delivery.send('cursor', new PublishBody(new Uint8Array(16), 'cursor', true))
    vi.advanceTimersByTime(100)
    replay.acknowledge(1)
    const bodies = Array.from({ length: 30 }, () => new PublishBody(new Uint8Array(16 * 1024), 'cursor', true))
    for (const body of bodies) delivery.send('cursor', body)
    // The one its page takes and 64 KiB ahead go; the 6th crosses that, and the other 24, 384 KiB, wait.
    expect(sent.length).toBe(7)
    // 96 KiB confirmed 1.9 s past the round trip, 52 B/ms: what waits takes it 7.6 s, past the 2.6 s one that keeps up
    // confirms in.
    vi.advanceTimersByTime(2_000)
    replay.acknowledge(7)
    delivery.mayGo()
    expect(sent.length).toBe(8)
    expect(sent.at(-1)).toBe(bodies.at(-1))
  } finally {
    release()
  }
})

test('on a 900 ms round trip that keeps up, a confirmation of what its page was handed lets all that waits go, in order', () => {
  const { replay, delivery, sent, release } = lossyPage()
  try {
    delivery.send('cursor', new PublishBody(new Uint8Array(16), 'cursor', true))
    vi.advanceTimersByTime(900)
    replay.acknowledge(1)
    const bodies = Array.from({ length: 30 }, () => new PublishBody(new Uint8Array(16 * 1024), 'cursor', true))
    for (const body of bodies) delivery.send('cursor', body)
    expect(sent.length).toBe(7)
    // 96 KiB confirmed 50 ms past the round trip: what waits takes it 200 ms.
    vi.advanceTimersByTime(950)
    replay.acknowledge(7)
    delivery.mayGo()
    expect(sent.slice(1)).toEqual(bodies)
  } finally {
    release()
  }
})

test("on a 1 MB/s link with a 100 ms round trip, a large publish that went alone and the next don't take the link for faster than it is: of ten more made at once, the newest goes once two are confirmed", () => {
  const { replay, delivery, sent, release } = lossyPage()
  try {
    const mib = () => new PublishBody(new Uint8Array(1024 * 1024), 'cursor', true)
    // Alone: the round trip and its own 1,049 ms.
    delivery.send('cursor', mib())
    vi.advanceTimersByTime(1_149)
    replay.acknowledge(1)
    // Behind a small frame its page hasn't confirmed, the same.
    replay.push(replay.nextSeq(), new Uint8Array(16))
    delivery.send('cursor', mib())
    vi.advanceTimersByTime(1_149)
    replay.acknowledge(3)
    const bodies = Array.from({ length: 10 }, mib)
    const handed = (from: number) => sent.slice(from).map((body) => bodies.indexOf(body))
    for (const body of bodies) delivery.send('cursor', body)
    // Before a rate, two go: a third would put past 64 KiB behind the one its page takes.
    expect(handed(2)).toEqual([0, 1])
    // Those two, confirmed after the round trip and 2,098 ms: the 8 MiB that wait take such a page 8.4 s.
    vi.advanceTimersByTime(2_198)
    replay.acknowledge(5)
    expect(replay.confirmRate).toBeCloseTo(1000, -1)
    expect(replay.leastConfirmMs).toBeCloseTo(100, 0)
    delivery.mayGo()
    expect(handed(2)).toEqual([0, 1, 9])
  } finally {
    release()
  }
})

test('on a 1 MB/s link, a 4 MiB publish behind a frame confirmed a little quicker than the lone one before it, then two confirmed together, measure the link', () => {
  const { replay, delivery, sent, release } = lossyPage()
  try {
    const p4 = () => new PublishBody(new Uint8Array(4 * 1024 * 1024), 'cursor', true)
    // Alone: a 15 ms round trip and its own 4,194 ms.
    delivery.send('cursor', p4())
    vi.advanceTimersByTime(4_209)
    replay.acknowledge(1)
    // Behind a 16 B frame, 9 ms quicker.
    replay.push(replay.nextSeq(), new Uint8Array(16))
    delivery.send('cursor', p4())
    vi.advanceTimersByTime(4_200)
    replay.acknowledge(3)
    const bodies = Array.from({ length: 10 }, p4)
    const handed = (from: number) => sent.slice(from).map((body) => bodies.indexOf(body))
    for (const body of bodies) delivery.send('cursor', body)
    expect(handed(2)).toEqual([0, 1])
    vi.advanceTimersByTime(15 + 2 * 4_194)
    replay.acknowledge(5)
    expect(replay.confirmRate).toBeCloseTo(1000, -1)
    delivery.mayGo()
    expect(handed(2)).toEqual([0, 1, 9])
  } finally {
    release()
  }
})

test('on a 900 ms round trip at 100 MB/s, a 1 MiB publish that went alone and two confirmed together let ten more made at once all go, in order', () => {
  const { replay, delivery, sent, release } = lossyPage()
  try {
    const mib = () => new PublishBody(new Uint8Array(1024 * 1024), 'cursor', true)
    // Alone: the round trip and its own 10.5 ms.
    delivery.send('cursor', mib())
    vi.advanceTimersByTime(910)
    replay.acknowledge(1)
    const bodies = Array.from({ length: 10 }, mib)
    const handed = (from: number) => sent.slice(from).map((body) => bodies.indexOf(body))
    for (const body of bodies) delivery.send('cursor', body)
    expect(handed(1)).toEqual([0, 1])
    vi.advanceTimersByTime(921)
    replay.acknowledge(3)
    // What waits takes such a page 84 ms.
    delivery.mayGo()
    expect(handed(1)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
  } finally {
    release()
  }
})
