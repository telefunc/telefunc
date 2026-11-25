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
    dropReplaceable() {},
    outOfCredit: () => false,
    bytes: (body) => body.frame.byteLength,
  })
  return { replay, delivery, sent, release: () => releaseHolder(holder) }
}

test('a confirmation of all its page was handed, at a rate that takes far longer than a page that keeps up for what waits, lets only the newest of that go', () => {
  const { replay, delivery, sent, release } = lossyPage()
  try {
    // A value alone, confirmed in the round trip: 100 ms.
    delivery.send(new PublishBody(new Uint8Array(16)))
    vi.advanceTimersByTime(100)
    replay.acknowledge(1)
    const bodies = Array.from({ length: 30 }, () => new PublishBody(new Uint8Array(16 * 1024)))
    for (const body of bodies) delivery.send(body)
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
    delivery.send(new PublishBody(new Uint8Array(16)))
    vi.advanceTimersByTime(900)
    replay.acknowledge(1)
    const bodies = Array.from({ length: 30 }, () => new PublishBody(new Uint8Array(16 * 1024)))
    for (const body of bodies) delivery.send(body)
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
