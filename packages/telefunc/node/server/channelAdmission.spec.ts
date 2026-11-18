// What the server admits near `config.channel.maxMemory`: a send is decided on the bytes it really holds, so one that
// fits makes no other page's channel let go of anything. A filler holder takes the budget down to the room each test
// names; the real ServerChannel, ServerBroadcast and memory ledger run.

import { expect, test } from 'vitest'

import { ServerChannel } from '../../wire-protocol/channel/server/ServerChannel.js'
import { Broadcast, ServerBroadcast } from '../../wire-protocol/channel/server/ServerBroadcast.js'
import { IndexedPeer, type PeerSender } from '../../wire-protocol/channel/server/IndexedPeer.js'
import { hold, letGo, releaseHolder, type MemoryHolder } from '../../wire-protocol/channel/server/channelMemory.js'
import { ChannelOverflowError } from '../../wire-protocol/channel/channelErrors.js'
import { config, getChannelConfig } from './serverConfig.js'

config.channel = { maxMemory: '2m' }

/** What this server's channels hold, on the ledger every channel shares. */
const ledgerTotal = () => (globalThis as any)._telefunc['channelMemory.ts'].total as number

/** A page's wire that takes nothing off its queue: every publish waits there, so its broadcast may shed them. */
const stalledSender: PeerSender = {
  send() {},
  carry() {},
  bufferedAmount: () => 0,
  behindAfter: () => Number.POSITIVE_INFINITY,
  deliveredBefore: () => 0,
  sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
  queue: { waitingPublishes: () => 2, dropWaitingPublishes() {}, dropWaitingFrom() {} },
}

/** A broadcast of `key` whose page subscribed to its text and reads nothing. */
function subscribedPage(key: string, lossy: boolean) {
  const broadcast = new ServerBroadcast<string>({ key, lossy })
  broadcast._registerChannel()
  broadcast._attachPeer(new IndexedPeer({ sender: stalledSender, index: 2, replay: broadcast._replayBuffer! }), {
    want: { version: 1, text: true, binary: false },
  })
  return broadcast
}

test("a reliable broadcast near the budget stays open when another channel's text send fits at its real size", async () => {
  const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  broadcast._registerChannel()
  broadcast._attachPeer(new IndexedPeer({ sender: stalledSender, index: 2, replay: broadcast._replayBuffer! }), {
    want: { version: 1, text: true, binary: false },
  })
  const channel = new ServerChannel<never, string>()
  channel._registerChannel()
  const filler: MemoryHolder = { heldBytes: 0 }
  try {
    await broadcast.publish('a'.repeat(400_000))
    await broadcast.publish('b'.repeat(400_000))
    // Room below the shedding target for 500,000 ASCII characters, not for three bytes each.
    hold(filler, (getChannelConfig().maxMemory * 7) / 8 - ledgerTotal() - 600_000)
    // The page hasn't attached: the send waits in the channel's buffer, held at its real size.
    void channel.send('c'.repeat(500_000))
    expect(channel.heldBytes).toBeGreaterThan(500_000)
    expect(broadcast.isClosed).toBe(false)
  } finally {
    letGo(filler, filler.heldBytes)
    releaseHolder(filler)
    channel._end()
    channel._release()
    broadcast._end()
    broadcast._release()
  }
})

for (const lossy of [false, true]) {
  test(`a ${lossy ? 'lossy' : 'reliable'} broadcast whose publish doesn't fit under the budget closes, and the server holds no more than it`, async () => {
    const key = crypto.randomUUID()
    const broadcast = subscribedPage(key, lossy)
    let closedWith: Error | undefined
    broadcast.onClose((err) => {
      closedWith = err
    })
    // What one-to-one channels hold, which no publish can make them let go of, leaves room for less than the publish.
    const filler: MemoryHolder = { heldBytes: 0 }
    const { maxMemory } = getChannelConfig()
    try {
      hold(filler, maxMemory - ledgerTotal() - 300_000)
      expect(await Broadcast.publish(key, 'a'.repeat(400_000))).toMatchObject({ key })
      expect(ledgerTotal()).toBeLessThanOrEqual(maxMemory)
      expect(broadcast.isClosed).toBe(true)
      expect(closedWith).toBeInstanceOf(ChannelOverflowError)
    } finally {
      letGo(filler, filler.heldBytes)
      releaseHolder(filler)
      broadcast._end()
      broadcast._release()
    }
  })
}

test("a publish that can't fit under the budget ends its own pages only: another key's keep what they hold", async () => {
  const others = [subscribedPage(crypto.randomUUID(), false), subscribedPage(crypto.randomUUID(), true)]
  const page = subscribedPage(crypto.randomUUID(), false)
  try {
    for (const other of others) await Broadcast.publish(other.key, 'a'.repeat(100_000))
    const held = others.map((other) => other.heldBytes)
    expect(held.every((bytes) => bytes > 100_000)).toBe(true)
    await Broadcast.publish(page.key, 'b'.repeat(8 * 1024 * 1024))
    expect(page.isClosed).toBe(true)
    expect(others.map((other) => other.isClosed)).toEqual([false, false])
    expect(others.map((other) => other.heldBytes)).toEqual(held)
  } finally {
    for (const broadcast of [...others, page]) {
      broadcast._end()
      broadcast._release()
    }
  }
})

test("a send larger than a channel may ever hold is refused, and another page's broadcast near the budget keeps what it holds", async () => {
  const broadcast = subscribedPage(crypto.randomUUID(), false)
  const channel = new ServerChannel<never, string>()
  channel._registerChannel()
  const filler: MemoryHolder = { heldBytes: 0 }
  try {
    await broadcast.publish('a'.repeat(400_000))
    await broadcast.publish('b'.repeat(400_000))
    const held = broadcast.heldBytes
    // Just below the shedding target: a send of any size would take the ledger past it.
    hold(filler, (getChannelConfig().maxMemory * 7) / 8 - ledgerTotal() - 1_000)
    const sent = channel.send('c'.repeat(getChannelConfig().maxMemory + 1))
    await expect(sent).rejects.toBeInstanceOf(ChannelOverflowError)
    expect(broadcast.isClosed).toBe(false)
    expect(broadcast.heldBytes).toBe(held)
  } finally {
    letGo(filler, filler.heldBytes)
    releaseHolder(filler)
    channel._end()
    channel._release()
    broadcast._end()
    broadcast._release()
  }
})
