// What the server admits near `config.channel.maxMemory`: a send is decided on the bytes it really holds, so one that
// fits makes no other page's channel let go of anything. A filler holder takes the budget down to the room each test
// names; the real ServerChannel, ServerBroadcast and memory ledger run.

import { expect, test } from 'vitest'

import { ServerChannel } from '../../wire-protocol/channel/server/ServerChannel.js'
import { ServerBroadcast } from '../../wire-protocol/channel/server/ServerBroadcast.js'
import { IndexedPeer, type PeerSender } from '../../wire-protocol/channel/server/IndexedPeer.js'
import { hold, letGo, releaseHolder, type MemoryHolder } from '../../wire-protocol/channel/server/channelMemory.js'
import { getChannelConfig } from './serverConfig.js'

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
  queue: { waitingPublishes: () => 2, dropWaitingPublishes() {}, dropWaitingFromPublish() {} },
}

test("a reliable broadcast near the budget stays open when another channel's text send fits at its real size", async () => {
  const broadcast = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  broadcast._registerChannel()
  broadcast._attachPeer(new IndexedPeer({ sender: stalledSender, index: 2, replay: broadcast._replayBuffer! }), {
    broadcast: { text: true, binary: false },
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
