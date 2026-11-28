import { expect, onTestFinished, test, vi } from 'vitest'
import { config } from '../../../node/server/serverConfig.js'
import { SendCredit } from '../../flowControl/SendCredit.js'
import { decodeChannelFrame } from '../../wire/decode.fixture.js'
import { encode } from '../../wire/frame.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { publishBody } from './PublishBody.js'
import { ServerBroadcastEnd } from './ServerBroadcastEnd.js'
import { ATTACHABLE_NOW, ServerChannel } from './ServerChannel.js'

const MEMORY = 2 * 1024 * 1024
const PAYLOAD = 1152 * 1024
config.channel = { maxMemory: MEMORY }

test.each([true, false])('settles superseded sends when attaching with replay=%s', async (replay) => {
  const channel = new ServerChannel()
  const old = attachable(channel)
  channel._attachPeer(old.peer)
  await channel.sendBinary(new Uint8Array(10))
  await channel.sendBinary(new Uint8Array(PAYLOAD), { lossy: true })
  await channel.sendBinary(new Uint8Array(PAYLOAD).fill(2), { lossy: true })

  const [first] = old.publishes
  if (!first) throw new Error('missing first send')
  expect(old.publishes).toHaveLength(1)
  const next = attachable(channel)
  const refund = vi.spyOn(SendCredit.prototype, 'refund')
  try {
    channel._onPageAttach(1, replay)
    channel._attachPeer(next.peer)
    expect(old.publishes).toHaveLength(1)
    if (replay) {
      expect(refund.mock.calls).toEqual([[first.byteLength, 1]])
      expect(next.publishes).toHaveLength(1)
    } else {
      expect(next.publishes).toHaveLength(0)
      expect(next.buffer.supersededPublishBytes).toBe(first.byteLength)
      channel._dispatchFrame(decodeChannelFrame(encode.window(7, MEMORY, 2)))
      expect(next.buffer.supersededPublishBytes).toBe(0)
      expect(refund).not.toHaveBeenCalled()
    }
    const [latest] = next.publishes
    if (!latest) throw new Error('missing newest send')
    expect(latest.at(-1)).toBe(2)
  } finally {
    refund.mockRestore()
  }
})

test('two caught-up broadcast subscribers send a shared large lossy publish immediately', () => {
  const first = new ServerBroadcastEnd()
  const second = new ServerBroadcastEnd()
  const pages = [first, second].map((end) => {
    end.setOwner({ _onPagePublish() {}, _onPagePublishBinary() {} })
    const page = attachable(end)
    end._attachPeer(page.peer)
    return page
  })
  const reliable = publishBody(new Uint8Array(10), { seq: 1, timestamp: 1 }, 'video', false)
  for (const end of [first, second]) end.deliver(reliable)
  const lossy = publishBody(new Uint8Array(PAYLOAD), { seq: 2, timestamp: 2 }, 'video', true)
  for (const end of [first, second]) end.deliver(lossy)
  for (const { publishes } of pages) {
    expect(publishes).toHaveLength(2)
    expect(publishes[1]).toBe(lossy.frame)
  }
})

function attachable(channel: ServerChannel) {
  if (channel._replayBuffer === null) {
    channel._registerChannel(ATTACHABLE_NOW)
    onTestFinished(() => {
      channel.abort()
      channel._release()
    })
  }
  const buffer = channel._replayBuffer
  if (buffer === null) throw new Error('missing replay buffer')
  const publishes: Uint8Array[] = []
  const sender: PeerSender = {
    send() {},
    sendPublish(_index, _seq, body) {
      publishes.push(body)
    },
    carry() {},
    bufferedAmount: () => 0,
    behindAfter: () => Infinity,
    deliveredBefore: () => 0,
    terminate() {},
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    waitingPublishes: () => 0,
    dropWaitingPublishes() {},
    dropUngiven: () => 0,
    dropWaitingFrom() {},
  }
  return { buffer, peer: new IndexedPeer({ sender, index: 7, replay: buffer }), publishes }
}
