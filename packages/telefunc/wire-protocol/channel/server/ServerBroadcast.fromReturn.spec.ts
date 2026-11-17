// A page hears a broadcast its telefunction returned from the return: what the key gets before the page says what it
// wants is held for it, and its first want or attach keeps what it wants of that. The real ChannelMux, ServerBroadcast
// and in-memory Broadcast run; the page's wire is a sender that records frames.

import { expect, test } from 'vitest'

import { config } from '../../../node/server/serverConfig.js'
import { CREDIT_WINDOW_INITIAL_BYTES } from '../../constants.js'
import { getChannelMux } from './ChannelMux.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { IndexedPeer, type PeerSender } from './IndexedPeer.js'
import { TAG } from '../../wire/tags.js'
import type { Want } from '../../wire/payloads.js'

config.channel = { maxPendingBytes: CREDIT_WINDOW_INITIAL_BYTES, maxMemory: '32m' }

/** A broadcast a telefunction returned, its page not attached yet. */
function returnedBroadcast() {
  const key = crypto.randomUUID()
  const broadcast = new ServerBroadcast<string>({ key })
  getChannelMux().registerChannel(broadcast)
  const frames: Uint8Array[] = []
  const sender: PeerSender = {
    send: (frame) => frames.push(frame),
    carry() {},
    bufferedAmount: () => 0,
    behindAfter: () => Number.POSITIVE_INFINITY,
    deliveredBefore: () => 0,
    sendWait: () => ({ waitNow: () => 0, longestWaitSince: () => 0 }),
    queue: { waitingPublishes: () => 0, dropWaitingPublishes() {}, dropWaitingFrom() {} },
  }
  const count = (tag: number) => frames.filter((frame) => frame[0] === tag).length
  return {
    key,
    broadcast,
    /** The page attaches, wanting `want`. */
    attach(want?: Want) {
      const replay = broadcast._replayBuffer
      if (!replay) throw new Error('A registered broadcast has its replay')
      broadcast._attachPeer(new IndexedPeer({ sender, index: 0, replay }), { want })
    },
    publishes: () => ({ text: count(TAG.PUBLISH), binary: count(TAG.PUBLISH_BINARY) }),
  }
}

const textOnly: Want = { version: 1, text: true, binary: false }

test('what the key gets between the return and the attach reaches the page, of the kinds it wants', async () => {
  const page = returnedBroadcast()
  await Broadcast.publish(page.key, 'text')
  await Broadcast.publishBinary(page.key, new Uint8Array([1]))
  page.attach(textOnly)
  expect(page.publishes()).toEqual({ text: 1, binary: 0 })
  // From the attach on, only the kinds it wants.
  await Broadcast.publishBinary(page.key, new Uint8Array([2]))
  await Broadcast.publish(page.key, 'text again')
  expect(page.publishes()).toEqual({ text: 2, binary: 0 })
  page.broadcast.abort()
})

test('a page that attaches wanting nothing gets nothing, and stays open', async () => {
  const page = returnedBroadcast()
  await Broadcast.publish(page.key, 'text')
  page.attach()
  await Broadcast.publish(page.key, 'text again')
  expect(page.publishes()).toEqual({ text: 0, binary: 0 })
  expect(page.broadcast._phase).toBe('open')
  page.broadcast.abort()
})

test('past `maxPendingBytes` before its page says what it wants, a page that wants that kind ends as one that missed it, and one that does not stays open', async () => {
  const big = 'x'.repeat(CREDIT_WINDOW_INITIAL_BYTES / 3)
  const wantsText = returnedBroadcast()
  const wantsBinary = returnedBroadcast()
  for (const page of [wantsText, wantsBinary]) for (let i = 0; i < 4; i++) await Broadcast.publish(page.key, big)
  wantsText.attach(textOnly)
  wantsBinary.attach({ version: 1, text: false, binary: true })
  await Promise.resolve()
  expect(wantsText.broadcast._phase).toBe('ended')
  expect(wantsBinary.broadcast._phase).toBe('open')
  wantsBinary.broadcast.abort()
})
