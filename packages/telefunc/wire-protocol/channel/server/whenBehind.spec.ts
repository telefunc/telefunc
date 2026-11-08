import { expect, test } from 'vitest'
import { ChannelMux, type ServerTransport } from './ChannelMux.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { decode, encode, isOneFrame, messageFrames, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'

type Wire = { sent: DecodedFrame[]; sessionId?: string }

const transport: ServerTransport<Wire> = {
  getSessionId: (wire) => wire.sessionId,
  setSessionId: (wire, id) => (wire.sessionId = id),
  getConnId: () => null,
  wholeMessages: true,
  sendNow: (wire, message) =>
    wire.sent.push(...(isOneFrame(message) ? [message] : [...messageFrames(message)]).map(decode)),
  bufferedAmount: () => 0,
  terminateWire() {},
}
const broadcast = { text: true, binary: false }

/** A page publishes and leaves before the answer, then three broadcasts follow: its own publish (seq 1), the three
 *  (seqs 2 to 4) and the answer it is owed are to come. */
async function pageAwayBehindThreePublishes(whenBehind: 'missOldest' | 'close') {
  const mux = new ChannelMux()
  const channel = new ServerBroadcast<string>({ key: `when-behind:${whenBehind}`, whenBehind })
  mux.registerChannel(channel)
  const first: Wire = { sent: [] }
  mux.onWireOpen(first, transport)
  await mux.onWireMessage(
    first,
    encode.reconcile({ open: [{ id: channel.id, ix: 1, lastSeq: 0, initial: true, broadcast }] }),
  )
  const answered = mux.onWireMessage(first, encode.publishAckReq(1, '"from the page"', 1))
  mux.onWireClosed(first, { permanent: false })
  await answered
  for (const message of ['a', 'b', 'c']) await Broadcast.publish(channel.key, message)
  return {
    channel,
    /** The page comes back from `lastSeq` 0: the tag and seq of each frame it reads in order. */
    async reconnect() {
      const second: Wire = { sent: [] }
      mux.onWireOpen(second, transport)
      await mux.onWireMessage(
        second,
        encode.reconcile({ sessionId: first.sessionId, open: [{ id: channel.id, ix: 1, lastSeq: 0, broadcast }] }),
      )
      return second.sent.flatMap((frame) => ('seq' in frame && frame.seq > 0 ? [[frame.tag, frame.seq]] : []))
    },
  }
}

test("with whenBehind 'close', a shed that would leave a gap ends the channel with nothing past the gap", async () => {
  const { channel, reconnect } = await pageAwayBehindThreePublishes('close')
  channel.shed(1)
  expect(channel.isClosed).toBe(true)
  expect(channel.heldBytes).toBe(0)
  expect(await reconnect()).toEqual([[TAG.ERROR, 5]])
})

test("with whenBehind 'missOldest', the same shed lets the oldest go and keeps the newest", async () => {
  const { channel, reconnect } = await pageAwayBehindThreePublishes('missOldest')
  channel.shed(1)
  expect(channel.isClosed).toBe(false)
  expect(await reconnect()).toEqual([
    [TAG.PUBLISH_TO, 4],
    [TAG.ACK_RES, 5],
  ])
})
