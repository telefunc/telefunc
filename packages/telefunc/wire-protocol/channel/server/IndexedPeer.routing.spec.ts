import { expect, test, vi } from 'vitest'
import { decode, encode, withTook } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { Broadcast, ServerBroadcast } from './ServerBroadcast.js'
import { ATTACHABLE_NOW } from './ServerChannel.js'
import { openWebSocket } from './serverPage.fixture.js'

test('a WebSocket gathers several recipients’ routes without encoding separate heads', async () => {
  const mux = getChannelMux()
  const key = crypto.randomUUID()
  const indexes = [7, 300]
  const channels = indexes.map((ix) => {
    const channel = new ServerBroadcast({ key })
    mux.registerChannel(channel._end, ATTACHABLE_NOW)
    return { channel, ix }
  })
  const page = openWebSocket({ reads: true })
  try {
    await mux.onWireMessage(
      page.wire,
      encode.reconcile({
        sessionId: crypto.randomUUID(),
        attempt: 1,
        open: channels.map(({ channel, ix }) => ({
          id: channel.id,
          ix,
          lastSeq: 0,
          initial: true,
          want: { version: 1, text: false, binary: true },
        })),
      }),
    )

    const taken = page.frames.reduce((bytes, frame) => bytes + frame.byteLength, 0)
    await mux.onWireMessage(page.wire, withTook(encode.ping([]), taken))

    const start = page.frames.length
    const publishTo = vi.spyOn(encode, 'publishTo')
    try {
      const first = Broadcast.publishBinary(key, Uint8Array.of(12))
      const second = Broadcast.publishBinary(key, Uint8Array.of(34))
      await Promise.all([first, second])
      await Broadcast.publishBinary(key, Uint8Array.of(56))

      const decoded = page.frames.slice(start).flatMap<{ index: number; seq: number } | number[]>((raw) => {
        const frame = decode(raw)
        if (frame.tag === TAG.PUBLISH_TO) return [{ index: frame.index, seq: frame.seq }]
        if (frame.tag === TAG.PUBLISH_BINARY) return [Array.from(frame.data)]
        return []
      })
      expect(decoded).toEqual([
        { index: 7, seq: 1 },
        [12],
        { index: 300, seq: 1 },
        [12],
        { index: 7, seq: 2 },
        [34],
        { index: 300, seq: 2 },
        [34],
        { index: 7, seq: 3 },
        [56],
        { index: 300, seq: 3 },
        [56],
      ])
      expect(publishTo).toHaveBeenCalledTimes(0)
    } finally {
      publishTo.mockRestore()
    }
  } finally {
    page.close()
    for (const { channel } of channels) channel.abort()
  }
})
