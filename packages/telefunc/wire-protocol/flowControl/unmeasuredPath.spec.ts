import { describe, expect, it } from 'vitest'

import { WIRE_QUEUE_DELAY_MS, WIRE_SEND_AHEAD_MIN_BYTES } from '../constants.js'
import { decode, encode, isOneFrame, messageFrames, type PiecesAck } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { ClientPieceReceiver } from './client/ClientPieceReceiver.js'
import { ServerHeadroom } from './server/ServerHeadroom.js'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** 125 kB/s, 1 Mbit/s. */
const LINK_BYTES_PER_MS = 125

/** A server wire to a page behind a slow link, through a socket that takes whatever it is handed and reports none of
 *  it buffered, as one whose kernel buffers hold it: the link carries what the socket took, in order, at its rate,
 *  `delayMs` each way. */
function wireBehindSlowLink({ bytesPerMs = LINK_BYTES_PER_MS, delayMs = 0 } = {}) {
  const link: Uint8Array<ArrayBuffer>[] = []
  let linkBytes = 0
  const receiver: ClientPieceReceiver = new ClientPieceReceiver({
    owner: {
      sendAck: (frame) => {
        setTimeout(() => headroom.acknowledged(decode(frame) as PiecesAck), Math.max(20, delayMs))
      },
      carryAck: () => {},
    },
  })
  const take = (message: Uint8Array<ArrayBuffer>) => {
    const at = performance.now()
    for (const frame of isOneFrame(message) ? [message] : Array.from(messageFrames(message))) {
      const decoded = decode(frame)
      if (decoded.tag === TAG.ANNOUNCE) receiver.announce(decoded.bytes, at, decoded.floor)
      else if (decoded.tag === TAG.PIECE) receiver.add(decoded.total, decoded.piece, at)
      else receiver.arrived(decoded.tag, frame.byteLength, at, 'alone')
    }
  }
  let budget = 0
  let last = performance.now()
  const timer = setInterval(() => {
    const now = performance.now()
    budget += (now - last) * bytesPerMs
    last = now
    while (link.length > 0 && link[0]!.byteLength <= budget) {
      const message = link.shift()!
      budget -= message.byteLength
      linkBytes -= message.byteLength
      if (delayMs === 0) take(message)
      else setTimeout(() => take(message), delayMs)
    }
    if (link.length === 0) budget = 0
  }, 5)
  const headroom: ServerHeadroom = new ServerHeadroom({
    socket: {
      send: (message) => {
        link.push(message)
        linkBytes += message.byteLength
      },
      bufferedAmount: 0,
      copiesOnSend: false,
    },
    pingInterval: 5_000,
  })
  let seq = 0
  return {
    send: (frame: (seq: number) => Uint8Array<ArrayBuffer>) => headroom.send(frame(++seq), performance.now()),
    linkBytes: () => linkBytes,
    sentBytes: () => headroom.sender.transmittedBytes,
    stop: () => clearInterval(timer),
  }
}

describe('ServerHeadroom.headroom', () => {
  it.each([
    ['64 KiB', 64 * 1024],
    ['16 KiB', 16 * 1024],
    ['1 KiB', 1024],
  ])(
    "a download in %s frames on a link not yet measured leaves another channel's answer little to wait behind",
    async (_, frameBytes) => {
      const wire = wireBehindSlowLink()
      try {
        // 2 MiB sent faster than the link takes it, in frames that go whole.
        const download = new Uint8Array(frameBytes)
        for (let sent = 0; sent < 2 * 1024 * 1024; sent += frameBytes)
          wire.send((seq) => encode.binary(1, download, seq))
        await sleep(1_000)
        const ahead = wire.linkBytes()
        wire.send((seq) => encode.text(2, '"pong"', seq))
        // What the link holds when the answer goes is what it waits behind: the least send-ahead and what the link
        // drains in the queue delay, not the download.
        expect(ahead).toBeLessThan(2 * WIRE_SEND_AHEAD_MIN_BYTES + LINK_BYTES_PER_MS * WIRE_QUEUE_DELAY_MS)
      } finally {
        wire.stop()
      }
    },
  )

  it('a stream on a link with a long round trip, not yet measured, keeps the link busy', async () => {
    // 50 kB/s, 450 ms each way, and 3300 B texts at 30 Hz: twice what the link carries.
    const wire = wireBehindSlowLink({ bytesPerMs: 50, delayMs: 450 })
    let seq = 0
    const body = 'x'.repeat(3300)
    const stream = setInterval(() => wire.send(() => encode.text(1, body, ++seq)), 1000 / 30)
    try {
      await sleep(3_000)
      // What the link carries in that time.
      expect(wire.sentBytes()).toBeGreaterThanOrEqual(50 * 3_000)
    } finally {
      clearInterval(stream)
      wire.stop()
    }
  })
})
