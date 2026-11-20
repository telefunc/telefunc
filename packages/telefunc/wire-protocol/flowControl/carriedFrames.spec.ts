import { describe, expect, it } from 'vitest'

import { addWireCopies, dropWireCopies } from '../channel/server/channelMemory.js'
import { encode, isOneFrame, messageFrames } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { PieceRate } from './PieceRate.js'
import { PieceSender } from './pieces.js'
import { ServerHeadroom } from './server/ServerHeadroom.js'

// A data frame that waits for headroom after the carried frames ahead of it were counted, then a second carried frame
// and a frame that goes: the message holding them counts each frame once, and nothing throws.

const FULL = 2 ** 40

/** How many of the frames the messages `sent` hold have `tag`. */
const countOf = (sent: Uint8Array<ArrayBuffer>[], tag: number) =>
  sent
    .flatMap((message) => (isOneFrame(message) ? [message] : Array.from(messageFrames(message))))
    .filter((frame) => frame[0] === tag).length

const bytesOf = (sent: Uint8Array<ArrayBuffer>[]) => sent.reduce((sum, message) => sum + message.byteLength, 0)

describe('a carried frame behind a data frame that waits, after the carried frames ahead of it were counted', () => {
  it.each(['a small control frame', 'a small data frame'] as const)(
    'on a socket that copies with no copy room left, %s next goes with both carried frames, each counted once',
    (next) => {
      const sent: Uint8Array<ArrayBuffer>[] = []
      const sender = new PieceSender({
        socket: { send: (message) => void sent.push(message), bufferedAmount: 0, copiesOnSend: true },
        pingInterval: 5_000,
        pieceRate: new PieceRate(),
        cuts: 'whole',
        owner: { handed: () => {}, copyRoom: () => 0 },
      })
      sender.carry(() => encode.window(1, 1_000_000, 1))
      sender.send(encode.binary(1, new Uint8Array(100_000), 1), 1_000_000)
      expect(sender.dataWaits).toBe(true)
      sender.carry(() => encode.window(1, 2_000_000, 2))
      sender.send(next === 'a small control frame' ? encode.ping() : encode.text(1, 'hi', 2), 1_000_000)
      sender.flush()
      expect(countOf(sent, TAG.WINDOW)).toBe(2)
      expect(sender.transmittedBytes).toBe(bytesOf(sent))
    },
  )

  it('on a page that cuts data to its headroom, once a rate is measured, its next control frame goes with both carried acknowledgements, each counted once', () => {
    const sent: Uint8Array<ArrayBuffer>[] = []
    const pieceRate = new PieceRate()
    pieceRate.rise(1_000, performance.now(), true)
    const sender = new PieceSender({
      socket: { send: (message) => void sent.push(message), bufferedAmount: 0 },
      pingInterval: 5_000,
      pieceRate,
      cuts: 'cut',
      owner: { handed: () => {} },
    })
    sender.carry(() => encode.msgWindow(1, 100))
    // Less headroom than a piece the receiver acknowledges at once: the frame waits.
    sender.send(encode.binary(1, new Uint8Array(100_000), 1), 1_000)
    expect(sender.dataWaits).toBe(true)
    sender.carry(() => encode.msgWindow(1, 200))
    sender.send(encode.ping(), 0)
    sender.flush()
    expect(countOf(sent, TAG.MSG_WINDOW)).toBe(2)
    expect(sender.transmittedBytes).toBe(bytesOf(sent))
  })

  it.each([
    { socket: 'copies on send', copiesOnSend: true, bytes: 100_000 },
    { socket: "doesn't copy, as Node's", copiesOnSend: false, bytes: 1_000_000 },
  ])(
    'a server wire whose socket $socket, with no copy room left, sends a small message and its PING after a $bytes-byte message waits, with nothing thrown in its task',
    async ({ copiesOnSend, bytes }) => {
      const sent: Uint8Array<ArrayBuffer>[] = []
      const headroom = new ServerHeadroom({
        socket: { send: (message) => void sent.push(message), bufferedAmount: 0, copiesOnSend },
        pingInterval: 5_000,
      })
      addWireCopies(FULL)
      const errors: unknown[] = []
      const onError = (err: unknown) => void errors.push(err)
      process.on('uncaughtException', onError)
      try {
        // What the page hasn't said it took: nearly the wire's share of copies.
        for (let seq = 1; seq <= 4; seq++) {
          headroom.send(encode.binary(1, new Uint8Array(15_000), seq), performance.now())
        }
        await new Promise((resolve) => setTimeout(resolve, 0))
        headroom.sender.carry(() => encode.window(1, 1_000_000, 1))
        headroom.send(encode.binary(1, new Uint8Array(bytes), 5), performance.now())
        expect(headroom.sender.dataWaits).toBe(true)
        headroom.sender.carry(() => encode.window(1, 2_000_000, 2))
        headroom.send(encode.text(1, 'hi', 6), performance.now())
        // The wire's next PING, which goes ahead of waiting data.
        headroom.send(encode.ping(), performance.now())
        // The task that hands what it gathered.
        await new Promise((resolve) => setTimeout(resolve, 0))
      } finally {
        process.off('uncaughtException', onError)
        dropWireCopies(FULL)
        headroom.close()
      }
      expect(errors).toEqual([])
      expect(countOf(sent, TAG.WINDOW)).toBe(2)
      expect(headroom.sender.transmittedBytes).toBe(bytesOf(sent))
    },
  )
})
