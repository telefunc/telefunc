import { afterEach, describe, expect, it } from 'vitest'

import { addWireCopies, dropWireCopies, hasCopyRoom } from '../channel/server/channelMemory.js'
import { WIRE_PIECES_ACK_GAP_MS, WIRE_SEND_AHEAD_MIN_BYTES } from '../constants.js'
import { decode, encode, isOneFrame, messageFrames, tookOf, type PiecesAck } from '../wire/frame.js'
import { TAG } from '../wire/tags.js'
import { ClientPieceReceiver } from './client/ClientPieceReceiver.js'
import { ServerHeadroom } from './server/ServerHeadroom.js'

const FULL = 2 ** 40
afterEach(() => dropWireCopies(FULL))

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** A server wire whose socket keeps a copy of each message, as Bun's, Deno's and a Durable Object's do, to a page that
 *  takes every message a task later and, as its credit does, sends a MSG_WINDOW every 25 messages it took. */
function wireToPage() {
  let taken = 0
  let acknowledgements = 0
  const receiver: ClientPieceReceiver = new ClientPieceReceiver({
    owner: {
      sendAck: (frame) => {
        acknowledgements++
        setTimeout(() => headroom.acknowledged(decode(frame) as PiecesAck))
      },
      // Nothing else goes before the page's next MSG_WINDOW: an acknowledgement waiting to ride along goes alone.
      carryAck: () => {},
    },
  })
  const tookText = () => {
    if (++taken % 25 > 0) return
    const frame = receiver.report(encode.msgWindow(1, 100 + taken))
    setTimeout(() => headroom.heartbeat(tookOf(frame)!, performance.now()))
  }
  const take = (message: Uint8Array<ArrayBuffer>) => {
    const at = performance.now()
    const frames = isOneFrame(message) ? [message] : Array.from(messageFrames(message))
    const own = frames.pop()!
    for (const each of frames) {
      receiver.arrived(each[0]!, each.byteLength, at, 'riding')
      if (each[0] === TAG.TEXT) tookText()
    }
    const frame = decode(own)
    if (frame.tag === TAG.ANNOUNCE) receiver.announce(frame.bytes, at, frame.floor)
    else if (frame.tag === TAG.PIECE) {
      if (receiver.add(frame.total, frame.piece, at)?.[0] === TAG.TEXT) tookText()
    } else {
      receiver.arrived(frame.tag, own.byteLength, at, 'alone')
      if (frame.tag === TAG.TEXT) tookText()
    }
  }
  const headroom: ServerHeadroom = new ServerHeadroom({
    socket: { send: (message) => setTimeout(() => take(message)), bufferedAmount: 0, copiesOnSend: true },
    pingInterval: 5_000,
  })
  let seq = 0
  const send = (count: number, bytes: number) => {
    for (let i = 0; i < count; i++) headroom.send(encode.text(1, 'x'.repeat(bytes), ++seq), performance.now())
  }
  return { send, taken: () => taken, acknowledgements: () => acknowledgements }
}

describe('ServerHeadroom.copyRoom', () => {
  it('a page reading small frames keeps its wire going once copies fill their budget, and sends nothing more', async () => {
    const wire = wireToPage()
    wire.send(20, 1000)
    await sleep(200)
    expect(wire.taken()).toBe(20)

    addWireCopies(FULL)
    expect(hasCopyRoom()).toBe(false)
    const acknowledged = wire.acknowledgements()
    // Four times what a wire may have in flight past what its page said it took.
    const frames = Math.ceil((4 * WIRE_SEND_AHEAD_MIN_BYTES) / 1000)
    wire.send(frames, 1000)
    // Well before the page's next PING, which comes a ping interval later.
    await sleep(10 * WIRE_PIECES_ACK_GAP_MS)
    expect(wire.taken()).toBe(20 + frames)
    expect(wire.acknowledgements()).toBe(acknowledged)
  })
})
