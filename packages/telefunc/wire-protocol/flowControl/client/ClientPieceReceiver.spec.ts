import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { WIRE_PIECES_ACK_BYTES } from '../../constants.js'
import { decode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ClientPieceReceiver } from './ClientPieceReceiver.js'

describe('ClientPieceReceiver', () => {
  beforeEach(() => vi.useFakeTimers())
  afterEach(() => vi.useRealTimers())

  it('acknowledges a burst at once each `WIRE_PIECES_ACK_BYTES` it takes, not only a gap after it began', () => {
    const acks: Uint8Array[] = []
    const receiver = new ClientPieceReceiver({ owner: { sendAck: (frame) => acks.push(frame), carryAck: () => {} } })
    const frameBytes = 100
    for (let i = 1; i * frameBytes <= 2 * WIRE_PIECES_ACK_BYTES; i++) {
      receiver.arrived(TAG.TEXT, frameBytes, performance.now(), 'alone')
      expect(acks).toHaveLength(Math.floor((i * frameBytes) / WIRE_PIECES_ACK_BYTES))
    }
  })

  it('reports how long it held the newest byte it acknowledges, a small control frame its own', () => {
    // The server dates that byte by when it handed it: one a PONG ends, handed after the data, read as arrived with the
    // data, would make the path's round trip 0, and the server's lossy streams, once the path is their limit, stop.
    let carried: (() => Uint8Array | null) | null = null
    const receiver = new ClientPieceReceiver({ owner: { sendAck: () => {}, carryAck: (take) => (carried = take) } })
    receiver.arrived(TAG.TEXT, 100, performance.now(), 'alone')
    vi.advanceTimersByTime(30)
    receiver.arrived(TAG.PONG, 12, performance.now(), 'alone')
    vi.advanceTimersByTime(5)
    const ack = decode(carried!()! as Uint8Array<ArrayBuffer>)
    expect(ack).toMatchObject({ tag: TAG.PIECES_ACK, bytes: 112, heldMs: 5 })
  })
})
