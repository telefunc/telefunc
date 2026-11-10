import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { WIRE_PIECES_ACK_BYTES } from '../../constants.js'
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
})
