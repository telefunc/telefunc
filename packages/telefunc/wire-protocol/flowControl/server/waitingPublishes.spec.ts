import { expect, it } from 'vitest'
import { overtakes } from '../../channel/server/EventStream.js'
import { droppingPublishes } from './waitingPublishes.js'
import { ERROR_REASON, encode } from '../../wire/frame.js'

// Channel 1's publish waits on the wire: its head, then its body.
const waiting = [encode.publishTo(1, 5), encode.publish('"a"', { seq: 1, timestamp: 0 })]

it('every other frame of a channel waits behind its publishes, one that ends or closes it too', () => {
  expect(overtakes(encode.close(1, 5000, 6), waiting)).toBe(false)
  expect(overtakes(encode.error(1, ERROR_REASON.OVERFLOW, 6), waiting)).toBe(false)
  expect(overtakes(encode.abort(1, '"x"', 6), waiting)).toBe(false)
  expect(overtakes(encode.closeAck(1, 5, 6), waiting)).toBe(false)
})

it("a close request passes another channel's publishes", () => {
  expect(overtakes(encode.close(2, 5000, 1), waiting)).toBe(true)
})

it("drops channel 1's publishes at the seqs given, each head with its body, and nothing else", () => {
  const queue = [
    encode.publishTo(1, 5),
    encode.publish('"a"', { seq: 1, timestamp: 0 }),
    encode.publishTo(2, 6),
    encode.publish('"b"', { seq: 2, timestamp: 0 }),
    encode.publishTo(1, 7),
    encode.publish('"c"', { seq: 3, timestamp: 0 }),
  ]
  const drops = droppingPublishes(1, [7])
  expect(queue.map((frame) => drops(frame))).toEqual([false, false, false, false, true, true])
})
