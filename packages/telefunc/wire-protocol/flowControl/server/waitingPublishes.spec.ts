import { expect, it } from 'vitest'
import { overtakes } from './waitingPublishes.js'
import { ERROR_REASON, encode } from '../../wire/frame.js'

// Channel 1's publish waits on the wire: its head, then its body.
const waiting = [encode.publishTo(1, 5), encode.publish('"a"', { seq: 1, timestamp: 0 })]

it("a close request and an error wait behind their channel's publishes", () => {
  expect(overtakes(encode.close(1, 5000, 6), waiting)).toBe(false)
  expect(overtakes(encode.error(1, ERROR_REASON.OVERFLOW, 6), waiting)).toBe(false)
})

it("an abort and the answer to the page's close pass them", () => {
  expect(overtakes(encode.abort(1, '"x"', 6), waiting)).toBe(true)
  expect(overtakes(encode.closeAck(1, 6), waiting)).toBe(true)
})

it("a close request passes another channel's publishes", () => {
  expect(overtakes(encode.close(2, 5000, 1), waiting)).toBe(true)
})
