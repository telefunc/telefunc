// The end a wire carries, where the server decided it: the refusal a request's answer carries, as a WebSocket's close
// reason and the event stream's last event carry it, and as the page reads it there.

import { expect, test } from 'vitest'
import { END_CAUSE, type RefusalCause } from './channel/endCause.js'
import { endOfWire } from './channel/client/readAnswer.js'
import { REFUSAL_MEDIA_TYPE, frameRefusal, wireEnd } from './refusal.js'

/** Every cause a refusal names, and the greatest channel index a RECONCILE may give (wire/payloads.ts, a u16). */
const causes: RefusalCause[] = ['frameOverLimit', 'frameRefused', 'unreadable', 'receiveBudget']
const channels = [undefined, 0xffff]
const rows = causes.flatMap((cause) => channels.map((channel): [RefusalCause, number | undefined] => [cause, channel]))

test.each(rows)(
  'the end of %s, naming channel %s, fits a WebSocket close reason (123 bytes of UTF-8), and the page reads it whole',
  (cause, channel) => {
    const told = wireEnd(frameRefusal(cause, channel))
    const { code } = END_CAUSE[cause]
    expect(new TextEncoder().encode(told).byteLength).toBeLessThanOrEqual(123)
    expect(endOfWire(told)).toEqual(
      channel === undefined ? { sessionEnded: code } : { framesRefused: { channel, cause: code } },
    )
  },
)

const media = REFUSAL_MEDIA_TYPE
test.each([
  ['cut in its media type', media.slice(0, -3)],
  ['cut after its media type', media],
  ['cut in its kind', `${media}; kind=fra`],
  ['cut after its kind', `${media}; kind=frames`],
  ['cut in its cause', `${media}; kind=frames; channel=3; cause=`],
  ['of the kind `retry`', `${media}; kind=retry`],
  ['of the kind `route`', `${media}; kind=route; cause=8`],
  ['of the kind `reload`', `${media}; kind=reload; cause=8`],
  ['of a cause no end has', `${media}; kind=frames; cause=99`],
  ['naming the channel `x`', `${media}; kind=frames; channel=x; cause=6`],
  ['naming the channel `-1`', `${media}; kind=frames; channel=-1; cause=6`],
  ['naming the channel `01`', `${media}; kind=frames; channel=01; cause=6`],
  ['naming the channel 65536, past a u16', `${media}; kind=frames; channel=65536; cause=6`],
])('a last word %s is no end the page reads: the wire ends as a loss', (_case, told) => {
  expect(endOfWire(told)).toBeUndefined()
})
