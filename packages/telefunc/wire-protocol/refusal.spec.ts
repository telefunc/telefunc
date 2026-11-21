// The end a wire carries, where the server decided it, as a WebSocket's close reason and the event stream's last event
// carry it, and as the page reads it there; a request's answer carries none.

import { expect, test } from 'vitest'
import { END_CAUSE, type RefusalCause } from './channel/endCause.js'
import { endOfWire, readAnswer } from './channel/client/readAnswer.js'
import { REFUSAL_MEDIA_TYPE, frameRefusal, refusalContentType, wireEnd } from './refusal.js'

/** Every end the server writes: a frame's cause naming its channel, the least and the greatest index a RECONCILE may
 *  give (wire/payloads.ts, a u16), and a session's cause naming none. */
const rows: [RefusalCause, number | undefined][] = [
  ['frameOverLimit', 0],
  ['frameOverLimit', 0xffff],
  ['frameRefused', 0],
  ['frameRefused', 0xffff],
  ['unreadable', undefined],
  ['receiveBudget', undefined],
]

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

/** What the server never writes, after `kind=frames; `: a scope its cause isn't written with, a cause no refusal names,
 *  a channel empty, valueless or named twice, a quote left open (the review of b06824dee, R2.1). */
const unwritten = [
  'cause=6',
  'channel=3; cause=8',
  'cause=1',
  'channel=; cause=8',
  'channel; cause=8',
  'channel=3; cause="6',
  'channel=3; channel=4; cause=6',
]

test.each(unwritten)("a last word the server's writer never writes (`%s`) is no end the page reads", (suffix) => {
  expect(endOfWire(`${media}; kind=frames; ${suffix}`)).toBeUndefined()
})

const answerOf = (contentType: string) =>
  readAnswer(new Response('refused', { status: 400, headers: { 'Content-Type': contentType } }), 'ok')

test.each([
  ...rows.map(([cause, channel]) => refusalContentType(frameRefusal(cause, channel))),
  ...unwritten.map((suffix) => `${media}; kind=frames; ${suffix}; reason="${END_CAUSE.frameRefused.reason}"`),
])(
  "an answer of the kind `frames` (`%s`) ends no channel: only a wire's own end carries an end, and the page reads the answer as a refusal it doesn't know",
  (contentType) => {
    expect(answerOf(contentType).tag).toBe('route')
  },
)
