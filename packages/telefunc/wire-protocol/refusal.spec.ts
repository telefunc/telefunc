// The end a wire carries, where the server decided it: the refusal a request's answer carries, as a WebSocket's close
// reason and the event stream's last event carry it, and as the page reads it there.

import { expect, test } from 'vitest'
import { END_CAUSE, type RefusalCause } from './channel/endCause.js'
import { endOfWire } from './channel/client/readAnswer.js'
import { frameRefusal, wireEnd } from './refusal.js'

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
