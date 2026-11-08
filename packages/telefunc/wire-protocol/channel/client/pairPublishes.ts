export { pairPublishes }

import { TAG } from '../../wire/tags.js'
import { assert } from '../../../utils/assert.js'
import type { DecodedFrame } from '../../wire/frame.js'

/** The server encodes a publish's body once and sends that same body to every subscriber, each behind a small
 *  `PUBLISH_TO` frame naming that subscriber's channel and seq: a copy per subscriber cost 804 ms of CPU a second for
 *  100 subscribers at 8 MiB/s. One per wire, this puts that channel and seq onto the body that follows and returns null
 *  for the `PUBLISH_TO`, so the rest of the client sees an ordinary publish. If the wire dies between the two frames,
 *  the `PUBLISH_TO` is dropped with it; the channel's seq hasn't moved, so the reconnect's replay sends both again. */
function pairPublishes(): (frame: DecodedFrame) => DecodedFrame | null {
  let head: { index: number; seq: number } | null = null
  return (frame) => {
    if (frame.tag === TAG.PUBLISH_TO) {
      head = frame
      return null
    }
    if (frame.tag === TAG.PUBLISH || frame.tag === TAG.PUBLISH_BINARY) {
      assert(head)
      frame.index = head.index
      frame.seq = head.seq
      head = null
    }
    return frame
  }
}
