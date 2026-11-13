export { pairPublishes }

import { TAG } from '../../wire/tags.js'
import { assert } from '../../../utils/assert.js'
import type { DecodedFrame } from '../../wire/frame.js'

/** The server encodes a publish's body once and sends that same body to every subscriber, each behind a small
 *  `PUBLISH_TO` frame naming that subscriber's channel and seq. One per wire, this puts that channel and seq onto the
 *  body that follows and returns null for the `PUBLISH_TO`, so the rest of the client sees an ordinary publish. A
 *  channel's lossy send comes behind one too, with its own index and seq, and is marked `lossy`. If the wire dies
 *  between the two frames, the `PUBLISH_TO` is dropped with it; the channel's seq hasn't moved, so the reconnect's
 *  replay sends both again. */
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
    } else if (
      head !== null &&
      (frame.tag === TAG.TEXT || frame.tag === TAG.BINARY) &&
      frame.index === head.index &&
      frame.seq === head.seq
    ) {
      frame.lossy = true
      head = null
    }
    return frame
  }
}
