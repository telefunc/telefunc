export { pairPublishes }

import { TAG } from '../../wire/tags.js'
import { assert } from '../../../utils/assert.js'
import type { DecodedFrame } from '../../wire/frame.js'

/** For one wire: gives each publish's body the channel and seq of the head ahead of it, and returns null for the head.
 *  A head whose body a dead wire lost goes with that wire, and the replay sends both again. */
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
