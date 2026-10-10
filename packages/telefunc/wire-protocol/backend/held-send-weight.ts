export { heldSendWeight }

import { ChannelOverflowError } from '../channel-errors.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import type { HoldWeight } from './subscription-manager.js'

const HELD_SEND_LIMIT = 1024

/** A send held for an establishing subscription is bounded like a channel's buffered sends: `HELD_SEND_LIMIT` of them,
 *  and `bufferLimit` bytes, `bufferLimitBinary` for binary, read only for a held send, as resolving the config is
 *  costly. One past either is refused with `ChannelOverflowError`. */
function heldSendWeight(kind: 'text' | 'binary', bytes: () => number, what: string): HoldWeight {
  return {
    class: kind,
    bytes,
    fits: (sends, held) => {
      const { channel } = getServerConfig()
      return sends <= HELD_SEND_LIMIT && held <= (kind === 'binary' ? channel.bufferLimitBinary : channel.bufferLimit)
    },
    overflow: () => new ChannelOverflowError(`${what} readiness buffer overflow`),
  }
}
