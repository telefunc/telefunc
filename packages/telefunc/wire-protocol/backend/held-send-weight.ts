export { heldSendWeight, HELD_SEND_OVERHEAD_BYTES }

import { ChannelOverflowError } from '../channel-errors.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import type { HoldWeight } from './subscription-manager.js'

/** What holding a send retains beyond its payload (130–160 bytes measured), so many small sends are bounded by memory
 *  too. */
const HELD_SEND_OVERHEAD_BYTES = 256

/** A send held for an establishing subscription is bounded like a channel's buffered sends: its payload and
 *  `HELD_SEND_OVERHEAD_BYTES` count against `bufferLimit`, `bufferLimitBinary` for binary, read only for a held send, as
 *  resolving the config is costly. One past it is refused with `ChannelOverflowError`. */
function heldSendWeight(kind: 'text' | 'binary', bytes: () => number, what: string): HoldWeight {
  return {
    class: kind,
    bytes: () => bytes() + HELD_SEND_OVERHEAD_BYTES,
    fits: (held) => {
      const { channel } = getServerConfig()
      return held <= (kind === 'binary' ? channel.bufferLimitBinary : channel.bufferLimit)
    },
    overflow: () => new ChannelOverflowError(`${what} readiness buffer overflow`),
  }
}
