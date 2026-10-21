export { heldSendWeight, ESTABLISHING_HOLD_OVERHEAD_BYTES }

import { ChannelOverflowError } from '../channel/channelErrors.js'
import { getServerConfig } from '../../node/server/serverConfig.js'
import type { HoldWeight } from './subscription-manager.js'

/** What holding a send retains beyond its payload (130–160 bytes measured), so small sends are bounded too. */
const ESTABLISHING_HOLD_OVERHEAD_BYTES = 256

/** A send held for an establishing subscription counts its payload and `ESTABLISHING_HOLD_OVERHEAD_BYTES` against `budget`'s
 *  `bufferLimit` (`bufferLimitBinary` for binary), read only for a held send, as resolving the config is costly; one
 *  past it is refused with `ChannelOverflowError`. */
function heldSendWeight(kind: 'text' | 'binary', budget: string, bytes: () => number, what: string): HoldWeight {
  return {
    budget,
    bytes: () => bytes() + ESTABLISHING_HOLD_OVERHEAD_BYTES,
    fits: (held) => {
      const { channel } = getServerConfig()
      return held <= (kind === 'binary' ? channel.bufferLimitBinary : channel.bufferLimit)
    },
    overflow: () => new ChannelOverflowError(`${what} readiness buffer overflow`),
  }
}
