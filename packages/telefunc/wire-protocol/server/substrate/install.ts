export { getChannelMux, installChannelSubstrate, _resetChannelSubstrateForTesting }

import { assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { ChannelMux } from './mux.js'
import { SubstrateMux } from './substrate-mux.js'
import type { ChannelSubstrate } from './protocol.js'

// Single-instance is the default — no substrate, no cluster I/O. `installChannelSubstrate`
// swaps to a `SubstrateMux` that extends `ChannelMux` with cross-instance routing.

const globalObject = getGlobalObject<{ mux: ChannelMux }>('wire-protocol/server/substrate/install.ts', () => ({
  mux: new ChannelMux(),
}))

function getChannelMux(): ChannelMux {
  return globalObject.mux
}

function installChannelSubstrate(substrate: ChannelSubstrate): void {
  if (globalObject.mux instanceof SubstrateMux) return
  assertUsage(
    !globalObject.mux.hasChannels(),
    '`config.channel.substrate` must be set before any channel is registered.',
  )
  swapMux(new SubstrateMux(substrate))
}

/** Test-only — swap to a SubstrateMux with the given substrate, or back to a plain
 *  ChannelMux when `substrate` is null. */
function _resetChannelSubstrateForTesting(substrate: ChannelSubstrate | null): void {
  swapMux(substrate ? new SubstrateMux(substrate) : new ChannelMux())
}

function swapMux(next: ChannelMux): void {
  globalObject.mux.dispose()
  globalObject.mux = next
}
