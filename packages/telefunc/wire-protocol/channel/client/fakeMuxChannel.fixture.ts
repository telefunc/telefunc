export { fakeMuxChannel }

import type { MuxChannel } from './ClientConnection.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'

/** A channel for the connection's specs: it does nothing with what it is handed, and is never at the edge of its
 *  credit, so its data frames wait for the batch timer. `calls` replace the calls a spec records. */
function fakeMuxChannel(
  calls: Partial<Pick<MuxChannel, 'id' | '_dispatchFrame' | '_onEnded' | '_onConnectionLost'>> = {},
): MuxChannel {
  return {
    id: crypto.randomUUID(),
    isClosed: false,
    _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    _onTransportOpen() {},
    _onTransportDisconnect() {},
    _dispatchFrame() {},
    _onEnded() {},
    _onConnectionLost() {},
    _attach: {
      sendCredit: { leavesCredit: () => true },
      state: () => ({}),
      receiveCredit: { onProbeAnswered() {}, acknowledge() {}, answered() {} },
      capWindows() {},
      onBatched() {},
      onUpgradeJoined() {},
      unconfirmedWant: null,
    },
    ...calls,
  }
}
