export { fakeMuxChannel }

import type { MuxChannel } from './ClientConnection.js'
import { ChannelAttach } from './ChannelAttach.js'
import { WIRE_MAX_RAW_FRAME_BYTES } from '../../constants.js'
import type { CreditOwner } from '../../flowControl/credit.js'
import { ReceiveCredit } from '../../flowControl/ReceiveCredit.js'
import { SelfTime } from '../../flowControl/SelfTime.js'
import { SendCredit } from '../../flowControl/SendCredit.js'

/** A channel for the connection's specs: it does nothing with what it is handed, and is never at the edge of its
 *  credit, so its data frames wait for the batch timer. `calls` replace the calls a spec records. */
function fakeMuxChannel(
  calls: Partial<Pick<MuxChannel, 'id' | '_dispatchFrame' | '_onEnded' | '_onConnectionLost'>> = {},
): MuxChannel {
  const owner: CreditOwner = {
    sendByteLimit() {},
    sendMessageLimit() {},
    sendBdpPing() {},
    backlogBytes: () => undefined,
    deliveredBefore: () => 0,
  }
  const selfTime = new SelfTime()
  return {
    id: crypto.randomUUID(),
    isClosed: false,
    _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    _onTransportOpen() {},
    _onTransportDisconnect() {},
    _dispatchFrame() {},
    _onEnded() {},
    _onConnectionLost() {},
    _attach: new ChannelAttach({
      channel: { isClosed: false },
      sendCredit: new SendCredit({ owner, selfTime }),
      receiveCredit: new ReceiveCredit({ owner, selfTime }),
    }),
    ...calls,
  }
}
