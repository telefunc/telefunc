export { muxChannelStub }

import { WIRE_MAX_RAW_FRAME_BYTES } from '../constants.js'
import type { DecodedFrame } from '../wire/connectionFrame.js'
import type { MuxChannel } from '../channel/client/connection.js'

/** A `MuxChannel` that keeps the frames dispatched to it: registering one is enough to make a connection open a wire. */
function muxChannelStub(id: string = crypto.randomUUID()): MuxChannel & { dispatched: DecodedFrame[] } {
  return {
    id,
    isClosed: false,
    _maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    dispatched: [],
    _onTransportOpen() {},
    _dispatchFrame(frame) {
      this.dispatched.push(frame)
    },
    _onTransportClose() {},
    _onTransportBatched() {},
    _reattachState: () => ({}),
    _onProbeAnswered() {},
    _fitReplays() {},
    _acknowledge() {},
    _answered() {},
  }
}
