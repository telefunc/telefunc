export { carryChannel }

import { getChannelMux } from '../../../wire-protocol/channel/server/ChannelMux.js'
import type { ServerChannel } from '../../../wire-protocol/channel/server/ServerChannel.js'
import type { RequestContext } from '../context/requestContext.js'

/** The request carries `channel`: it holds the request till it closes; a callback's `Abort()` aborts the response. */
function carryChannel<ClientToServer, ServerToClient>(
  channel: ServerChannel<ClientToServer, ServerToClient>,
  requestContext: RequestContext,
): void {
  // The response's abort first: registering attaches a page that waits for the channel, which runs its `onOpen`
  // callbacks, and an `Abort()` in one aborts the response.
  channel._setResponseAbort(requestContext.responseAbort.abort)
  getChannelMux().registerChannel(channel, requestContext.request)
  channel.onClose(requestContext.trackPending())
}
