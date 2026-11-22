export { carryChannel }

import { getChannelMux } from '../../../wire-protocol/channel/server/ChannelMux.js'
import type { ServerChannel } from '../../../wire-protocol/channel/server/ServerChannel.js'
import type { RequestContext } from '../context/requestContext.js'

/** The request carries `channel`: it holds the request till it closes; a callback's `Abort()` aborts the response. */
function carryChannel(channel: ServerChannel<any, any>, requestContext: RequestContext): void {
  // Wired first: registering attaches a page that waits for the channel, which runs its `onOpen` callbacks.
  channel._setResponseAbort(requestContext.responseAbort.abort)
  channel._callRequest = requestContext.request
  getChannelMux().registerChannel(channel, requestContext.bodyRead)
  channel.onClose(requestContext.trackPending())
}
