export { carryChannel }

import { getChannelMux } from '../../../wire-protocol/channel/server/ChannelMux.js'
import type { ServerChannel } from '../../../wire-protocol/channel/server/ServerChannel.js'
import type { RequestContext } from '../context/requestContext.js'
import { getSessionToken } from '../../../wire-protocol/wire/requestKind.js'

/** The request carries `channel`: it holds the request till it closes; a callback's `Abort()` aborts the response.
 *  Returns what the response waits for first (`ServerChannel._listening`). */
function carryChannel(channel: ServerChannel<any, any>, requestContext: RequestContext): Promise<unknown> | undefined {
  // Wired first: registering attaches a page that waits for the channel, which runs its `onOpen` callbacks.
  channel._setResponseAbort(requestContext.responseAbort.abort)
  channel._session = getSessionToken(requestContext.request)
  getChannelMux().registerChannel(channel)
  channel.onClose(requestContext.trackPending())
  return channel._listening()
}
