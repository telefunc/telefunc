export { getConnectionOptions }

import type { ConnectionOptions } from '../../wire-protocol/channel/client/channel.js'
import type { ChannelTransports } from '../../wire-protocol/constants.js'

/** The connection a call's channels ride, as its context configures it. */
function getConnectionOptions(callContext: {
  channel: { transports: ChannelTransports }
  connectionKey?: string
  channelIdleTimeout?: number
  headers?: Record<string, string> | null
  telefuncUrl: string
}): ConnectionOptions {
  return {
    transports: callContext.channel.transports,
    connectionKey: callContext.connectionKey,
    headers: callContext.headers ?? undefined,
    telefuncUrl: callContext.telefuncUrl,
    idleTimeout: callContext.channelIdleTimeout,
  }
}
