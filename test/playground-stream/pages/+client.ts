import { config } from 'telefunc/client'
import { parseChannelTransports } from '../channelTransports'

// config.stream.transport = "binary-inline"
// config.channel.transports = ["sse"]

const channelTransports = parseChannelTransports(import.meta.env.PUBLIC_ENV__CHANNEL_TRANSPORTS)
config.stream.transport = (import.meta.env.PUBLIC_ENV__STREAM_TRANSPORT || 'channel') as
  | 'binary-inline'
  | 'sse-inline'
  | 'channel'
config.channel.transports = channelTransports
// config.channel.transports = ['ws']
// config.channel.transports = ['sse', 'ws']
