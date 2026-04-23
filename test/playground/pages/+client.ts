import { config } from 'telefunc/client'

// config.stream.transport = "binary-inline"
// config.channel.transports = ["sse"]

const channelTransports = parseChannelTransports(import.meta.env.PUBLIC_ENV__CHANNEL_TRANSPORTS)
config.stream.transport = (import.meta.env.PUBLIC_ENV__STREAM_TRANSPORT || 'channel') as
  | 'binary-inline'
  | 'sse-inline'
  | 'channel'
config.channel.transports = channelTransports

function parseChannelTransports(value: string | undefined): Array<'sse' | 'ws'> {
  const parsed: unknown = JSON.parse(value ?? '["sse"]')
  if (
    !Array.isArray(parsed) ||
    parsed.length === 0 ||
    !parsed.every((transport) => transport === 'sse' || transport === 'ws')
  ) {
    throw new Error(`Invalid PUBLIC_ENV__CHANNEL_TRANSPORTS: ${value ?? '(unset)'}`)
  }
  return parsed
}
