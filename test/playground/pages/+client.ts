import { config } from 'telefunc/client'
config.stream = {
  transport: import.meta.env.PUBLIC_ENV__STREAM_TRANSPORT || 'channel',
} as const
config.channel = {
  transport: import.meta.env.PUBLIC_ENV__CHANNEL_TRANSPORT || 'sse',
} as const
