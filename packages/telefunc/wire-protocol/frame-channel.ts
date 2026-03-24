export { injectFrameChannel, extractFrameChannel }

import { assert } from '../utils/assert.js'
import { FRAME_CHANNEL_KEY, SERIALIZER_PREFIX_CHANNEL } from './constants.js'

type FrameChannelMetadata = {
  channelId: string
}

function injectFrameChannel(body: string, metadata: FrameChannelMetadata) {
  assert(body[0] === '{')
  const value = JSON.stringify(SERIALIZER_PREFIX_CHANNEL + JSON.stringify(metadata))
  return `{"${FRAME_CHANNEL_KEY}":${value},${body.slice(1)}`
}

function extractFrameChannel(body: string): { metadata: FrameChannelMetadata; strippedBody: string } | null {
  const parsed: Record<string, unknown> = JSON.parse(body)
  const raw = parsed[FRAME_CHANNEL_KEY]
  if (typeof raw !== 'string' || !raw.startsWith(SERIALIZER_PREFIX_CHANNEL)) return null
  const metadata: FrameChannelMetadata = JSON.parse(raw.slice(SERIALIZER_PREFIX_CHANNEL.length))
  delete parsed[FRAME_CHANNEL_KEY]
  return { metadata, strippedBody: JSON.stringify(parsed) }
}
