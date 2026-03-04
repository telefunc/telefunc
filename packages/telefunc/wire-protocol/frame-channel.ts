export { injectFrameChannel, extractFrameChannel }

import { assert } from '../utils/assert.js'
import { FRAME_CHANNEL_KEY, SERIALIZER_PREFIX_CHANNEL } from './constants.js'
import type { ChannelContract } from './placeholder-types.js'

function injectFrameChannel(body: string, metadata: ChannelContract['metadata']) {
  assert(body[0] === '{')
  const value = JSON.stringify(SERIALIZER_PREFIX_CHANNEL + JSON.stringify(metadata))
  return `{"${FRAME_CHANNEL_KEY}":${value},${body.slice(1)}`
}

function extractFrameChannel(body: string): { metadata: ChannelContract['metadata']; strippedBody: string } | null {
  const parsed: Record<string, unknown> = JSON.parse(body)
  const raw = parsed[FRAME_CHANNEL_KEY]
  if (typeof raw !== 'string' || !raw.startsWith(SERIALIZER_PREFIX_CHANNEL)) return null
  const metadata: ChannelContract['metadata'] = JSON.parse(raw.slice(SERIALIZER_PREFIX_CHANNEL.length))
  delete parsed[FRAME_CHANNEL_KEY]
  return { metadata, strippedBody: JSON.stringify(parsed) }
}
