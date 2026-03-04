export { injectFrameChannel, extractFrameChannel }

import { assert } from '../utils/assert.js'
import { FRAME_CHANNEL_KEY, SERIALIZER_PREFIX_CHANNEL } from './constants.js'
import { ClientChannel } from '../client/channel.js'

/** Inject a frame channel placeholder into a serialized JSON response body.
 *
 *  json-serializer always produces a top-level JSON object (`{...}`).
 *  We parse it, add the channel placeholder field, and re-stringify.
 *  This is safe because json-serializer special prefixes (e.g. `!TelefuncGenerator:`)
 *  are regular JSON string values — a JSON round-trip preserves them. */
function injectFrameChannel(body: string, channelId: string): string {
  assert(body.startsWith('{') && body.endsWith('}'))
  const parsed: Record<string, unknown> = JSON.parse(body)
  assert(!(FRAME_CHANNEL_KEY in parsed))
  parsed[FRAME_CHANNEL_KEY] = SERIALIZER_PREFIX_CHANNEL + JSON.stringify({ channelId })
  return JSON.stringify(parsed)
}

/** Extract the frame channel from a raw JSON response body string.
 *
 *  Mirrors `injectFrameChannel`: parses the JSON, checks for `__frameChannel`,
 *  creates a `ClientChannel` from the embedded channelId, and returns the body
 *  with `__frameChannel` stripped so downstream revivers never see it.
 *
 *  Returns `null` if no frame channel is present. */
function extractFrameChannel(body: string): { channel: ClientChannel; strippedBody: string } | null {
  const parsed: Record<string, unknown> = JSON.parse(body)
  if (!(FRAME_CHANNEL_KEY in parsed)) return null
  const raw = parsed[FRAME_CHANNEL_KEY]
  if (typeof raw !== 'string' || !raw.startsWith(SERIALIZER_PREFIX_CHANNEL)) return null
  const metadata: unknown = JSON.parse(raw.slice(SERIALIZER_PREFIX_CHANNEL.length))
  assert(typeof metadata === 'object' && metadata !== null && 'channelId' in metadata)
  const { channelId } = metadata as { channelId: string }
  delete parsed[FRAME_CHANNEL_KEY]
  return { channel: new ClientChannel(channelId), strippedBody: JSON.stringify(parsed) }
}
