export { withFrameLimit }

import { getAdapterMaxFrameBytes } from '../../../../node/server/serverConfig.js'

/** A channel-backed value's `metadata`, with the adapter's frame limit added where the adapter has one: the page's
 *  end of the channel sends no larger frame. */
function withFrameLimit<Metadata extends { maxFrameBytes?: number }>(metadata: Metadata): Metadata {
  const maxFrameBytes = getAdapterMaxFrameBytes()
  if (maxFrameBytes) metadata.maxFrameBytes = maxFrameBytes
  return metadata
}
