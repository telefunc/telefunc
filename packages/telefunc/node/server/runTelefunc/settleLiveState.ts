export { settleLiveState }

import { publishQueuedTags } from '../live/tags.js'

/** Core-owned settle, run on every settle path after the user settled hooks: publish the request's
 *  queued tag batch (awaited — a publish failure is logged and fired to local subscribers by the tag
 *  layer, never masking the result). */
async function settleLiveState(): Promise<void> {
  await publishQueuedTags()
}
