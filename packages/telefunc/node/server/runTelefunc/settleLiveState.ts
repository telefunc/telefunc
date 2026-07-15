export { settleLiveState }

import { publishQueuedTags } from '../live/tags.js'
import { disposeUntakenLiveSources } from '../live/source.js'

/** Core-owned settle, run on every settle path after the user settled hooks: publish the request's
 *  queued tag batch (awaited — a publish failure is logged, counted, and fired to local subscribers
 *  by the tag layer, never masking the result), then dispose live sources no result hook took. */
async function settleLiveState(): Promise<void> {
  await publishQueuedTags()
  disposeUntakenLiveSources()
}
