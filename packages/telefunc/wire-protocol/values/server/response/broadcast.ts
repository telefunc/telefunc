export { broadcastReplacer }

import type { BroadcastContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_BROADCAST } from '../../../constants.js'
import { ServerBroadcast } from '../../../channel/server/ServerBroadcast.js'
import { withFrameLimit } from './withFrameLimit.js'
import { assertIsNotBrowser } from '../../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const broadcastReplacer: ReplacerType<BroadcastContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_BROADCAST,
  detect(value): value is BroadcastContract['value'] {
    return ServerBroadcast.isServerBroadcast(value)
  },
  replace(broadcast, context) {
    context.registerChannel(broadcast)
    const metadata: BroadcastContract['metadata'] = { channelId: broadcast.id, key: broadcast.key }
    if (broadcast.lossy) metadata.lossy = true
    return {
      metadata: withFrameLimit(metadata),
      async close() {
        await broadcast.close()
      },
      abort(abortError) {
        broadcast.abort(abortError.abortValue)
      },
    }
  },
}
