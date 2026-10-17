export { broadcastReplacer }

import type { BroadcastContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_BROADCAST } from '../../constants.js'
import { ServerBroadcast } from '../server-broadcast.js'
import { getAdapterMaxFrameBytes } from '../../../node/server/serverConfig.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const broadcastReplacer: ReplacerType<BroadcastContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_BROADCAST,
  detect(value): value is BroadcastContract['value'] {
    return ServerBroadcast.isServerBroadcast(value)
  },
  replace(broadcast, context) {
    const maxFrameBytes = getAdapterMaxFrameBytes()
    context.registerChannel(broadcast)
    const metadata: BroadcastContract['metadata'] = { channelId: broadcast.id, key: broadcast.key }
    if (maxFrameBytes) metadata.maxFrameBytes = maxFrameBytes
    return {
      metadata,
      async close() {
        await broadcast.close()
      },
      abort(abortError) {
        broadcast.abort(abortError.abortValue)
      },
    }
  },
}
