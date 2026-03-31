export { pubsubReplacer }

import type { PubSubContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_PUBSUB } from '../../constants.js'
import { ServerPubSub } from '../server-pubsub.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const pubsubReplacer: ReplacerType<PubSubContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_PUBSUB,
  detect(value): value is PubSubContract['value'] {
    return ServerPubSub.isServerPubSub(value)
  },
  getMetadata(ps, { registerChannel }) {
    ps._registerChannel()
    registerChannel(ps)
    return {
      channelId: ps.id,
      key: ps.key,
    }
  },
}
