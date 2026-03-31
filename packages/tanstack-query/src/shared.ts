export { __LQ__PUBSUB_KEY_PREFIX, __LQ__DATA_KEY, __LQ__CHANNEL_KEY, EXTENSION_NAME }
export type { LiveQueryExtensionData, LiveQueryResult }

import type { ClientChannel } from 'telefunc'

const __LQ__PUBSUB_KEY_PREFIX = '__lq__:'
const __LQ__DATA_KEY = '__lq__data'
const __LQ__CHANNEL_KEY = '__lq__channel'
const EXTENSION_NAME = 'telefunc/tanstack-query'

type LiveQueryExtensionData = { queryKey: readonly unknown[] }

type LiveQueryResult = {
  [__LQ__DATA_KEY]: unknown
  [__LQ__CHANNEL_KEY]: ClientChannel<never, 'invalidate'>
}
