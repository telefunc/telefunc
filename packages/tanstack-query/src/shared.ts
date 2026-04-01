export { __TQ__PUBSUB_KEY_PREFIX, __TQ__DATA_KEY, __TQ__CHANNEL_KEY, EXTENSION_NAME }
export type { TanstackQueryExtensionData, TanstackQueryResult }

import type { ClientChannel } from 'telefunc'

const __TQ__PUBSUB_KEY_PREFIX = '__tq__:'
const __TQ__DATA_KEY = '__tq__data'
const __TQ__CHANNEL_KEY = '__tq__channel'
const EXTENSION_NAME = 'telefunc/tanstack-query'

type TanstackQueryExtensionData = { queryKey: readonly unknown[] } | { invalidates: readonly (readonly unknown[])[] }

type TanstackQueryResult = {
  [__TQ__DATA_KEY]: unknown
  [__TQ__CHANNEL_KEY]: ClientChannel<never, 'invalidate'>
}
