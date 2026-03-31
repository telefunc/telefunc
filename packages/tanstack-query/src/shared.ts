export { LQ_KEY_PREFIX, EXTENSION_NAME }
export type { LiveQueryResult }

import type { ClientChannel } from 'telefunc'

declare module 'telefunc' {
  interface TelefuncExtensionRegistry {
    'telefunc/tanstack-query': { queryKey: unknown[] }
  }
}

const LQ_KEY_PREFIX = 'lq:'
const EXTENSION_NAME = 'telefunc/tanstack-query'

type LiveQueryResult = {
  data: unknown
  channel: ClientChannel<never, 'invalidate'>
}
