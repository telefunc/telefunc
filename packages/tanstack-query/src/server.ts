export { invalidate }

import { channel, pubsub, config } from 'telefunc'
import type { TelefuncServerExtension } from 'telefunc'
import { partialMatchKey } from '@tanstack/query-core'
import {
  __LQ__PUBSUB_KEY_PREFIX,
  __LQ__DATA_KEY,
  __LQ__CHANNEL_KEY,
  type LiveQueryExtensionData,
  type LiveQueryResult,
} from './shared.js'

function topLevelKey(queryKey: readonly unknown[]): string {
  return __LQ__PUBSUB_KEY_PREFIX + String(queryKey[0] ?? '')
}

// --- Extension ---

const extension = {
  name: 'telefunc/tanstack-query',
  hooks: {
    onTransformResult(ctx): LiveQueryResult {
      const { queryKey } = ctx.data as LiveQueryExtensionData
      const ch = channel<never, 'invalidate'>()

      const unsub = pubsub.subscribe<unknown[]>(topLevelKey(queryKey), (invalidatedKey) => {
        if (partialMatchKey(queryKey, invalidatedKey)) {
          ch.send('invalidate')
        }
      })

      ch.onClose(() => unsub())

      return { [__LQ__DATA_KEY]: ctx.result, [__LQ__CHANNEL_KEY]: ch.client }
    },
  },
} satisfies TelefuncServerExtension
config.extensions.push(extension)

// --- Publishing ---

function invalidate(queryKey: unknown[]) {
  pubsub.publish(topLevelKey(queryKey), queryKey)
}
