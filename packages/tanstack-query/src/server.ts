export { invalidate }

import { channel, pubsub, config } from 'telefunc'
import type { TelefuncServerExtension } from 'telefunc'
import { partialMatchKey } from '@tanstack/query-core'
import {
  __TQ__PUBSUB_KEY_PREFIX,
  __TQ__DATA_KEY,
  __TQ__CHANNEL_KEY,
  type TanstackQueryExtensionData,
  type TanstackQueryResult,
} from './shared.js'

function topLevelKey(queryKey: readonly unknown[]): string {
  return __TQ__PUBSUB_KEY_PREFIX + String(queryKey[0] ?? '')
}

// --- Extension ---

const extension = {
  name: 'telefunc/tanstack-query',
  hooks: {
    onTransformResult(ctx) {
      const data = ctx.data as TanstackQueryExtensionData

      if ('invalidates' in data) {
        for (const queryKey of data.invalidates) {
          invalidate(queryKey)
        }
        return ctx.result
      }

      const { queryKey } = data
      const ch = channel<never, 'invalidate'>()

      const unsub = pubsub.subscribe<readonly unknown[]>(topLevelKey(queryKey), (invalidatedKey) => {
        if (partialMatchKey(queryKey, invalidatedKey)) {
          ch.send('invalidate')
        }
      })

      ch.onClose(() => unsub())

      return { [__TQ__DATA_KEY]: ctx.result, [__TQ__CHANNEL_KEY]: ch.client } satisfies TanstackQueryResult
    },
  },
} satisfies TelefuncServerExtension
config.extensions.push(extension)

// --- Publishing ---

function invalidate(queryKey: readonly unknown[]) {
  pubsub.publish(topLevelKey(queryKey), queryKey)
}
