export { invalidate }

import { channel, pubsub, config } from 'telefunc'
import type { TelefuncServerExtension } from 'telefunc'
import { LQ_KEY_PREFIX, type LiveQueryResult } from './shared.js'

function topLevelKey(queryKey: unknown[]): string {
  return LQ_KEY_PREFIX + String(queryKey[0] ?? '')
}

function isPrefix(prefix: unknown[], full: unknown[]): boolean {
  if (prefix.length > full.length) return false
  for (let i = 0; i < prefix.length; i++) {
    if (prefix[i] !== full[i]) return false
  }
  return true
}

// --- Extension ---

const extension = {
  name: 'telefunc/tanstack-query',
  hooks: {
    onTransformResult(ctx): LiveQueryResult {
      const { queryKey } = ctx.data
      const ch = channel<never, 'invalidate'>()

      const unsub = pubsub.subscribe<unknown[]>(topLevelKey(queryKey), (invalidatedKey) => {
        if (isPrefix(invalidatedKey, queryKey)) {
          ch.send('invalidate')
        }
      })

      ch.onClose(() => unsub())

      return { data: ctx.result, channel: ch.client }
    },
  },
} satisfies TelefuncServerExtension
config.extensions.push(extension)

// --- Publishing ---

function invalidate(queryKey: unknown[]) {
  pubsub.publish(topLevelKey(queryKey), queryKey)
}
