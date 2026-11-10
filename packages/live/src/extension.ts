import { Channel, config, getRawContext } from 'telefunc'
import type { TelefuncServerExtension } from 'telefunc'
import { LIVE_READS, type LiveReads } from './callReads.js'
import { EXTENSION_NAME, type LiveResult } from './shared.js'

config.extensions.push({
  name: EXTENSION_NAME,
  hooks: {
    onTelefunctionResult({ result }) {
      const changes = new Channel<never, 'changed'>()
      const reads = getRawContext()?.[LIVE_READS] as LiveReads | undefined
      const stop = reads?.onChange(() => changes.send('changed'))
      if (stop) changes.onClose(stop)
      return { data: result, changes: changes.client } satisfies LiveResult
    },
  },
} satisfies TelefuncServerExtension)
