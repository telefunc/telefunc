// The server half of the wire: a `Live` in a telefunction's result is sent as its snapshot plus a channel that says
// when it went stale. Registered when `@telefunc/live` is imported, which telefunc does for every `.telefunc` file.

import { config } from 'telefunc'
import type { ReplacerType, ServerReplacerContext, TypeContract } from 'telefunc'
import { LIVE, type LiveCell } from '../live.js'
import { EXTENSION_NAME, LIVE_PREFIX } from './shared.js'

type LiveContract = TypeContract<LiveCell<unknown>, unknown, { data: unknown; channelId: string }>

const liveReplacer: ReplacerType<LiveContract, ServerReplacerContext> = {
  prefix: LIVE_PREFIX,
  detect: (value): value is LiveCell<unknown> => typeof value === 'object' && value !== null && LIVE in value,
  replace(live, context) {
    // The message carries nothing: its arrival is the signal.
    const stale = context.createChannel<never, undefined>()
    const stop = live.onStale(() => void stale.send(undefined))
    const data = live.send()
    stale.onClose(() => {
      stop()
      live.release()
    })
    return {
      metadata: { data, channelId: stale.id },
      close: async () => {
        await stale.close()
      },
      abort: (abortError) => stale.abort(abortError.abortValue),
    }
  },
}

// A second copy of this package (two versions installed) must not register twice.
if (!config.extensions.some((extension) => extension.name === EXTENSION_NAME))
  config.extensions.push({
    name: EXTENSION_NAME,
    responseTypes: [liveReplacer as ReplacerType<TypeContract, ServerReplacerContext>],
  })
