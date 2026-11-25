// The client half of the wire: a `Live` arrives as its snapshot plus the channel saying when it went stale.
// Registered by importing `@telefunc/live/wire`, which telefunc does in the browser for every `.telefunc` file: a
// response carrying a `Live` can't arrive before it.

import { config } from 'telefunc/client'
import type { ClientReviverContext, ReviverType, TypeContract } from 'telefunc/client'
import { EXTENSION_NAME, LIVE_PREFIX, SUBSCRIPTION, type Subscription } from './shared.js'

type LiveContract = TypeContract<unknown, unknown, { data: unknown; channelId: string }>

const liveReviver: ReviverType<LiveContract, ClientReviverContext> = {
  prefix: LIVE_PREFIX,
  revive({ data, channelId }, context) {
    const stale = context.createChannel<never, undefined>({ channelId })
    const listeners = new Set<() => void>()
    stale.listen(() => {
      for (const listener of [...listeners]) listener()
    })
    const subscription: Subscription = {
      onStale(listener) {
        listeners.add(listener)
        return () => listeners.delete(listener)
      },
      close: async () => {
        await stale.close()
      },
    }
    return {
      value: { data, [SUBSCRIPTION]: subscription },
      close: subscription.close,
      abort: (abortError) => stale.abort(abortError.abortValue, abortError.message),
    }
  },
}

if (!config.extensions.some((extension) => extension.name === EXTENSION_NAME))
  config.extensions.push({
    name: EXTENSION_NAME,
    responseTypes: [liveReviver as unknown as ReviverType<TypeContract, ClientReviverContext>],
  })
