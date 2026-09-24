export { callDefinedCommand }

import type { Cluster, Redis } from 'ioredis'
import { assert } from './assert.js'

/** Invoke a command registered via ioredis's `defineCommand`. ioredis attaches the
 *  registered name as a dynamic method on the client which TypeScript can't see, so
 *  we go through an indexed access. The shape `(...keysThenArgv)` is ioredis's
 *  contract for `defineCommand`-bound calls. */
function callDefinedCommand(
  client: Redis | Cluster,
  command: string,
  keysAndArgs: ReadonlyArray<string | Uint8Array>,
): Promise<unknown> {
  const fn = (client as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[command]
  assert(typeof fn === 'function', `Redis command "${command}" was not registered via defineCommand`)
  return fn.apply(client, keysAndArgs as unknown[])
}
