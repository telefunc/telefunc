export { setShardInfo, getShardForPost, getLastShard }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/**
 * Client-side shard registry.
 *
 * Keeps the client's latest shard token in memory for each `telefuncUrl`,
 * so follow-up requests stay routed to the same server-side Durable Object shard.
 *
 * - `getLastShard` — returns the last known shard; used by `ClientChannel`
 *   to open the WS connection to the correct Durable Object.
 * - `getShardForPost` — returns the shard for appending as an advisory `?shard=` token to POST URLs.
 */

const globalObject = getGlobalObject<{ registry: Map<string, string> }>('shard-registry.ts', {
  registry: new Map<string, string>(),
})

function setShardInfo(telefuncUrl: string, shard: string): void {
  globalObject.registry.set(telefuncUrl, shard)
}

function getLastShard(telefuncUrl: string): string | undefined {
  return globalObject.registry.get(telefuncUrl)
}

function getShardForPost(telefuncUrl: string): string | undefined {
  return globalObject.registry.get(telefuncUrl)
}
