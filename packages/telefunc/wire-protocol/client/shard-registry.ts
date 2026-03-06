export { setShardInfo, getLastShard, getStickyShardForPost }

/**
 * Client-side shard registry.
 *
 * Records the shard and stickiness preference from the server's response headers
 * (`x-telefunc-shard` and `x-telefunc-sticky`), keyed by `telefuncUrl`.
 *
 * - `getLastShard` — always returns the last known shard; used by `ClientChannel`
 *   to open the WS connection to the correct Durable Object.
 * - `getStickyShardForPost` — returns the shard only when the server opted in to
 *   stickiness; used by `remoteTelefunctionCall` to append `?shard=N` to POST URLs.
 */

type ShardInfo = { shard: string; sticky: boolean }

const registry = new Map<string, ShardInfo>()

function setShardInfo(telefuncUrl: string, shard: string, sticky: boolean): void {
  registry.set(telefuncUrl, { shard, sticky })
}

/** Always returns the last known shard (for WS/channel routing). */
function getLastShard(telefuncUrl: string): string | undefined {
  return registry.get(telefuncUrl)?.shard
}

/** Returns the shard only when the server has opted in to sticky sharding (for POST URL pinning). */
function getStickyShardForPost(telefuncUrl: string): string | undefined {
  const info = registry.get(telefuncUrl)
  return info?.sticky ? info.shard : undefined
}
