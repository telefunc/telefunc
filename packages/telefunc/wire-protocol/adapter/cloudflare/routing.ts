/// <reference types="@cloudflare/workers-types" />
export {
  DEFAULT_BROADCAST_BUCKETS,
  TELEFUNC_BROADCAST_BUCKET_HEADER,
  TELEFUNC_SHARD_HEADER,
  assertLocationFallbackIsScaled,
  getBucketCoordinatorScale,
  getDeterministicKeyBucketIndex,
  readForwardedTarget,
  readLocationBucket,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
}
export type { CloudflareScale, LocationBucket, SessionRoutingTarget }

import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { assert, assertUsage } from '../../../utils/assert.js'

/**
 * Cloudflare routing primitives for Telefunc's session and broadcast Durable Objects.
 *
 * Routing resolves to one of Telefunc's canonical Cloudflare location hints, or to `locationFallback` where
 * Cloudflare gives no precise one.
 */
const DEFAULT_BROADCAST_BUCKETS = [
  'wnam',
  'enam',
  'weur',
  'eeur',
  'apac',
  'oc',
] as const satisfies readonly DurableObjectLocationHint[]
const TELEFUNC_BROADCAST_BUCKET_HEADER = 'x-telefunc-broadcast-bucket'
/** The session Durable Object's name. */
const TELEFUNC_SHARD_HEADER = 'x-telefunc-shard'
/** Each bucket Broadcast places members in, by its name. */
const BROADCAST_BUCKETS_BY_NAME = new Map<string, LocationBucket>(
  DEFAULT_BROADCAST_BUCKETS.map((bucket) => [bucket, bucket]),
)

type LocationBucket = DurableObjectLocationHint
type CloudflareScale = number | Partial<Record<DurableObjectLocationHint, number>>
type SessionRoutingTarget = {
  sessionInstanceName: string
  locationBucket: LocationBucket
}

// Fallback regions for continents without a direct DurableObject location hint:
// AF → weur (lowest-latency for African traffic), SA → enam, AN → oc.
const CLOUDFLARE_CONTINENT_LOCATION_HINT_MAP = {
  AF: 'weur',
  AS: 'apac',
  OC: 'oc',
  SA: 'enam',
  AN: 'oc',
} as const satisfies Partial<Record<string, DurableObjectLocationHint>>
/** Each colo's bucket, and each continent's, by its code. */
const BUCKETS_BY_COLO = new Map<string, LocationBucket>(Object.entries(CLOUDFLARE_COLO_LOCATION_HINT_MAP))
const BUCKETS_BY_CONTINENT = new Map<string, LocationBucket>(Object.entries(CLOUDFLARE_CONTINENT_LOCATION_HINT_MAP))

/** `bucket` as it was stored or sent, once checked as one of the buckets Broadcast places members in. */
function readLocationBucket(bucket: string): LocationBucket {
  const known = BROADCAST_BUCKETS_BY_NAME.get(bucket)
  if (known === undefined) assert(false, `Invalid Broadcast location bucket "${bucket}".`)
  return known
}

/** Builds the concrete session Durable Object name for one bucket-local shard ordinal. */
function getSessionShardName(baseInstanceName: string, locationBucket: LocationBucket, shardOrdinal: number): string {
  assert(
    Number.isInteger(shardOrdinal) && shardOrdinal >= 0,
    `Cloudflare session shard ordinal must be a non-negative integer. Received ${String(shardOrdinal)}.`,
  )
  return `${baseInstanceName}-shard-${locationBucket}-${shardOrdinal}`
}

/**
 * Derives a session shard target from the session's home, else the request's Cloudflare location metadata, and the
 * current scale.
 */
function resolveSessionRoutingTarget(
  baseInstanceName: string,
  scale: CloudflareScale | undefined,
  request: Request,
  locationFallback: DurableObjectLocationHint,
  token: string,
  home: string | null,
): SessionRoutingTarget {
  // The region its session Durable Object named, from any region the page roams to.
  let locationBucket =
    (home !== null && BROADCAST_BUCKETS_BY_NAME.get(home)) || resolveCloudflareLocationHint(request, locationFallback)
  // With a per-region `scale` map, a recognized region the user didn't list has no Durable Objects
  // of its own — route those requests to the `locationFallback` region instead of failing.
  if (getScaleCountForBucket(scale, locationBucket) === 0) {
    locationBucket = locationFallback
  }
  // By the session's token, so its calls and channels reach one shard of its region.
  const shardOrdinal = getDeterministicKeyBucketIndex(token, getBucketScale(scale, locationBucket))
  const sessionInstanceName = getSessionShardName(baseInstanceName, locationBucket, shardOrdinal)

  return { sessionInstanceName, locationBucket }
}

/** The session Durable Object and region the Worker forwarded `request` to, as its routing headers name them. */
function readForwardedTarget(request: Request): SessionRoutingTarget | null {
  const sessionInstanceName = request.headers.get(TELEFUNC_SHARD_HEADER)
  const locationBucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER)
  return sessionInstanceName && locationBucket
    ? { sessionInstanceName, locationBucket: readLocationBucket(locationBucket) }
    : null
}

/** Deterministically maps a key, a broadcast's channel key or a session's token, into one bucket index. */
function getDeterministicKeyBucketIndex(key: string, bucketCount: number): number {
  assert(
    Number.isInteger(bucketCount) && bucketCount > 0,
    `Cloudflare key routing requires a positive bucket count. Received ${String(bucketCount)}.`,
  )

  let hash = 0

  for (let index = 0; index < key.length; index++) {
    hash = (hash * 31 + key.charCodeAt(index)) >>> 0
  }

  return hash % bucketCount
}

/** Resolves the canonical Cloudflare location hint for one request, or the explicit user fallback when no precise hint exists. */
function resolveCloudflareLocationHint(
  request: Request,
  locationFallback: DurableObjectLocationHint,
): DurableObjectLocationHint {
  assert(
    BROADCAST_BUCKETS_BY_NAME.has(locationFallback),
    `Cloudflare locationFallback must be one of ${DEFAULT_BROADCAST_BUCKETS.join(', ')}. Received ${String(locationFallback)}.`,
  )

  const { cf } = request
  return bucketOf(BUCKETS_BY_COLO, cf?.colo) ?? bucketOf(BUCKETS_BY_CONTINENT, cf?.continent) ?? locationFallback
}

/** The bucket `code`, a colo or a continent as Cloudflare names it on a request, maps to in `buckets`. */
function bucketOf(buckets: Map<string, LocationBucket>, code: unknown): LocationBucket | undefined {
  return typeof code === 'string' ? buckets.get(code.trim().toUpperCase()) : undefined
}

/**
 * Reads one location bucket's effective session scale.
 * Canonical hinted buckets follow the user map.
 */
function getBucketScale(scale: CloudflareScale | undefined, locationBucket: LocationBucket): number {
  const count = getScaleCountForBucket(scale, locationBucket)
  assert(
    Number.isInteger(count) && count >= 0,
    `Cloudflare WebSocket scale for location bucket "${locationBucket}" must be a non-negative integer. Received ${String(count)}.`,
  )
  return count
}

/**
 * A per-region `scale` map may omit regions; requests from those regions route to `locationFallback`.
 * That only works when `locationFallback` itself is scaled — otherwise the fallback lands on a bucket
 * with no shards and routing throws at request time. Enforce it once, at config time.
 */
function assertLocationFallbackIsScaled(
  scale: CloudflareScale | undefined,
  locationFallback: DurableObjectLocationHint,
): void {
  if (typeof scale !== 'object') return
  assertUsage(
    getScaleCountForBucket(scale, locationFallback) > 0,
    `Cloudflare \`locationFallback\` "${locationFallback}" must appear in your per-region \`scale\` map with a count greater than 0, since requests from regions you didn't list route to it (\`locationFallback\` defaults to "weur").`,
  )
}

function getScaleCountForBucket(scale: CloudflareScale | undefined, locationBucket: LocationBucket): number {
  if (typeof scale === 'number' || scale === undefined) {
    const count = scale ?? 1
    assert(
      Number.isInteger(count) && count > 0,
      `Cloudflare WebSocket scale must be a positive integer. Received ${String(count)}.`,
    )

    return count
  }

  const exactCount = scale[locationBucket]
  if (exactCount !== undefined) return exactCount

  return 0
}

/**
 * Derives coordinator count from session count for one location bucket.
 * Coordinators are intentionally fewer than session shards because they aggregate cross-shard fanout work.
 */
function getBucketCoordinatorScale(scale: CloudflareScale | undefined, locationBucket: LocationBucket): number {
  const sessionScale = getBucketScale(scale, locationBucket)
  assert(
    sessionScale > 0,
    `Cloudflare bucket coordinator scale does not define any shards for location bucket "${locationBucket}".`,
  )
  return Math.ceil(sessionScale / 2)
}
