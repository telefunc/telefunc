/// <reference types="@cloudflare/workers-types" />
export {
  DEFAULT_PUBSUB_BUCKETS,
  KNOWN_PUBSUB_BUCKETS,
  TELEFUNC_PUBSUB_BUCKET_HEADER,
  TELEFUNC_SESSION_HEADER,
  TELEFUNC_SHARD_HEADER,
  getBucketCoordinatorShardIndices,
  getDeterministicKeyBucketIndex,
  getShardIndicesForBucket,
  resolveCloudflareLocationHint,
  resolveSessionRoutingTarget,
}
export type { CloudflareScale, LocationBucket, SessionRoutingTarget }

import { CLOUDFLARE_COLO_LOCATION_HINT_MAP } from './coloLocationHintMap.js'
import { TELEFUNC_SESSION_HEADER } from '../../../constants.js'
import { assert } from '../../../../utils/assert.js'

/**
 * Cloudflare routing primitives for Telefunc's session and pub/sub Durable Objects.
 *
 * Routing now always resolves to one of Telefunc's canonical Cloudflare location hints.
 * If Cloudflare doesn't provide a precise mapping, Telefunc uses `locationFallback`.
 */
const DEFAULT_PUBSUB_BUCKETS = [
  'wnam',
  'enam',
  'weur',
  'eeur',
  'apac',
  'oc',
] as const satisfies readonly DurableObjectLocationHint[]
const TELEFUNC_PUBSUB_BUCKET_HEADER = 'x-telefunc-pubsub-bucket'
/** Internal: forwarded to the DO so it knows its own instance name. */
const TELEFUNC_SHARD_HEADER = 'x-telefunc-shard'
const KNOWN_PUBSUB_BUCKETS = new Set<string>(DEFAULT_PUBSUB_BUCKETS as readonly string[])

type LocationBucket = DurableObjectLocationHint
type CloudflareScale = number | Partial<Record<DurableObjectLocationHint, number>>
type CloudflareRequest = Request & { cf?: { colo?: string; continent?: string } }
type SessionRoutingTarget = {
  sessionInstanceName: string
  locationBucket: LocationBucket
  shardOrdinal: number
}

const CLOUDFLARE_CONTINENT_LOCATION_HINT_MAP = {
  AF: 'weur',
  AS: 'apac',
  OC: 'oc',
  SA: 'enam',
  AN: 'oc',
} as const satisfies Partial<Record<string, DurableObjectLocationHint>>

/**
 * Maps one location bucket to its local coordinator-shard ordinals.
 * Buckets no longer share one global shard index space; each bucket owns ordinals `0..n-1` independently.
 */
function getBucketCoordinatorShardIndices(
  scale: CloudflareScale | undefined,
  locationBucket: LocationBucket,
): number[] {
  const shardCount = getBucketCoordinatorScale(scale, locationBucket)
  assert(
    shardCount > 0,
    `Cloudflare bucket coordinator scale does not define any shards for location bucket "${locationBucket}".`,
  )
  return Array.from({ length: shardCount }, (_, shardOrdinal) => shardOrdinal)
}

/**
 * Maps one location bucket to its local session-shard ordinals.
 * Buckets no longer share one global shard index space; each bucket owns ordinals `0..n-1` independently.
 */
function getShardIndicesForBucket(scale: CloudflareScale | undefined, locationBucket: LocationBucket): number[] {
  const shardCount = getBucketScale(scale, locationBucket)
  assert(shardCount > 0, `Cloudflare session scale does not define any shards for location bucket "${locationBucket}".`)
  return Array.from({ length: shardCount }, (_, shardOrdinal) => shardOrdinal)
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
 * Derives a session shard target from the request's Cloudflare location metadata and the current scale.
 */
function resolveSessionRoutingTarget(
  baseInstanceName: string,
  scale: CloudflareScale | undefined,
  request: Request,
  locationFallback: DurableObjectLocationHint,
): SessionRoutingTarget {
  const locationBucket = resolveCloudflareLocationHint(request, locationFallback)
  const shardIndices = getShardIndicesForBucket(scale, locationBucket)
  const shardOrdinal = shardIndices[Math.floor(Math.random() * shardIndices.length)]!
  const sessionInstanceName = getSessionShardName(baseInstanceName, locationBucket, shardOrdinal)

  return { sessionInstanceName, locationBucket, shardOrdinal }
}

/**
 * Deterministically maps one channel key into one bucket index.
 * Shared by both coordinator-bucket selection and in-bucket shard selection so all keyed pubsub routing uses the same hash.
 */
function getDeterministicKeyBucketIndex(key: string, bucketCount: number): number {
  assert(
    Number.isInteger(bucketCount) && bucketCount > 0,
    `Cloudflare keyed pubsub routing requires a positive bucket count. Received ${String(bucketCount)}.`,
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
    KNOWN_PUBSUB_BUCKETS.has(locationFallback),
    `Cloudflare locationFallback must be one of ${DEFAULT_PUBSUB_BUCKETS.join(', ')}. Received ${String(locationFallback)}.`,
  )

  const cf = (request as CloudflareRequest).cf
  const colo = cf?.colo?.trim().toUpperCase()

  return (
    (colo && CLOUDFLARE_COLO_LOCATION_HINT_MAP[colo as keyof typeof CLOUDFLARE_COLO_LOCATION_HINT_MAP]) ||
    CLOUDFLARE_CONTINENT_LOCATION_HINT_MAP[
      cf?.continent?.trim().toUpperCase() as keyof typeof CLOUDFLARE_CONTINENT_LOCATION_HINT_MAP
    ] ||
    locationFallback
  )
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
  return sessionScale === 0 ? 0 : Math.ceil(sessionScale / 2)
}
