export {
  isRecord,
  ownMetadata,
  stampNewer,
  leaveCauseFromWire,
  leaveCauseToWire,
  mergeAttributes,
  normalizeJoinOptions,
}

import { assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import type { JoinOptions, LeaveCause, ParticipantMeta, RoomMeta } from './types.js'

function isRecord(value: unknown): value is Record<string, unknown> {
  if (!isObject(value) || Array.isArray(value)) return false
  try {
    const prototype = Object.getPrototypeOf(value)
    return prototype === null || (Object.getPrototypeOf(prototype) === null && prototype.constructor?.name === 'Object')
  } catch {
    return false
  }
}
/** Take ownership of metadata at a state boundary and expose only the immutable owned value. */
function ownMetadata<T extends RoomMeta | ParticipantMeta>(meta: T): T {
  return Object.freeze({ ...meta }) as T
}

/** Later timestamp wins; equal timestamps break deterministically by writer ID. */
function stampNewer(a: { at: number; by: string }, b: { at: number; by: string }): boolean {
  return a.at > b.at || (a.at === b.at && a.by > b.by)
}

/** Decode a leave event's cause — an absent wire cause means a voluntary leave. */
function leaveCauseFromWire(event: {
  cause?: 'removed' | 'disconnected' | 'closed'
  reason?: unknown
}): LeaveCause {
  if (event.cause === 'removed') {
    return event.reason === undefined ? { type: 'removed' } : { type: 'removed', reason: event.reason }
  }
  return { type: event.cause ?? 'left' }
}
/** Encode a cause into leave-event fields — `'left'` is the wire default and travels as nothing. */
function leaveCauseToWire(cause: LeaveCause): {
  cause?: 'removed' | 'disconnected' | 'closed'
  reason?: unknown
} {
  if (cause.type === 'removed')
    return cause.reason === undefined ? { cause: 'removed' } : { cause: 'removed', reason: cause.reason }
  return cause.type === 'left' ? {} : { cause: cause.type }
}

/** Merge `attrs` into `meta` per key, returning a new object — the `setAttributes()` semantics. A value of `undefined` deletes its key (the serializer preserves `undefined` on the wire). */
function mergeAttributes(meta: ParticipantMeta, attrs: ParticipantMeta): ParticipantMeta {
  const next: ParticipantMeta = { ...meta }
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined) delete next[key]
    else Object.defineProperty(next, key, { value, enumerable: true, configurable: true, writable: true })
  }
  return Object.freeze(next)
}
/** Validates `join(options)` and resolves the participant `meta` + `selfDelivery`. */
function normalizeJoinOptions(options: JoinOptions | undefined): {
  meta: ParticipantMeta
  selfDelivery: boolean
  identity: string | null
  hidden: boolean
} {
  assertUsage(options === undefined || isRecord(options), 'join() options should be an object')
  const meta = options?.meta ?? {}
  assertUsage(isRecord(meta), 'join() options.meta should be an object')
  assertUsage(
    options?.selfDelivery === undefined || typeof options.selfDelivery === 'boolean',
    'join() options.selfDelivery should be a boolean',
  )
  assertUsage(
    options?.identity === undefined || (typeof options.identity === 'string' && options.identity.length > 0),
    'join() options.identity should be a non-empty string',
  )
  assertUsage(
    options?.hidden === undefined || typeof options.hidden === 'boolean',
    'join() options.hidden should be a boolean',
  )
  return {
    meta: ownMetadata(meta),
    selfDelivery: options?.selfDelivery !== false,
    identity: options?.identity ?? null,
    hidden: options?.hidden ?? false,
  }
}
