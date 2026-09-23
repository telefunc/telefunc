export {
  isRecord,
  ownMetadata,
  ownLeaveCause,
  stampNewer,
  leaveCauseFromWire,
  leaveCauseToWire,
  mergeAttributes,
  normalizeJoinOptions,
  assertParticipantIdentity,
  removedCause,
  senderOf,
  ownMetaArgument,
  recipientId,
}

import { assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import type { JoinOptions, LeaveCause, ParticipantMeta, RoomMeta, Sender } from './types.js'
import type { WireLeaveCause } from './protocol.js'

function isRecord(value: unknown): value is Record<string, unknown> {
  if (!isObject(value) || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === null || (Object.getPrototypeOf(prototype) === null && prototype.constructor?.name === 'Object')
}
/** Take ownership of metadata at a state boundary and expose only the immutable owned value. */
function ownMetadata<T extends RoomMeta | ParticipantMeta>(meta: T): T {
  return Object.freeze({ ...meta }) as T
}
const ownLeaveCause = (cause: LeaveCause): LeaveCause => Object.freeze({ ...cause })
/** A detached snapshot of a member, for guards and for senders a view doesn't know. */
function senderOf(id: string, meta: ParticipantMeta, identity: string | null): Sender {
  return Object.freeze({ id, meta, identity })
}
function removedCause(reason: unknown): LeaveCause {
  return ownLeaveCause(reason === undefined ? { type: 'removed' } : { type: 'removed', reason })
}
/** Validate and own a `setMeta()`/`setAttributes()` argument. */
function ownMetaArgument(value: ParticipantMeta, what: string): ParticipantMeta {
  assertUsage(isRecord(value), `${what} should be an object`)
  return ownMetadata(value)
}
function recipientId(to: string | Sender): string {
  const id: unknown = typeof to === 'object' && to !== null ? to.id : to
  assertUsage(typeof id === 'string', 'send() recipient should be a participant or its id')
  return id
}

/** Later timestamp wins; equal timestamps break deterministically by writer ID. */
function stampNewer(a: { at: number; by: string }, b: { at: number; by: string }): boolean {
  return a.at > b.at || (a.at === b.at && a.by > b.by)
}

function leaveCauseFromWire(event: WireLeaveCause): LeaveCause {
  if (event.cause === 'removed') return removedCause(event.reason)
  return ownLeaveCause({ type: event.cause ?? 'left' })
}
function leaveCauseToWire(cause: LeaveCause): WireLeaveCause {
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
function assertParticipantIdentity(identity: unknown, where: string): asserts identity is string {
  assertUsage(
    typeof identity === 'string' && identity.length > 0 && identity.isWellFormed(),
    `${where} should be a non-empty well-formed string`,
  )
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
  if (options?.identity !== undefined) assertParticipantIdentity(options.identity, 'join() options.identity')
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
