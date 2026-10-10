export {
  assertKnownOptions,
  isRecord,
  ownMessage,
  ownMetadata,
  ownLeaveCause,
  mergeAttributes,
  normalizeJoinOptions,
  assertParticipantIdentity,
  assertRoomTimeout,
  removedCause,
  senderOf,
  ownMetaArgument,
  recipientId,
}

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import { TIMER_DELAY_MAX_MS } from '../constants.js'
import type { JoinOptions, LeaveCause, ParticipantMeta, RoomMeta, Sender } from './types.js'

function isRecord(value: unknown): value is Record<string, unknown> {
  if (!isObject(value) || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === null || (Object.getPrototypeOf(prototype) === null && prototype.constructor?.name === 'Object')
}
/** A value as it is now, detached from the caller's object: what its receivers decode. */
function ownMessage<T>(value: T): T {
  return parse(stringify(value)) as T
}
/** Take ownership of metadata at a state boundary and expose only the owned value, frozen at its top. */
function ownMetadata<T extends RoomMeta | ParticipantMeta>(meta: T): T {
  return Object.freeze(ownMessage(meta))
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

/** Merge `attrs` into `meta` per key, returning a new object (the `setAttributes()` semantics). A value of `undefined` deletes its key (the serializer preserves `undefined` on the wire). */
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
/** Milliseconds a timer can wait, or `Infinity` for never. */
function assertRoomTimeout(value: unknown, what: string): asserts value is number {
  assertUsage(
    value === Infinity ||
      (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 && value <= TIMER_DELAY_MAX_MS),
    `\`${what}\` should be a non-negative safe integer of milliseconds, at most ${TIMER_DELAY_MAX_MS}, the longest a timer waits, or Infinity`,
  )
}
/** An option the call doesn't have, a misspelled one included, would otherwise be ignored silently. */
function assertKnownOptions(options: object | null | undefined, known: readonly string[], what: string): void {
  for (const key of Object.keys(options ?? {})) assertUsage(known.includes(key), `Unknown ${what} option: ${key}`)
}
/** Validates `join(options)` and resolves each option's default. */
function normalizeJoinOptions(options: JoinOptions | undefined): {
  meta: ParticipantMeta
  selfDelivery: boolean
  identity: string | null
  hidden: boolean
} {
  assertUsage(options === undefined || isRecord(options), 'join() options should be an object')
  assertKnownOptions(options, ['meta', 'selfDelivery', 'identity', 'hidden'], 'join()')
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
