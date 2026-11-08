export {
  assertKnownOptions,
  assertPublishOptions,
  isRecord,
  ownMessage,
  ownMetadata,
  ownLeaveCause,
  mergeAttributes,
  normalizeJoinOptions,
  assertParticipantIdentity,
  removedCause,
  unknownLeaveCause,
  senderSnapshot,
  ownMetaArgument,
  recipientId,
}

import { parse } from '@brillout/json-serializer/parse'
import { isObject } from 'telefunc/__internal'
import { stringify } from '@brillout/json-serializer/stringify'
import { assertUsage } from '../utils/assert.js'
import { assignAttributes } from './MemberMeta.js'
import { freezeMeta } from './freezeMeta.js'
import type { JoinOptions, LeaveCause, ParticipantMeta, PublishOptions, RoomMeta, Sender } from '../types.js'

function assertPublishOptions(options: PublishOptions | undefined): void {
  assertKnownOptions(options, ['track', 'lossy', 'retain'], 'publish()')
  for (const name of ['retain', 'lossy'] as const) {
    assertUsage(
      options?.[name] === undefined || typeof options[name] === 'boolean',
      `publish() options.${name} should be a boolean`,
    )
  }
  assertUsage(
    options?.lossy === undefined || options.track !== undefined,
    "publish() options.lossy needs options.track: a message on no track reaches everyone, in the room's order",
  )
}

function isRecord(value: unknown): value is Record<string, unknown> {
  if (!isObject(value) || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === null || (Object.getPrototypeOf(prototype) === null && prototype.constructor?.name === 'Object')
}
/** A value as it is now, detached from the caller's object: what its receivers decode. */
function ownMessage<T>(value: T): T {
  return parse(stringify(value)) as T
}
/** Take ownership of metadata at a state boundary and expose only the owned value, frozen. */
function ownMetadata<T extends RoomMeta | ParticipantMeta>(meta: T): T {
  return freezeMeta(ownMessage(meta))
}
function ownLeaveCause(cause: LeaveCause): LeaveCause {
  return Object.freeze({ ...cause })
}
/** A detached snapshot of a member, for guards and for senders a view doesn't know. */
function senderSnapshot(id: string, meta: ParticipantMeta, identity: string | null): Sender {
  return Object.freeze({ id, meta: freezeMeta(meta), identity })
}
/** The cause of a leave a view didn't follow, once nothing records it. */
function unknownLeaveCause(): LeaveCause {
  return ownLeaveCause({ type: 'unknown' })
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

/** Merge `attrs` into `meta`, returning a new object (the `setAttributes()` semantics). */
function mergeAttributes(meta: ParticipantMeta, attrs: ParticipantMeta): ParticipantMeta {
  const next: ParticipantMeta = { ...meta }
  assignAttributes(next, attrs)
  return freezeMeta(next)
}
function assertParticipantIdentity(identity: unknown, where: string): asserts identity is string {
  assertUsage(
    typeof identity === 'string' && identity.length > 0 && identity.isWellFormed(),
    `${where} should be a non-empty well-formed string`,
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
