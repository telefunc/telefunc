// Room's own encodings read back where they arrive as bytes or parsed text, which no type carries: each reader builds
// its value field by field as it reads, a variant by its tag. A value of another shape asserts, as Room wrote it.
export {
  fieldsOf,
  textOf,
  numberOf,
  metaOf,
  optionalText,
  textListOf,
  leaveCauseOf,
  metaEntryOf,
  memberMetaUpdateOf,
  roomFailureOf,
  ctrlEnvelopeOf,
  dmEnvelopeOf,
}

import { isObject } from 'telefunc/__internal'
import { assert } from '../utils/assert.js'
import { isRecord } from './arguments.js'
import { isRoomErrorCode } from './RoomError.js'
import type {
  MemberMetaUpdate,
  RoomCtrlEnvelope,
  RoomDmEnvelope,
  RoomMetaEntry,
  SenderStamp,
  RoomFailure,
  WireLeaveCause,
} from './protocol.js'
import type { ParticipantMeta } from '../types.js'

function fieldsOf(value: unknown, what: string): Record<string | symbol, unknown> {
  assert(isObject(value), `${what} is not an object`)
  return value
}

function textOf(value: unknown, what: string): string {
  assert(typeof value === 'string', `${what} is not text`)
  return value
}

function numberOf(value: unknown, what: string): number {
  assert(typeof value === 'number', `${what} is not a number`)
  return value
}

/** A list of text, read where it lies. */
function textListOf(value: unknown, what: string): string[] {
  assert(Array.isArray(value) && isTextList(value), `${what} is not a list of text`)
  return value
}

function isTextList(items: unknown[]): items is string[] {
  for (const item of items) if (typeof item !== 'string') return false
  return true
}

function metaOf(value: unknown, what: string): ParticipantMeta {
  assert(isRecord(value), `${what} is not a meta`)
  return value
}

function optionalText(value: unknown, what: string): string | undefined {
  return value === undefined ? undefined : textOf(value, what)
}

/** A leave's cause as `leaveCauseToWire()` writes it: none where the member left on its own. */
function leaveCauseOf(fields: Record<string | symbol, unknown>): WireLeaveCause {
  const { cause } = fields
  const wire: WireLeaveCause = {}
  if (cause !== undefined) {
    assert(cause === 'removed' || cause === 'disconnected' || cause === 'closed' || cause === 'unknown')
    wire.cause = cause
  }
  if ('reason' in fields) wire.reason = fields.reason
  return wire
}

function metaEntryOf(value: unknown): RoomMetaEntry {
  assert(Array.isArray(value) && value.length === 4, 'A meta entry is not four items')
  const [key, entry, placeSeq, placeIndex]: unknown[] = value
  return [textOf(key, 'A meta key'), entry, numberOf(placeSeq, 'A meta place'), numberOf(placeIndex, 'A meta place')]
}

/** A member's meta at `seq`, whole or as a write merged it, or its connection (`RoomWrites.writeMetaInOrder()`). */
function memberMetaUpdateOf(fields: Record<string | symbol, unknown>): MemberMetaUpdate {
  const { seq, meta, attrs, connected } = fields
  const at = numberOf(seq, "A meta update's seq")
  if (meta !== undefined) {
    const whole = metaOf(meta, "A meta update's meta")
    return fields.disconnected === true ? { seq: at, meta: whole, disconnected: true } : { seq: at, meta: whole }
  }
  if (attrs !== undefined) return { seq: at, attrs: metaOf(attrs, "A meta update's attributes") }
  assert(typeof connected === 'boolean', 'A meta update carries neither meta nor a connection')
  return { seq: at, connected }
}

/** A Room answer that is no value, as `toRoomFailure()` makes it. */
function roomFailureOf(fields: Record<string | symbol, unknown>): RoomFailure {
  assert(fields.ok === false, 'A Room answer is neither a value nor a failure')
  const { kind } = fields
  if (kind === 'abort') return { ok: false, kind, abortValue: fields.abortValue }
  const message = textOf(fields.message, "A Room failure's message")
  if (kind === 'error') {
    const { code } = fields
    assert(isRoomErrorCode(code), 'A Room failure has no error code')
    return { ok: false, kind, code, message }
  }
  assert(kind === 'shield' || kind === 'overflow', 'A Room failure is of no known kind')
  return { ok: false, kind, message }
}

/** An event of a room's control lane. */
function ctrlEnvelopeOf(value: unknown): RoomCtrlEnvelope {
  const fields = fieldsOf(value, 'A control event')
  const { __r: tag } = fields
  switch (tag) {
    case 'join': {
      const join: RoomCtrlEnvelope = {
        __r: tag,
        id: textOf(fields.id, "A join's member"),
        meta: metaOf(fields.meta, "A join's meta"),
        joinedAt: numberOf(fields.joinedAt, "A join's time"),
      }
      const identity = optionalText(fields.identity, "A join's identity")
      if (identity !== undefined) join.identity = identity
      if (fields.hidden === true) join.hidden = true
      return join
    }
    case 'leave': {
      const id = textOf(fields.id, "A leave's member")
      return fields.hidden === true
        ? { __r: tag, id, hidden: true, ...leaveCauseOf(fields) }
        : { __r: tag, id, ...leaveCauseOf(fields) }
    }
    case 'p-meta': {
      const id = textOf(fields.id, "A meta event's member")
      return fields.hidden === true
        ? { __r: tag, id, hidden: true, ...memberMetaUpdateOf(fields) }
        : { __r: tag, id, ...memberMetaUpdateOf(fields) }
    }
    case 'update': {
      const seq = numberOf(fields.seq, "A room meta update's seq")
      const { meta, changes } = fields
      if (meta !== undefined) {
        assert(Array.isArray(meta), "A room meta update's meta is not a list")
        return { __r: tag, seq, meta: meta.map(metaEntryOf) }
      }
      assert(Array.isArray(changes), 'A room meta update carries neither meta nor changes')
      return {
        __r: tag,
        seq,
        changes: changes.map((change: unknown): RoomMetaEntry | [key: string] => {
          if (Array.isArray(change) && change.length === 1) return [textOf(change[0], 'A removed meta key')]
          return metaEntryOf(change)
        }),
      }
    }
    case 'demand-ask': {
      const instance = textOf(fields.instance, "A demand ask's instance")
      const of = optionalText(fields.of, "A demand ask's instance asked")
      return of === undefined ? { __r: tag, instance } : { __r: tag, instance, of }
    }
    case 'closed':
      return { __r: tag, ...leaveCauseOf(fields) }
  }
  assert(false, 'A control event of no known kind')
}

/** A DM, as an inbox lane carries it or a recipient's stub relays it. */
function dmEnvelopeOf(fields: Record<string | symbol, unknown>): RoomDmEnvelope {
  const to = textOf(fields.to, "A DM's recipient")
  const from = textOf(fields.from, "A DM's sender")
  const { data, fromMeta, fromMetaSeq } = fields
  let dm: RoomDmEnvelope
  if (fromMetaSeq === undefined) {
    assert(fromMeta === undefined || fromMeta === null, 'An unstamped DM carries a sender meta')
    dm = fromMeta === null ? { __r: 'dm', to, from, data, fromMeta } : { __r: 'dm', to, from, data }
  } else {
    const stamped: RoomDmEnvelope & SenderStamp = {
      __r: 'dm',
      to,
      from,
      data,
      fromMeta: metaOf(fromMeta, "A DM's sender meta"),
      fromMetaSeq: numberOf(fromMetaSeq, "A DM's sender meta seq"),
    }
    const fromIdentity = optionalText(fields.fromIdentity, "A DM's sender identity")
    if (fromIdentity !== undefined) stamped.fromIdentity = fromIdentity
    if (fields.fromDisconnected === true) stamped.fromDisconnected = true
    dm = stamped
  }
  const ackId = optionalText(fields.ackId, "A DM's ack id")
  if (ackId !== undefined) dm.ackId = ackId
  const field = optionalText(fields.field, "A DM's field")
  if (field !== undefined) dm.field = field
  return dm
}
