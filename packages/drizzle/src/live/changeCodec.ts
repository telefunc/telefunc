// The wire format for a change batch. The transport carries an OPAQUE string, not live JS objects, so a
// serializing adapter (Redis-class) cannot silently corrupt the values a precise row delta is made of.
//
// Change rows carry ordinary SQL values — BigInt ids, Date timestamps, byte arrays, NULL, composite keys —
// and plain `JSON` destroys most of them (it throws on BigInt and turns a Date into a string). Making that
// the adapter author's problem is how you get a transport that "works" until the first `bigserial` column.
// Encoding here instead narrows every adapter's job to delivering a string.
//
// VERSIONED, and VALIDATED on the way in — every change, field by field. A payload from an unknown version,
// one that does not parse, or one that parses into a shape that is not exactly a batch of TableChanges is
// never guessed at. All of those fail toward COARSE: the receiver invalidates its watched graphs without
// applying a row, because over-fetching is sound and a fabricated row is not. The half-valid payload is the
// dangerous one, not the obviously-broken one — a change missing its `table` would route to nothing and
// silently MISS an invalidation, which is precisely what this codec exists to prevent.
//
// Byte arrays are the one type the underlying serializer does not carry (verified: BigInt, Date, NaN, the
// infinities, `undefined` and `null` all survive it; `Uint8Array` and `Buffer` come back as plain objects),
// so they get an explicit tagged encoding here — as a STRING TOKEN in the serializer's own namespace, never
// as an object shape. That distinction is load-bearing; see BYTES_TAG.

export { encodeChangePayload, decodeChangePayload, CHANGE_CODEC_VERSION }
export type { ChangeEnvelope }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import type { Row, TableChange } from '../router/events.js'

/** Bumped when the envelope shape changes. A receiver that does not recognise the version coarsens rather
 *  than interpreting a payload it does not understand. */
const CHANGE_CODEC_VERSION = 3

/**
 * What one publication carries.
 *
 * `namespace` is the LOGICAL DATABASE the changes belong to. Without it a shared bus carries every db's row
 * deltas to every other db, and two unrelated databases that happen to share a table name apply each other's
 * rows into precise graphs — a wrong row, not an over-fire. The topic is namespaced too; this field is the
 * receiver-side check for an adapter that fans out more widely than its topics say.
 *
 * `origin` identifies the publishing runtime, so it can drop its own echo — its graphs were fed directly,
 * before publishing. `seq` is that origin's monotonic publication counter, which is what lets the receiver
 * tolerate duplicates and detect reordering/gaps instead of demanding both from every adapter.
 *
 * `coarseAll` is the value-free form: a mutation whose touched tables are unknowable (raw SQL, batch), or a
 * precise batch that could not be encoded.
 *
 * `eraCut` marks the first publication after this origin CHANGED TRANSPORT. It exists because the receiver's
 * deferred baseline (changeSequence.ts) rests on a dichotomy that a transport rotation breaks: a sequence
 * below the first one seen was either published before the receiver was admitted (already in its snapshot)
 * or is still owed to it by an at-least-once adapter. A message published on the PREVIOUS transport is
 * neither — it was published after admission, and it is not deliverable on the transport the receiver is
 * on, so no straggler can ever arrive to correct the bet. A receiver cannot infer this from `seq` alone:
 * first-seeing an origin at seq 2 looks identical whether seq 1 went to another transport or is simply
 * about to arrive. The publisher is the only party that knows a cut happened, so it says so.
 */
type ChangeEnvelopeBase = {
  version: number
  namespace: string
  origin: string
  seq: number
  /** Present only on the first publication of a new transport era. */
  eraCut?: true
}
type ChangeEnvelope = (ChangeEnvelopeBase & { changes: TableChange[] }) | (ChangeEnvelopeBase & { coarseAll: true })

/**
 * The byte-array token, deliberately inside the underlying serializer's `!`-prefixed namespace.
 *
 * An earlier version tagged bytes with an OBJECT shape (`{ __telefunc_live_bytes__: [...] }`) and hunted for
 * it on the way back. That ALIASES USER DATA: a JSON/JSONB column holding an object with that key decoded
 * into a `Uint8Array`, and whatever sibling fields it had were dropped — silent corruption of exactly the
 * row this codec exists to carry faithfully. No object shape can be safe here, because any shape the decoder
 * recognises is one a JSON column can contain.
 *
 * A string token can be safe, because the serializer already solves this for its own types: its last rule
 * escapes ANY ordinary string starting with `!` by prefixing another one. So a user string `'!Bytes:1,2'`
 * goes out as `'!!Bytes:1,2'` and revives to itself, while a token written below goes out raw. A single `!`
 * followed by this tag therefore only ever originates here.
 */
const BYTES_TAG = '!Bytes:'

/** Encode a batch for the wire. Throws only if the value domain defeats the serializer — the caller turns
 *  that into a coarse-all publication rather than dropping the invalidation. */
function encodeChangePayload(envelope: ChangeEnvelope): string {
  return stringify(envelope, {
    replacer(_key, value) {
      // Covers Node's Buffer too (a Uint8Array subclass). Revived as a plain Uint8Array, deliberately NOT a
      // Buffer — the row's consumer compares bytes, and reviving a Node type in a package that may run
      // elsewhere would not survive the environment.
      //
      // `resolved` defaults to true, so this string is emitted AS-IS and skips the `!`-escaping rule that
      // ordinary strings go through. That asymmetry is the anti-collision mechanism: only the encoder can
      // produce a single-`!` token.
      if (value instanceof Uint8Array) return { replacement: BYTES_TAG + Array.from(value).join(',') }
      return undefined
    },
  })
}

/** Decode a payload. Returns `undefined` for anything not understood — an unknown version, malformed text,
 *  or a shape that is not exactly an envelope of well-formed changes — so the caller coarsens instead of
 *  applying guessed rows. */
function decodeChangePayload(payload: string): ChangeEnvelope | undefined {
  let decoded: unknown
  try {
    decoded = parse(payload, {
      reviver: (_path, value) =>
        value.startsWith(BYTES_TAG) ? { replacement: bytesOf(value.slice(BYTES_TAG.length)) } : undefined,
    })
  } catch {
    return undefined
  }
  if (!isEnvelope(decoded)) return undefined
  return decoded.version === CHANGE_CODEC_VERSION ? decoded : undefined
}

/** One byte: decimal digits only, no sign, no exponent, no whitespace — exactly what the encoder writes. */
const BYTE_PATTERN = /^(?:0|[1-9][0-9]*)$/

/** A token's payload back to bytes, GRAMMAR-CHECKED. `Uint8Array.from` is not a validator: it truncates
 *  (`999` → 231) and turns `NaN` into 0, so a mangled token would quietly become a plausible byte string
 *  rather than being rejected. Throwing here reaches decode's catch and coarsens, which is what this
 *  module's contract promises for a payload that arrives mangled.
 *
 *  An empty token is the empty array — `''.split(',')` would otherwise yield `['']`. */
function bytesOf(encoded: string): Uint8Array {
  if (encoded === '') return new Uint8Array()
  const parts = encoded.split(',')
  const bytes = new Uint8Array(parts.length)
  for (let index = 0; index < parts.length; index++) {
    const part = parts[index]!
    if (!BYTE_PATTERN.test(part)) throw new Error(`malformed byte token: ${part}`)
    const byte = Number(part)
    if (byte > 255) throw new Error(`byte out of range: ${part}`)
    bytes[index] = byte
  }
  return bytes
}

function isEnvelope(value: unknown): value is ChangeEnvelope {
  if (!isRecord(value)) return false
  if (typeof value.version !== 'number') return false
  if (typeof value.origin !== 'string' || value.origin === '') return false
  // A blank namespace would collapse every database onto one identity — the exact failure the field exists
  // to prevent — and a non-integer seq would make the ordering comparisons below meaningless.
  if (typeof value.namespace !== 'string' || value.namespace === '') return false
  if (typeof value.seq !== 'number' || !Number.isInteger(value.seq) || value.seq < 1) return false
  // Only the exact marker, never a coerced truthy value: a receiver that took a malformed `eraCut` as absent
  // would bet precise across a cut, which is the one thing the field exists to prevent.
  if (value.eraCut !== undefined && value.eraCut !== true) return false
  if (value.coarseAll === true) return true
  return Array.isArray(value.changes) && value.changes.every(isTableChange)
}

const KINDS = new Set(['insert', 'update', 'delete', 'coarse'])

/** One change, validated field by field. ONE bad entry fails the whole envelope: a partly-valid batch has
 *  no safe partial interpretation, and the coarse fallback covers all of it soundly.
 *
 *  The row requirement is per KIND, not "some row field present". Checking merely that one of the three
 *  arrived accepts `{ kind:'insert', old:{…} }` — which routes to exactly the right graph and then applies
 *  nothing, because an insert is applied from its post-image. A hydrated stateful graph reports
 *  `invalidated: false` for it: a silently MISSED invalidation, produced inside the boundary whose whole job
 *  is to fail closed. So: insert and update need the post-image they are applied from; a delete needs
 *  something identifying the row it retracts. */
function isTableChange(value: unknown): value is TableChange {
  if (!isRecord(value)) return false
  if (typeof value.table !== 'string' || value.table === '') return false
  if (typeof value.kind !== 'string' || !KINDS.has(value.kind)) return false
  for (const field of ['old', 'new', 'key'] as const) {
    if (value[field] !== undefined && !isRow(value[field])) return false
  }
  if (value.kind === 'coarse') return true
  if (value.kind === 'delete') return value.key !== undefined || value.old !== undefined
  return value.new !== undefined // insert / update — both are applied from the post-image
}

/** A row is a plain object of columns. A Date is a fine column VALUE; it is not a row. */
function isRow(value: unknown): value is Row {
  return isRecord(value)
}

/** A plain object — not null, not an array, and not a class instance the serializer revived. */
function isRecord(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}
