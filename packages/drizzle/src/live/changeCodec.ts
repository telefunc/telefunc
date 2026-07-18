// The wire format for a change batch. The transport carries an OPAQUE string, not live JS objects, so a
// serializing adapter (Redis-class) cannot silently corrupt the values a precise row delta is made of.
//
// Change rows carry ordinary SQL values — BigInt ids, Date timestamps, byte arrays, NULL, composite keys —
// and plain `JSON` destroys most of them (it throws on BigInt and turns a Date into a string). Making that
// the adapter author's problem is how you get a transport that "works" until the first `bigserial` column.
// Encoding here instead narrows every adapter's job to delivering a string.
//
// VERSIONED, and validated on the way in: a payload from an unknown version, or one that does not decode,
// is never guessed at. Both directions fail toward COARSE — the receiver invalidates its watched graphs
// without applying a row — because over-fetching is sound and a fabricated row is not.
//
// Byte arrays are the one type the underlying serializer does not carry (verified: BigInt, Date, NaN, the
// infinities, `undefined` and `null` all survive it; `Uint8Array` and `Buffer` come back as plain objects),
// so they get an explicit tagged encoding here.

export { encodeChangePayload, decodeChangePayload, CHANGE_CODEC_VERSION }
export type { ChangeEnvelope }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import type { TableChange } from '../router/events.js'

/** Bumped when the envelope shape changes. A receiver that does not recognise the version coarsens rather
 *  than interpreting a payload it does not understand. */
const CHANGE_CODEC_VERSION = 1

/** What one publication carries. `origin` lets the publishing runtime drop its own echo — its graphs were
 *  fed directly, before publishing. `coarseAll` is the value-free form: a mutation whose touched tables are
 *  unknowable (raw SQL, batch), or a precise batch that could not be encoded. */
type ChangeEnvelope =
  | { version: number; origin: string; changes: TableChange[] }
  | { version: number; origin: string; coarseAll: true }

/** Tag for a byte array, chosen to be inert as an object key and unlikely in user data. */
const BYTES_TAG = '__telefunc_live_bytes__'

/** Encode a batch for the wire. Throws only if the value domain defeats the serializer — the caller turns
 *  that into a coarse-all publication rather than dropping the invalidation. */
function encodeChangePayload(envelope: ChangeEnvelope): string {
  return stringify(envelope, {
    replacer(_key, value) {
      if (value instanceof Uint8Array) {
        // Covers Node's Buffer too (a Uint8Array subclass). `Array.from` keeps it JSON-plain; the reviver
        // rebuilds a Uint8Array, deliberately NOT a Buffer — the row's consumer compares bytes, and
        // reviving a Node type in a browser-capable package would not survive the environment.
        return { replacement: { [BYTES_TAG]: Array.from(value) } }
      }
      return undefined
    },
  })
}

/** Decode a payload. Returns `undefined` for anything not understood — an unknown version, malformed text,
 *  or a shape that is not an envelope — so the caller coarsens instead of applying guessed rows. */
function decodeChangePayload(payload: string): ChangeEnvelope | undefined {
  let decoded: unknown
  try {
    decoded = parse(payload)
  } catch {
    return undefined
  }
  if (!isEnvelope(decoded)) return undefined
  if (decoded.version !== CHANGE_CODEC_VERSION) return undefined
  return { ...decoded, ...('changes' in decoded ? { changes: reviveBytes(decoded.changes) as TableChange[] } : {}) }
}

function isEnvelope(value: unknown): value is ChangeEnvelope {
  if (typeof value !== 'object' || value === null) return false
  const candidate = value as Record<string, unknown>
  if (typeof candidate.version !== 'number' || typeof candidate.origin !== 'string') return false
  if (candidate.coarseAll === true) return true
  return Array.isArray(candidate.changes)
}

/** Rebuild tagged byte arrays anywhere in the decoded structure. Walked here rather than through the
 *  serializer's reviver because the tag is a plain object shape, not a custom scalar token.
 *
 *  Only PLAIN objects and arrays are rebuilt. Recursing into everything would reconstruct class instances
 *  from their own enumerable entries, which silently flattens the very types this codec exists to preserve —
 *  a `Date` has none, so it came back as `{}`. Anything with another prototype is already the value the
 *  serializer revived; leave it alone. */
function reviveBytes(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(reviveBytes)
  if (typeof value !== 'object' || value === null) return value
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null) return value
  const record = value as Record<string, unknown>
  const tagged = record[BYTES_TAG]
  if (Array.isArray(tagged)) return Uint8Array.from(tagged as number[])
  const out: Record<string, unknown> = {}
  for (const [key, inner] of Object.entries(record)) out[key] = reviveBytes(inner)
  return out
}
