// The change envelope + value codec (final-plan §5.6). A ChangeSource emits `ChangeBatch`es —
// ordered, atomic sets of TableChanges from one committed transaction. Ordering and delivery
// reliability are the SOURCE's own concern, so the shared shape carries NO transport position
// (no LSN, no cursor). The codec round-trips every ordinary SQL value (BigInt, Date, byte arrays,
// NULL, composite PKs) via explicit type tags so a value is never silently corrupted, and returns a
// typed failure ONLY for genuinely unserializable payloads (cyclic / function). A wire-based source
// (CDC) uses the codec; the in-process ORM source needs no wire.

export { type Row, type TableChange, type ChangeBatch, type SerializeResult, serializeBatch, deserializeBatch }

type Row = Record<string, unknown>

/** One captured row change. `old` present ⇒ the full old row is inline (RI FULL / ORM
 *  returning); `key` carries the PK for a key-only retraction (RI DEFAULT); `new` is the
 *  full post-image (insert / update). */
type TableChange = {
  table: string
  kind: 'insert' | 'update' | 'delete'
  old?: Row
  new?: Row
  key?: Row
}

/** An ordered, atomic set of changes from one committed transaction. */
type ChangeBatch = { changes: TableChange[] }

type SerializeResult = { ok: true; wire: string; bytes: number } | { ok: false; reason: 'cyclic' | 'function' }

/** Encode a batch to a wire string. Returns a typed failure (never throws, never silently drops) so
 *  a wire-based source can degrade rather than corrupt. */
function serializeBatch(batch: ChangeBatch): SerializeResult {
  let node: unknown
  try {
    node = encode(batch, new Set())
  } catch (error) {
    return { ok: false, reason: (error as EncodeError).reason }
  }
  const wire = JSON.stringify(node)
  return { ok: true, wire, bytes: wire.length }
}

function deserializeBatch(wire: string): ChangeBatch {
  return decode(JSON.parse(wire)) as ChangeBatch
}

// ── Tagged codec ────────────────────────────────────────────────────
// Every node is `[tag, payload]`, so JSON survives the value types SQL produces that plain
// JSON cannot (bigint, Date, byte arrays, undefined) with no delimiter ambiguity.

type EncodeError = { reason: 'cyclic' | 'function' }

function encode(value: unknown, seen: Set<object>): unknown {
  if (value === null) return ['n', null]
  if (value === undefined) return ['u']
  const type = typeof value
  if (type === 'function') throw { reason: 'function' } satisfies EncodeError
  if (type === 'bigint') return ['g', (value as bigint).toString()]
  if (type === 'string' || type === 'number' || type === 'boolean') return ['p', value]
  if (value instanceof Date) return ['d', value.getTime()]
  if (value instanceof Uint8Array) return ['b', [...value]]
  if (Array.isArray(value)) {
    guard(value, seen)
    const out = value.map((item) => encode(item, seen))
    seen.delete(value)
    return ['a', out]
  }
  const record = value as Record<string, unknown>
  guard(record, seen)
  const entries = Object.keys(record).map((key) => [key, encode(record[key], seen)])
  seen.delete(record)
  return ['o', entries]
}

function guard(value: object, seen: Set<object>): void {
  if (seen.has(value)) throw { reason: 'cyclic' } satisfies EncodeError
  seen.add(value)
}

function decode(node: unknown): unknown {
  const [tag, payload] = node as [string, unknown]
  if (tag === 'n') return null
  if (tag === 'u') return undefined
  if (tag === 'p') return payload
  if (tag === 'g') return BigInt(payload as string)
  if (tag === 'd') return new Date(payload as number)
  if (tag === 'b') return new Uint8Array(payload as number[])
  if (tag === 'a') return (payload as unknown[]).map(decode)
  const out: Record<string, unknown> = {}
  for (const [key, child] of payload as Array<[string, unknown]>) out[key] = decode(child)
  return out
}
