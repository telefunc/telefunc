// The positioned change envelope (final-plan §5.6, audit switch #7; T5.G5). A source emits
// `ChangeBatch`es carrying predecessor linkage (R4) so the router can detect duplicates and
// gaps deterministically; ticket 5 ships the SHAPE + (de)serialization + the heartbeat
// contract, not a live source (tickets 6–7). Serialization round-trips every ordinary SQL
// value (BigInt, Date, byte arrays, NULL, composite PKs) via explicit type tags — a value is
// never silently corrupted — and degrades to a coarse resync ONLY for genuinely unserializable
// payloads (cyclic / function) or a batch past the configurable size bound.

export {
  type Row,
  type TableChange,
  type ChangeBatch,
  type SerializeResult,
  DEFAULT_MAX_BATCH_BYTES,
  serializeBatch,
  deserializeBatch,
}

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

/** A positioned batch. `predecessor` is the position of the immediately-preceding frame on
 *  this source (`null` for the first) — CDC `{ lsn, prevLsn }`, or a per-source/per-writer
 *  contiguous ordinal. `heartbeat` frames carry no changes and advance liveness only. */
type ChangeBatch = {
  sourceId: string
  position: number
  predecessor: number | null
  heartbeat?: boolean
  changes: TableChange[]
}

/** ~1 MB default; a source may configure a different bound. */
const DEFAULT_MAX_BATCH_BYTES = 1_000_000

type SerializeResult =
  | { ok: true; wire: string; bytes: number }
  | { ok: false; reason: 'cyclic' | 'function' | 'oversize' }

/** Encode a batch to a wire string. Returns a typed failure (never throws, never silently
 *  drops) so the router can degrade the batch to a coarse resync. */
function serializeBatch(batch: ChangeBatch, maxBytes: number = DEFAULT_MAX_BATCH_BYTES): SerializeResult {
  let node: unknown
  try {
    node = encode(batch, new Set())
  } catch (error) {
    return { ok: false, reason: (error as EncodeError).reason }
  }
  const wire = JSON.stringify(node)
  const bytes = wire.length
  if (bytes > maxBytes) return { ok: false, reason: 'oversize' }
  return { ok: true, wire, bytes }
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
