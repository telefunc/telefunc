// What a test page or telefunction reads, checked as the type it uses where it reads it: a value of another type fails
// there, naming what it read, where a cast would let it through.
export {
  textOf,
  optionalTextOf,
  textListOf,
  numberOf,
  listOf,
  recordOf,
  fieldOf,
  textAt,
  numberAt,
  byteAt,
  present,
  entryOf,
  searchParam,
  messageOf,
  nameOf,
}

function textOf(value: unknown): string {
  if (typeof value !== 'string') throw new Error(`Expected a string, read ${String(value)}`)
  return value
}

function optionalTextOf(value: unknown): string | undefined {
  return value === undefined ? undefined : textOf(value)
}

function textListOf(value: unknown): string[] {
  if (!Array.isArray(value)) throw new Error(`Expected a list, read ${String(value)}`)
  return value.map(textOf)
}

function numberOf(value: unknown): number {
  if (typeof value !== 'number') throw new Error(`Expected a number, read ${String(value)}`)
  return value
}

function listOf(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error(`Expected a list, read ${String(value)}`)
  return [...value]
}

/** A plain object's fields, as a record of its own. */
function recordOf(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error(`Expected an object, read ${String(value)}`)
  }
  return Object.fromEntries(Object.entries(value))
}

/** An object's field `key`. */
function fieldOf(value: unknown, key: string): unknown {
  if (typeof value !== 'object' || value === null || !hasKey(value, key)) {
    throw new Error(`Expected a field ${key}, read ${String(value)}`)
  }
  return value[key]
}

function hasKey<Key extends string>(value: object, key: Key): value is Record<Key, unknown> {
  return key in value
}

function textAt(value: unknown, key: string): string {
  return textOf(fieldOf(value, key))
}

function numberAt(value: unknown, key: string): number {
  return numberOf(fieldOf(value, key))
}

/** The byte at `at` of a frame that has it. */
function byteAt(data: Uint8Array, at = 0): number {
  const byte = data[at]
  if (byte === undefined) throw new Error(`Expected a byte at ${at}, read ${data.length} bytes`)
  return byte
}

function present<T>(value: T | null | undefined, what: string): T {
  if (value === null || value === undefined) throw new Error(`Expected ${what}, read ${String(value)}`)
  return value
}

/** What `map` keeps under `key`, which the caller put there. */
function entryOf<Key, Value>(map: ReadonlyMap<Key, Value>, key: Key): Value {
  const value = map.get(key)
  if (value === undefined) throw new Error(`Nothing is kept under ${String(key)}`)
  return value
}

/** The page URL's search parameter `name`, which the test sets. */
function searchParam(name: string): string {
  return present(new URLSearchParams(location.search).get(name), `the URL's ${name}`)
}

/** What a thrown or rejected value says: an `Error`'s message. */
function messageOf(error: unknown): string {
  if (!(error instanceof Error)) throw new Error(`Expected an Error, read ${String(error)}`)
  return error.message
}

function nameOf(error: unknown): string {
  if (!(error instanceof Error)) throw new Error(`Expected an Error, read ${String(error)}`)
  return error.name
}
