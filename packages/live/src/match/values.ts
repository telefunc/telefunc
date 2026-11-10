export { compareValues, valueKey, hasCanonicalText, paramText }
export type { Column }

/** A column as the catalog describes it: how Postgres compares its values. */
type Column = {
  name: string
  /** `pg_type.typcategory`: N numeric, S string, B boolean, D date/time, U user-defined, E enum, ... */
  category: string
  type: string
  /** Text order is reproducible in JavaScript only under the `C` (or `POSIX`) collation. */
  orderableText: boolean
  /** A nondeterministic collation can call different strings equal. */
  deterministic: boolean
}

/** Compares two values in Postgres's text form the way Postgres compares them, or returns `undefined` when this can't
 *  be reproduced exactly (then the caller treats the comparison as possibly true either way). */
function compareValues(a: string, b: string, column: Column): -1 | 0 | 1 | undefined {
  switch (column.category) {
    case 'N':
      return compareNumbers(a, b)
    case 'B':
      return compareOrdered(parseBoolean(a), parseBoolean(b))
    case 'D':
      return compareDateTime(a, b, column.type)
    case 'S':
      if (!column.deterministic) return undefined
      if (a === b) return 0
      return column.orderableText ? compareCodePoints(a, b) : equalityOnly(a, b)
    case 'U':
      if (column.type !== 'uuid') return undefined
      return compareOrdered(a.toLowerCase(), b.toLowerCase())
    case 'E':
      return equalityOnly(a, b)
    default:
      return undefined
  }
}

/** A string equal for two values exactly when Postgres calls them equal (an index key), or `undefined` when that
 *  can't be told from the text. */
function valueKey(text: string, column: Column): string | undefined {
  switch (column.category) {
    case 'N': {
      const m = DECIMAL.exec(text)
      if (m) return isZero(m) ? '0' : `${m[1] === '-' ? '-' : ''}${m[2] || '0'}${m[3] ? `.${m[3]}` : ''}`
      const n = finiteNumber(text)
      return n === undefined ? undefined : String(n)
    }
    case 'B':
      return parseBoolean(text)?.toString()
    case 'D': {
      if (!['timestamptz', 'timestamp', 'date'].includes(column.type)) return undefined
      return microseconds(text, column.type === 'timestamptz', column.type === 'date')?.toString()
    }
    case 'S':
      return column.deterministic ? text : undefined
    case 'U':
      return column.type === 'uuid' ? text.toLowerCase() : undefined
    case 'E':
      return text
    default:
      return undefined
  }
}

/** Whether equal values of `column` always have the same text (`1.0` and `1.00` are equal `numeric`s). */
function hasCanonicalText(column: Column): boolean {
  switch (column.category) {
    case 'N':
      return ['int2', 'int4', 'int8'].includes(column.type)
    case 'S':
      return column.deterministic
    case 'D':
      return ['timestamptz', 'timestamp', 'date'].includes(column.type)
    case 'U':
      return column.type === 'uuid'
    case 'B':
    case 'E':
      return true
    default:
      return false
  }
}

/** The text Postgres receives for a bound parameter, or `undefined` when it isn't a plain scalar. */
function paramText(value: unknown): string | null | undefined {
  if (value === null || value === undefined) return null
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'bigint') return String(value)
  if (typeof value === 'boolean') return value ? 'true' : 'false'
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? undefined : value.toISOString()
  return undefined
}

// Different text means different values only for types whose text form is canonical.
function equalityOnly(a: string, b: string): 0 | undefined {
  return a === b ? 0 : undefined
}

function compareOrdered<T extends string | number | bigint>(
  a: T | undefined,
  b: T | undefined,
): -1 | 0 | 1 | undefined {
  if (a === undefined || b === undefined) return undefined
  return a < b ? -1 : a > b ? 1 : 0
}

function compareCodePoints(a: string, b: string): -1 | 1 {
  // `C` sorts by bytes of UTF-8, which is code point order (JavaScript's `<` compares UTF-16 code units).
  const x = [...a]
  const y = [...b]
  for (let i = 0; i < Math.min(x.length, y.length); i++) {
    const d = x[i]!.codePointAt(0)! - y[i]!.codePointAt(0)!
    if (d !== 0) return d < 0 ? -1 : 1
  }
  return x.length < y.length ? -1 : 1
}

function parseBoolean(text: string): number | undefined {
  const t = text.trim().toLowerCase()
  if (['t', 'true', 'y', 'yes', 'on', '1'].includes(t)) return 1
  if (['f', 'false', 'n', 'no', 'off', '0'].includes(t)) return 0
  return undefined
}

const DECIMAL = /^\s*([+-]?)0*(\d*)(?:\.(\d*?)0*)?\s*$/

function compareNumbers(a: string, b: string): -1 | 0 | 1 | undefined {
  const x = DECIMAL.exec(a)
  const y = DECIMAL.exec(b)
  if (!x || !y) return compareOrdered(finiteNumber(a), finiteNumber(b))
  const signX = isZero(x) ? 0 : x[1] === '-' ? -1 : 1
  const signY = isZero(y) ? 0 : y[1] === '-' ? -1 : 1
  if (signX !== signY) return signX < signY ? -1 : 1
  if (signX === 0) return 0
  const magnitude = compareMagnitudes(x[2]!, x[3] ?? '', y[2]!, y[3] ?? '')
  return (magnitude * signX) as -1 | 0 | 1
}

function isZero(match: RegExpExecArray): boolean {
  return match[2] === '' && (match[3] ?? '') === ''
}

function compareMagnitudes(intX: string, fracX: string, intY: string, fracY: string): -1 | 0 | 1 {
  if (intX.length !== intY.length) return intX.length < intY.length ? -1 : 1
  if (intX !== intY) return intX < intY ? -1 : 1
  const width = Math.max(fracX.length, fracY.length)
  const fx = fracX.padEnd(width, '0')
  const fy = fracY.padEnd(width, '0')
  return fx < fy ? -1 : fx > fy ? 1 : 0
}

function finiteNumber(text: string): number | undefined {
  const n = Number(text)
  return Number.isFinite(n) ? n : undefined
}

// `2026-11-10 08:00:00.5+01`, `2026-11-10T07:00:00.500Z`, `2026-11-10`
const DATE_TIME =
  /^(\d{4,})-(\d\d)-(\d\d)(?:[ T](\d\d):(\d\d)(?::(\d\d)(?:\.(\d{1,6}))?)?)?\s*(Z|[+-]\d\d(?::\d\d){0,2})?$/i

function compareDateTime(a: string, b: string, type: string): -1 | 0 | 1 | undefined {
  // Postgres ignores a zone written into a `timestamp` (without time zone) value, which JavaScript can't tell apart.
  const withZone = type === 'timestamptz'
  if (type !== 'timestamptz' && type !== 'timestamp' && type !== 'date') return undefined
  const dateOnly = type === 'date'
  return compareOrdered(microseconds(a, withZone, dateOnly), microseconds(b, withZone, dateOnly))
}

function microseconds(text: string, withZone: boolean, dateOnly: boolean): bigint | undefined {
  const m = DATE_TIME.exec(text.trim())
  if (!m) return undefined
  const [, year, month, day, hour = '0', minute = '0', second = '0', fraction = '', zone] = m
  if (dateOnly) return BigInt(Date.UTC(Number(year), Number(month) - 1, Number(day)))
  // A value without a zone is read in the session's time zone, which isn't known here.
  if (withZone && zone === undefined) return undefined
  if (!withZone && zone !== undefined && !/^(Z|[+-]00(:00)*)$/i.test(zone)) return undefined
  const utc = Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute), Number(second))
  const micros = BigInt(utc) * BigInt(1000) + BigInt(fraction.padEnd(6, '0'))
  if (!withZone || zone!.toUpperCase() === 'Z') return micros
  const [h = '0', mm = '0', ss = '0'] = zone!.slice(1).split(':')
  const offset = BigInt((Number(h) * 3600 + Number(mm) * 60 + Number(ss)) * 1_000_000)
  return zone![0] === '-' ? micros + offset : micros - offset
}
