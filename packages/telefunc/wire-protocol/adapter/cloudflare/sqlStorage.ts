export { blobColumn, integerColumn, nullableIntegerColumn, nullableTextColumn, textColumn }
export type { DurableStorage, SqlBinding, SqlCursor, SqlRow, SqlStore, SqlValue }

// A Durable Object's SQLite storage, as far as Telefunc's Durable Objects and its extensions' use it. Cloudflare's own
// `ctx.storage` is one as it is. A row's columns are read through the decoders below, which check each value's type as
// they build a record from it: a row holds what the schema stored, never a type its reader names.

/** A column's value, as Cloudflare's SQLite storage gives it. */
type SqlValue = ArrayBuffer | string | number | null
type SqlRow = Record<string, SqlValue>
/** A value a statement binds. */
type SqlBinding = SqlValue | Uint8Array
/** A statement's rows, and how many rows it changed. */
type SqlCursor = { toArray(): SqlRow[]; one(): SqlRow; readonly rowsWritten: number }
type SqlStore = { exec(query: string, ...bindings: SqlBinding[]): SqlCursor }
type DurableStorage = {
  readonly sql: SqlStore
  transactionSync<T>(closure: () => T): T
  setAlarm(scheduledTime: number): Promise<void>
  deleteAlarm(): Promise<void>
}

function textColumn(row: SqlRow, column: string): string {
  const value = row[column]
  if (typeof value === 'string') return value
  throw columnError(column, value, 'text')
}

function nullableTextColumn(row: SqlRow, column: string): string | null {
  const value = row[column]
  if (value === null || typeof value === 'string') return value
  throw columnError(column, value, 'text or null')
}

function integerColumn(row: SqlRow, column: string): number {
  const value = row[column]
  if (typeof value === 'number') return value
  throw columnError(column, value, 'an integer')
}

function nullableIntegerColumn(row: SqlRow, column: string): number | null {
  const value = row[column]
  if (value === null || typeof value === 'number') return value
  throw columnError(column, value, 'an integer or null')
}

function blobColumn(row: SqlRow, column: string): ArrayBuffer {
  const value = row[column]
  if (value instanceof ArrayBuffer) return value
  throw columnError(column, value, 'a blob')
}

function columnError(column: string, value: SqlValue | undefined, expected: string): Error {
  const held = value === undefined ? 'nothing' : value === null ? 'null' : typeof value
  return new Error(`The SQL column '${column}' holds ${held}, not ${expected}`)
}
