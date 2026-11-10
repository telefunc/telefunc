export { LiveRead }
export type { Change }

import { mayBeTrue, type Row } from './condition.js'
import type { Alias, ReadShape } from './readShape.js'
import { compareValues, paramText, valueKey, type Column } from './values.js'

/** A committed change to a table, with the images of the rows it wrote. `old` may lack columns: an `UPDATE` on
 *  Postgres < 18 only tells which columns it assigned. */
type Change =
  | { table: string; kind: 'insert'; new: Row }
  | { table: string; kind: 'update'; old: Row; new: Row }
  | { table: string; kind: 'delete'; old: Row }
  /** Rows of `table` may have changed in any way: those matching `row` (the columns it has), or any row; `*`: any
   *  table. */
  | { table: string; kind: 'unknown'; row?: Row }

/** What the read returned, as far as the matcher needs it. */
type Result = {
  /** The page is full: a row sorting after its last row can't enter. */
  full: boolean
  /** Per hidden column, the canonical keys of its values; `undefined` entries can't be keyed. */
  values: Set<string | undefined>[]
  first: (string | null)[] | undefined
  last: (string | null)[] | undefined
}

/** One `SELECT` a telefunction ran, while a client watches its result: decides which changes can alter it. */
class LiveRead {
  readonly shape: ReadShape
  readonly params: readonly unknown[]
  private result: Result | undefined
  /** Changes that arrived while the statement ran: they are judged once its result is known. */
  private pending: Change[] | undefined = []
  private affected = false
  private readonly onAffected: () => void

  constructor(shape: ReadShape, params: readonly unknown[], onAffected: () => void) {
    this.shape = shape
    this.params = params
    this.onAffected = onAffected
  }

  /** The statement's result: its rows' hidden columns as text, one array per row. */
  resolve(hiddenRows: (string | null)[][] | undefined): void {
    if (hiddenRows) this.result = this.resultOf(hiddenRows)
    const pending = this.pending!
    this.pending = undefined
    for (const change of pending) this.consider(change)
  }

  consider(change: Change): void {
    if (this.affected) return
    if (this.pending) {
      this.pending.push(change)
      return
    }
    if (!this.affects(change)) return
    this.affected = true
    this.onAffected()
  }

  /** Whether `change` can alter this read's result; `true` whenever that can't be ruled out. */
  affects(change: Change): boolean {
    const { alsoReads, aliases } = this.shape
    if (change.table === '*' || alsoReads === 'all' || alsoReads.has(change.table)) return true
    const through = aliases.filter((alias) => alias.table.id === change.table)
    if (change.kind !== 'unknown') return through.some((alias) => this.affectsThrough(alias, change))
    // Rows matching `row` may have been deleted, changed or inserted: as if each were its old and new image.
    const { row } = change
    return through.some(
      (alias) => !row || this.affectsThrough(alias, { table: change.table, kind: 'update', old: row, new: row }, true),
    )
  }

  private affectsThrough(
    alias: Alias,
    change: Change & { kind: 'insert' | 'update' | 'delete' },
    partialRow = false,
  ): boolean {
    const old = change.kind === 'insert' ? undefined : change.old
    const now = change.kind === 'delete' ? undefined : change.new
    const oldMatches = old !== undefined && mayBeTrue(alias.condition(old, this.params))
    const newMatches = now !== undefined && mayBeTrue(alias.condition(now, this.params))
    if (!oldMatches && !newMatches) return false
    if (old && now && !partialRow && unchanged(alias, old, now)) return false
    const result = this.result
    if (alias.role === 'other' || !result) return true
    if (alias.role === 'child') {
      return alias.correlations.some(
        (c) =>
          (oldMatches && this.inResult(old![c.column.name], c.column, c.hidden!)) ||
          (newMatches && this.inResult(now![c.column.name], c.column, c.hidden!)),
      )
    }
    if (oldMatches && this.wasInResult(alias, old!)) return true
    if (newMatches && this.mayEnter(alias, now!)) return true
    // A row leaving from before the page moves the page.
    if (oldMatches && this.shape.offset && this.sortsBefore(alias, old!, result.first)) return true
    return false
  }

  private wasInResult(alias: Alias, row: Row): boolean {
    // A row of the null-extended side was only in the result joined to an outer row in it.
    if (alias.partners.some((p) => !this.inResult(row[p.column.name], p.column, p.hidden!))) return false
    if (!alias.primaryKeyHidden) return true
    return alias.table.primaryKey.every((name, i) =>
      this.inResult(row[name], alias.table.column.get(name)!, alias.primaryKeyHidden![i]!),
    )
  }

  private mayEnter(alias: Alias, row: Row): boolean {
    const result = this.result!
    if (alias.nullable) {
      // A row of the null-extended side only joins outer rows; every outer row is in an unlimited result.
      if (alias.partners.length === 0 || result.full) return true
      return alias.partners.every((p) => this.inResult(row[p.column.name], p.column, p.hidden!))
    }
    // An inner join: its partner rows may be missing from the result for lack of this row.
    if (this.shape.aliases.some((a) => a !== alias && a.role === 'top' && !a.nullable)) return true
    if (!result.full) return true
    return this.sortsBefore(alias, row, result.last, true)
  }

  /** Whether `row` sorts before the boundary row (or ties with it, when `orEqual`). Unknown order counts as before. */
  private sortsBefore(alias: Alias, row: Row, boundary: (string | null)[] | undefined, orEqual = false): boolean {
    const keys = this.shape.sort
    if (!keys || !boundary || keys.some((k) => k.alias !== alias)) return true
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i]!
      const a = row[key.column.name]
      const b = boundary[i]
      if (a === undefined || b === undefined) return true
      let order: number
      if (a === null || b === null) order = a === b ? 0 : (a === null) === key.nullsFirst ? -1 : 1
      else {
        const compared = compareValues(a, b, key.column)
        if (compared === undefined) return true
        order = key.descending ? -compared : compared
      }
      if (order !== 0) return order < 0
    }
    return orEqual
  }

  private inResult(text: string | null | undefined, column: Column, hidden: number): boolean {
    if (text === undefined) return true
    // `NULL` joins nothing.
    if (text === null) return false
    const key = valueKey(text, column)
    const values = this.result!.values[hidden]!
    return key === undefined || values.has(undefined) || values.has(key)
  }

  private resultOf(rows: (string | null)[][]): Result {
    const { hiddenColumns, sort, limit } = this.shape
    const columns = hiddenColumnTypes(this.shape)
    const values = hiddenColumns.map((_, i) => {
      const keys = new Set<string | undefined>()
      for (const row of rows) {
        const text = row[i]
        if (text !== null && text !== undefined) keys.add(columns[i] ? valueKey(text, columns[i]!) : undefined)
      }
      return keys
    })
    const limitText =
      limit?.kind === 'param' ? paramText(this.params[limit.index]) : limit?.kind === 'const' ? limit.text : undefined
    const full =
      limit !== undefined && (limitText === undefined || limitText === null || rows.length >= Number(limitText))
    const sortTexts = (row: (string | null)[] | undefined) =>
      row && sort ? sort.map((k) => row[k.hidden]!) : undefined
    return { full, values, first: sortTexts(rows[0]), last: sortTexts(rows.at(-1)) }
  }
}

/** The column each hidden column reads, to key its values. */
function hiddenColumnTypes(shape: ReadShape): (Column | undefined)[] {
  const columns: (Column | undefined)[] = shape.hiddenColumns.map(() => undefined)
  for (const alias of shape.aliases) {
    alias.primaryKeyHidden?.forEach((hidden, i) => {
      columns[hidden] = alias.table.column.get(alias.table.primaryKey[i]!)
    })
    for (const c of [...alias.correlations, ...alias.partners]) columns[c.hidden!] = c.parentColumn
  }
  for (const key of shape.sort ?? []) columns[key.hidden] = key.column
  return columns
}

/** Nothing the statement uses changed. */
function unchanged(alias: Alias, old: Row, now: Row): boolean {
  const columns = alias.referenced === 'all' ? alias.table.columns.map((c) => c.name) : alias.referenced
  for (const name of columns) {
    const before = old[name]
    if (before === undefined || before !== now[name]) return false
  }
  return true
}
