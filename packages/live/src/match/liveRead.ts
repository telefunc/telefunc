export { LiveRead }
export type { Change }

import { isTrue, mayBeTrue, type Row } from './condition.js'
import { SEED_LIMIT, type Alias, type Join, type ReadShape } from './readShape.js'
import { Tally } from './tally.js'
import { compareValues, paramText, valueKey, type Column } from './values.js'

/** A committed change to a table, with the images of the rows it wrote. `old` may lack columns: an `UPDATE` on
 *  Postgres < 18 only tells which columns it assigned. */
type Change =
  | { table: string; kind: 'insert'; new: Row }
  | { table: string; kind: 'update'; old: Row; new: Row }
  /** `several`: every row matching `old` (the columns it has) may have been deleted, by a foreign key's cascade. */
  | { table: string; kind: 'delete'; old: Row; several?: true }
  /** Any row of `table` may have changed in any way; `*`: any table. */
  | { table: string; kind: 'unknown' }

/** What the read returned, as far as the matcher needs it. */
type Result = {
  /** The page is full: a row sorting after its last row can't enter. */
  full: boolean
  /** Per hidden column, the canonical keys of its values; `undefined` entries can't be keyed. */
  values: Set<string | undefined>[]
  first: (string | null)[] | undefined
  last: (string | null)[] | undefined
  /** Per join seed: the keys of its values, plus those of matching rows written since; `undefined`: unknown. */
  seeds: Map<number, Set<string | undefined> | undefined>
  /** Per `EXISTS` count: how many rows it found for each outer row, by correlation key; it only goes down (a row
   *  counted twice, a change seen twice, can only make it reach 0 early). */
  counts: Map<number, Map<string | undefined, number>>
  /** For a `SELECT DISTINCT`: how many rows have each result row's values, by `tupleKey()`; it only goes down. */
  tuples: Map<string, number>
}

/** One `SELECT` a telefunction ran, while a client watches its result: decides which changes can alter it. */
class LiveRead {
  readonly shape: ReadShape
  readonly params: readonly unknown[]
  private result: Result | undefined
  private tally: Tally | undefined
  /** Changes that arrived while the statement ran: they are judged once its result is known. */
  private pending: Change[] | undefined = []
  private affected = false
  private readonly onAffected: () => void

  constructor(shape: ReadShape, params: readonly unknown[], onAffected: () => void) {
    this.shape = shape
    this.params = params
    this.onAffected = onAffected
  }

  /** The statement's result: its rows' hidden columns as text, one array per row; and the rows of the shape's
   *  `state` query, when it ran. */
  resolve(hiddenRows: (string | null)[][] | undefined, state?: (string | null)[][]): void {
    const { tally } = this.shape
    if (hiddenRows && tally) this.tally = state && Tally.of(tally, this.params, hiddenRows, state)
    else if (hiddenRows) this.result = this.resultOf(hiddenRows, state?.[0])
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
    if (change.kind === 'unknown') return through.length > 0
    return through.some((alias) => this.affectsThrough(alias, change))
  }

  private affectsThrough(alias: Alias, change: Change & { kind: 'insert' | 'update' | 'delete' }): boolean {
    const old = change.kind === 'insert' ? undefined : change.old
    const now = change.kind === 'delete' ? undefined : change.new
    const oldTruth = old && alias.condition(old, this.params)
    const newTruth = now && alias.condition(now, this.params)
    const oldMatches = oldTruth !== undefined && mayBeTrue(oldTruth)
    const newMatches = newTruth !== undefined && mayBeTrue(newTruth)
    // Surely part of the result, were it not for the other tables, the limit and the offset.
    const oldSurely = oldTruth !== undefined && isTrue(oldTruth)
    const newSurely = newTruth !== undefined && isTrue(newTruth)
    if (!oldMatches && !newMatches) return false
    if (old && now && unchanged(alias, old, now)) return false
    const several = change.kind === 'delete' && change.several === true
    const [before, after] = [oldMatches ? old : undefined, newMatches ? now : undefined]
    if (this.tally) return this.tally.changed(alias, before, after, oldSurely && newSurely, several)
    const result = this.result
    if (alias.role === 'other' || !result) return true
    if (newMatches) this.growSeeds(alias, now!)
    if (alias.role === 'child') {
      return alias.correlations.some(
        (c) =>
          (oldMatches && this.inResult(old![c.column.name], c.column, c.hidden!)) ||
          (newMatches && this.inResult(now![c.column.name], c.column, c.hidden!)),
      )
    }
    if (alias.role === 'exists') return this.existsChanged(alias, before, after, newSurely, several)
    if (this.shape.distinct) return this.distinctChanged(alias, before, after, newSurely, several)
    if (oldSurely && newSurely && this.staysOutside(alias, old!, now!)) return false
    if (oldMatches && this.wasInResult(alias, old!)) return true
    if (newMatches && this.mayEnter(alias, now!, change.kind === 'insert')) return true
    // A row leaving from before the page moves the page.
    if (oldMatches && this.shape.offset && this.sortsBefore(alias, old!, result.first)) return true
    return false
  }

  /** A row of a one-table read that surely matches before and after, sorting the same, and wasn't in the result: it
   *  neither enters nor moves the page. */
  private staysOutside(alias: Alias, old: Row, now: Row): boolean {
    const { aliases, sort } = this.shape
    if (aliases.length !== 1 || !sort || this.wasInResult(alias, old)) return false
    return sort.every((key) => old[key.column.name] !== undefined && old[key.column.name] === now[key.column.name])
  }

  private wasInResult(alias: Alias, row: Row): boolean {
    // A joined row was only in the result with a partner in it: an inner join's, or the outer row of a null-extended
    // side.
    for (const { column, hidden } of [...alias.partners, ...alias.joins]) {
      if (hidden !== undefined && !this.inResult(row[column.name], column, hidden)) return false
    }
    if (!alias.primaryKeyHidden) return true
    return alias.table.primaryKey.every((name, i) =>
      this.inResult(row[name], alias.table.column.get(name)!, alias.primaryKeyHidden![i]!),
    )
  }

  private mayEnter(alias: Alias, row: Row, inserted: boolean): boolean {
    const result = this.result!
    if (alias.nullable) {
      // A row of the null-extended side only joins outer rows; every outer row is in an unlimited result.
      if (alias.partners.length === 0 || result.full) return true
      return alias.partners.every((p) => this.inResult(row[p.column.name], p.column, p.hidden!))
    }
    if (!alias.joins.every((join) => this.mayHavePartner(join, row, inserted))) return false
    if (!result.full) return true
    return this.sortsBefore(alias, row, result.last, true)
  }

  private mayHavePartner(join: Join, row: Row, inserted: boolean): boolean {
    const value = row[join.column.name]
    // `NULL` equals nothing; nothing referenced a row before it existed.
    if (value === null || (inserted && join.referenced)) return false
    if (value === undefined || join.seed === undefined) return true
    const seed = this.result!.seeds.get(join.seed)
    return !seed || seed.has(valueKey(value, join.partnerColumn))
  }

  /** A matching row of `alias` may be a partner of rows written later: its value joins the seeds. */
  private growSeeds(alias: Alias, row: Row): void {
    const { seeds } = this.result!
    for (const join of this.shape.aliases.flatMap((a) => a.joins)) {
      if (join.partner !== alias || join.seed === undefined) continue
      const value = row[join.partnerColumn.name]
      if (value === undefined) seeds.set(join.seed, undefined)
      else if (value !== null) seeds.get(join.seed)?.add(valueKey(value, join.partnerColumn))
    }
  }

  /** A row of a `SELECT DISTINCT` (`old` and `now` when they may match it; `several` rows matching `old`): its values
   *  enter when no row had them, leave when it was their last row. */
  private distinctChanged(
    alias: Alias,
    old: Row | undefined,
    now: Row | undefined,
    newSurely: boolean,
    several: boolean,
  ): boolean {
    const { tuples, first } = this.result!
    const before = old && tupleKey(this.shape.distinct!.columns, old)
    const after = now && tupleKey(this.shape.distinct!.columns, now)
    if (now && (after === undefined || !tuples.has(after)) && this.mayEnter(alias, now, false)) return true
    if (!old) return false
    if (before === undefined) return true
    if (newSurely && before === after) return false
    // Values before the page, whose last row may have left.
    if (!tuples.has(before)) return this.shape.offset !== undefined && this.sortsBefore(alias, old, first)
    if (several) return true
    const left = tuples.get(before)! - 1
    tuples.set(before, left)
    return left <= 0
  }

  /** A row of an `EXISTS` subquery (`old` and `now` when they may match it; `several` rows matching `old`): its outer
   *  row may enter when it is the first found, leave when it was the last. */
  private existsChanged(
    alias: Alias,
    old: Row | undefined,
    now: Row | undefined,
    newSurely: boolean,
    several: boolean,
  ): boolean {
    const { column, hidden } = alias.correlations[0]!
    const before = old?.[column.name]
    const after = now?.[column.name]
    if (old && before === undefined) return true
    if (now && after !== null && !this.inResult(after, column, hidden!)) return true
    if (!old || before === null || (newSurely && before === after) || !this.inResult(before, column, hidden!))
      return false
    if (several) return true
    const counts = this.result!.counts.get(alias.existsCountHidden!)!
    const key = valueKey(before!, column)
    const left = (counts.get(key) ?? 1) - 1
    counts.set(key, left)
    return key === undefined || left <= 0
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

  /** `seedRow`: the seeds, when no row carries them. */
  private resultOf(rows: (string | null)[][], seedRow: (string | null)[] | undefined): Result {
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
    const seeds: Result['seeds'] = new Map()
    const counts: Result['counts'] = new Map()
    for (const alias of this.shape.aliases) {
      for (const join of alias.joins) {
        if (join.seed !== undefined)
          seeds.set(join.seed, seedKeys((rows[0] ?? seedRow)?.[join.seed], join.partnerColumn))
      }
      if (alias.existsCountHidden === undefined) continue
      const { parentColumn, hidden } = alias.correlations[0]!
      const found = new Map<string | undefined, number>()
      for (const row of rows) {
        const parent = row[hidden!]
        if (parent != null) found.set(valueKey(parent, parentColumn), Number(row[alias.existsCountHidden]))
      }
      counts.set(alias.existsCountHidden, found)
    }
    const tuples = new Map<string, number>()
    const { distinct } = this.shape
    for (const row of distinct ? rows : []) {
      const key = tupleKey(
        distinct!.columns,
        Object.fromEntries(distinct!.columns.map((c, i) => [c.name, row[distinct!.hidden[i]!]!])),
      )
      if (key !== undefined) tuples.set(key, Number(row[distinct!.countHidden]))
    }
    return { full, values, first: sortTexts(rows[0]), last: sortTexts(rows.at(-1)), seeds, counts, tuples }
  }
}

/** A key for a row's values of `columns`; `undefined` when one is missing or can't be keyed. */
function tupleKey(columns: Column[], row: Row): string | undefined {
  const keys: (string | null)[] = []
  for (const column of columns) {
    const value = row[column.name]
    const key = value === null ? null : value === undefined ? undefined : valueKey(value, column)
    if (key === undefined) return undefined
    keys.push(key)
  }
  return JSON.stringify(keys)
}

/** A seed column's JSON array as keys; `undefined` when there was no row to carry it, or too many values. */
function seedKeys(text: string | null | undefined, column: Column): Set<string | undefined> | undefined {
  if (typeof text !== 'string') return undefined
  const values = JSON.parse(text) as string[]
  return values.length > SEED_LIMIT ? undefined : new Set(values.map((v) => valueKey(v, column)))
}

/** The column each hidden column reads, to key its values. */
function hiddenColumnTypes(shape: ReadShape): (Column | undefined)[] {
  const columns: (Column | undefined)[] = shape.hiddenColumns.map(() => undefined)
  for (const alias of shape.aliases) {
    alias.primaryKeyHidden?.forEach((hidden, i) => {
      columns[hidden] = alias.table.column.get(alias.table.primaryKey[i]!)
    })
    for (const c of [...alias.correlations, ...alias.partners]) columns[c.hidden!] = c.parentColumn
    for (const join of alias.joins) if (join.hidden !== undefined) columns[join.hidden] = join.partnerColumn
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
