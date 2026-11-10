export { Tally }

import { and, COMPARISONS, FALSE, isTrue, mayBeTrue, not, NULL, or, TRUE, type Row, type Truth } from './condition.js'
import type { Alias } from './readShape.js'
import { MAX_TALLY_ROWS, type ColumnAggregate, type Comparison, type SetTree, type TallyShape } from './tallyShape.js'
import type { OperandValue } from './operands.js'
import { compareValues, valueKey, type Column } from './values.js'

/** How many matching rows of `source` have some values (`texts`, the source's columns): at least `lo`, at most `hi`.
 *  A change may be counted already, by the counts query that ran after the read, or not: an insert raises `hi`, a
 *  delete lowers `lo`. A change seen twice only widens the range. */
type Count = { source: number; texts: (string | null)[]; lo: number; hi: number }

/** A group of an aggregate, or an output row of a set operation: its values, its counts by source and the other
 *  columns, and its hidden columns when the read returned it. */
type Group = { texts: (string | null)[]; counts: Map<string, Count>; shown?: (string | null)[] }

/** Where a row is counted. */
type Place = { group: string; entry: string; source: number; texts: (string | null)[] }

const UNKNOWN = TRUE | FALSE | NULL

/** The counts of a tally read, and what the read returned: decides whether a change can alter its result. */
class Tally {
  private readonly shape: TallyShape
  private readonly operands: readonly OperandValue[]
  private readonly groups = new Map<string, Group>()

  /** `undefined` when the counts can't be keyed, or there are too many to keep. */
  static of(
    shape: TallyShape,
    operands: readonly OperandValue[],
    hiddenRows: (string | null)[][],
    countRows: (string | null)[][],
  ): Tally | undefined {
    if (countRows.length > MAX_TALLY_ROWS) return undefined
    const tally = new Tally(shape, operands)
    for (const [json, count] of countRows) {
      const [source, ...texts] = JSON.parse(json!) as [string, ...(string | null)[]]
      const place = tally.placeOf(Number(source), texts)
      if (!place) return undefined
      const counted = tally.countAt(place)
      counted.lo += Number(count)
      counted.hi += Number(count)
    }
    const { columns, groupColumns } = shape.sources[0]!
    const shownHidden = shape.rule.kind === 'set' ? columns.map((_, i) => i) : shape.rule.groupHidden
    for (const row of hiddenRows) {
      const texts = shownHidden.map((i) => row[i]!)
      const key = keyOf(columns.slice(0, groupColumns), texts)
      if (key === undefined) return undefined
      tally.groupAt(key, texts).shown = row
    }
    return tally
  }

  private constructor(shape: TallyShape, operands: readonly OperandValue[]) {
    this.shape = shape
    this.operands = operands
  }

  /** Counts a change to a row of `alias`: `old` and `now` when they may match, `surely` when both surely do; `several`
   *  when every row matching `old` (the columns it has) may be gone. Whether the result can differ from what the read
   *  returned. */
  changed(alias: Alias, old: Row | undefined, now: Row | undefined, surely: boolean, several: boolean): boolean {
    const touched: string[] = []
    for (const [source, { alias: sourceAlias, columns }] of this.shape.sources.entries()) {
      if (sourceAlias !== alias) continue
      if (several) {
        touched.push(...this.dropAll(source, old!))
        continue
      }
      const before =
        old &&
        this.placeOf(
          source,
          columns.map((c) => old[c.name]),
        )
      const after =
        now &&
        this.placeOf(
          source,
          columns.map((c) => now[c.name]),
        )
      if ((old && !before) || (now && !after)) return true
      // Counted where it was.
      if (surely && before && after && before.group === after.group && before.entry === after.entry) continue
      if (before) {
        const counted = this.countAt(before)
        counted.lo = Math.max(0, counted.lo - 1)
        touched.push(before.group)
      }
      if (after) {
        this.countAt(after).hi++
        touched.push(after.group)
      }
    }
    return touched.some((group) => this.mayDiffer(this.groups.get(group)!))
  }

  /** Every count of `source` whose values `row` may have can be down to 0; returns the groups `row` may be in. */
  private dropAll(source: number, row: Row): string[] {
    const { columns, groupColumns } = this.shape.sources[source]!
    const touched: string[] = []
    for (const [key, group] of this.groups) {
      if (!mayHave(row, columns.slice(0, groupColumns), group.texts)) continue
      touched.push(key)
      for (const count of group.counts.values()) {
        const rest = count.texts.slice(groupColumns)
        if (count.source === source && mayHave(row, columns.slice(groupColumns), rest)) count.lo = 0
      }
    }
    return touched
  }

  private mayDiffer({ counts: byEntry, shown }: Group): boolean {
    const counts = [...byEntry.values()]
    const { rule } = this.shape
    const differs = (present: Truth) => (shown ? !isTrue(present) : mayBeTrue(present))
    if (rule.kind === 'set') return differs(member(rule.tree, counts))
    let present = rule.grouped ? presence(total(counts)) : TRUE
    for (const test of rule.having) present = and(present, this.holds(test, counts))
    if (differs(present)) return true
    if (!shown) return false
    if (rule.otherOutputs) return true
    return rule.outputs.some(({ aggregate, hidden }) => !this.surelyEquals(aggregate, counts, shown[hidden]!))
  }

  private holds(test: Comparison, counts: Count[]): Truth {
    const { aggregate, operator, operand } = test
    const text = this.operands[operand]
    if (text === null) return NULL
    if (typeof text !== 'string') return UNKNOWN
    const holds = (order: number) => (COMPARISONS[operator]!(order as -1 | 0 | 1) ? TRUE : FALSE)
    if (aggregate.kind === 'count' || aggregate.kind === 'countDistinct') {
      const { lo, hi } = aggregate.kind === 'count' ? total(counts) : this.distinct(aggregate.column, counts)
      const n = Number(text)
      if (!Number.isFinite(n)) return UNKNOWN
      // Every count in between compares the same way when both ends do.
      return Math.sign(lo - n) === Math.sign(hi - n) ? holds(Math.sign(lo - n)) : TRUE | FALSE
    }
    const extreme = this.extreme(aggregate, counts)
    if (!extreme) return UNKNOWN
    if (extreme.value === null) return NULL
    const order = compareValues(extreme.value, text, this.columnOf(aggregate))
    return order === undefined ? TRUE | FALSE : holds(order)
  }

  private surelyEquals(aggregate: ColumnAggregate, counts: Count[], shown: string | null): boolean {
    if (aggregate.kind === 'countDistinct') {
      const { lo, hi } = this.distinct(aggregate.column, counts)
      return lo === hi && String(lo) === shown
    }
    const extreme = this.extreme(aggregate, counts)
    if (!extreme || extreme.value === null || shown === null) return extreme?.value === shown
    return compareValues(extreme.value, shown, this.columnOf(aggregate)) === 0
  }

  /** The `min` or `max`, when it can only be one value. */
  private extreme(aggregate: ColumnAggregate, counts: Count[]): { value: string | null } | undefined {
    const column = this.columnOf(aggregate)
    const possible = [...this.values(aggregate.column, counts).values()].filter((v) => v.hi > 0)
    const certain = possible.filter((v) => v.lo > 0)
    if (certain.length === 0) return possible.length === 0 ? { value: null } : undefined
    const sign = aggregate.kind === 'max' ? 1 : -1
    const beats = (a: string, b: string) => {
      const order = compareValues(a, b, column)
      return order === undefined ? undefined : order * sign > 0
    }
    let best = certain[0]!
    for (const v of certain) {
      const better = beats(v.text, best.text)
      if (better === undefined) return undefined
      if (better) best = v
    }
    // A value that may still be there and would win leaves it open.
    if (possible.some((v) => v !== best && beats(v.text, best.text) !== false)) return undefined
    return { value: best.text }
  }

  /** How many distinct values `count(DISTINCT)` may count. */
  private distinct(column: number, counts: Count[]): { lo: number; hi: number } {
    const values = [...this.values(column, counts).values()]
    return { lo: values.filter((v) => v.lo > 0).length, hi: values.filter((v) => v.hi > 0).length }
  }

  /** The non-`NULL` values of `column` in a group, by key, with their counts. */
  private values(column: number, counts: Count[]): Map<string, { text: string; lo: number; hi: number }> {
    const values = new Map<string, { text: string; lo: number; hi: number }>()
    const type = this.shape.sources[0]!.columns[column]!
    for (const { texts, lo, hi } of counts) {
      const text = texts[column]
      if (text === null || text === undefined) continue
      const key = valueKey(text, type)!
      const value = values.get(key) ?? { text, lo: 0, hi: 0 }
      value.lo += lo
      value.hi += hi
      values.set(key, value)
    }
    return values
  }

  private columnOf(aggregate: ColumnAggregate): Column {
    return this.shape.sources[0]!.columns[aggregate.column]!
  }

  private placeOf(source: number, texts: (string | null | undefined)[]): Place | undefined {
    if (texts.some((t) => t === undefined)) return undefined
    const { columns, groupColumns } = this.shape.sources[source]!
    const group = keyOf(columns.slice(0, groupColumns), texts.slice(0, groupColumns) as (string | null)[])
    const rest = keyOf(columns.slice(groupColumns), texts.slice(groupColumns) as (string | null)[])
    if (group === undefined || rest === undefined) return undefined
    return { group, entry: `${source}:${rest}`, source, texts: texts as (string | null)[] }
  }

  private groupAt(key: string, texts: (string | null)[]): Group {
    let group = this.groups.get(key)
    if (!group) {
      group = { texts, counts: new Map() }
      this.groups.set(key, group)
    }
    return group
  }

  private countAt({ group, entry, source, texts }: Place): Count {
    const { counts } = this.groupAt(group, texts.slice(0, this.shape.sources[source]!.groupColumns))
    let count = counts.get(entry)
    if (!count) {
      count = { source, texts, lo: 0, hi: 0 }
      counts.set(entry, count)
    }
    return count
  }
}

/** Whether a set operation's output row is in its result. */
function member(tree: SetTree, counts: Count[]): Truth {
  if (typeof tree === 'number') return presence(total(counts.filter((c) => c.source === tree)))
  const left = member(tree.left, counts)
  const right = member(tree.right, counts)
  if (tree.op === 'union') return or(left, right)
  return and(left, tree.op === 'intersect' ? right : not(right))
}

function presence({ lo, hi }: { lo: number; hi: number }): Truth {
  return lo > 0 ? TRUE : hi === 0 ? FALSE : TRUE | FALSE
}

function total(counts: Count[]): { lo: number; hi: number } {
  return counts.reduce((sum, c) => ({ lo: sum.lo + c.lo, hi: sum.hi + c.hi }), { lo: 0, hi: 0 })
}

/** Whether `row`, of which some columns may be unknown, may have the values `texts` of `columns`. */
function mayHave(row: Row, columns: Column[], texts: (string | null)[]): boolean {
  return columns.every((column, i) => {
    const [a, b] = [row[column.name], texts[i]!]
    if (a === undefined) return true
    return a === null || b === null ? a === b : valueKey(a, column) === valueKey(b, column)
  })
}

/** A key equal for two rows exactly when Postgres calls their values equal; `undefined` when it can't be told. */
function keyOf(columns: Column[], texts: (string | null)[]): string | undefined {
  const keys: (string | null)[] = []
  for (const [i, column] of columns.entries()) {
    const text = texts[i]!
    const key = text === null ? null : valueKey(text, column)
    if (key === undefined) return undefined
    keys.push(key)
  }
  return JSON.stringify(keys)
}
