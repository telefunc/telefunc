export { Router }

import type { Row } from './condition.js'
import type { Change, LiveRead } from './liveRead.js'
import { paramText, valueKey, type Column } from './values.js'

/** The live reads of one table: by the value each requires of a column, and those that require none. */
type TableIndex = {
  byColumn: Map<string, { column: Column; byValue: Map<string, Set<LiveRead>> }>
  unrouted: Set<LiveRead>
}

/** Finds the live reads a change can reach without visiting the others: a read filtering `user_id = 7` is only
 *  looked at for changes to rows whose `user_id` was or is 7. */
class Router {
  private readonly tables = new Map<string, TableIndex>()
  /** Reads that may read any table. */
  private readonly everyTable = new Set<LiveRead>()
  /** Where each read is filed, to remove it (and the index entry it leaves empty). */
  private readonly filings = new Map<
    LiveRead,
    { set: Set<LiveRead>; owner?: Map<string, Set<LiveRead>>; key?: string }[]
  >()

  add(read: LiveRead): void {
    const filings: { set: Set<LiveRead>; owner?: Map<string, Set<LiveRead>>; key?: string }[] = []
    const file = (set: Set<LiveRead>, owner?: Map<string, Set<LiveRead>>, key?: string) => {
      set.add(read)
      filings.push({ set, owner, key })
    }
    const { aliases, alsoReads } = read.shape
    if (alsoReads === 'all') file(this.everyTable)
    else for (const table of alsoReads) file(this.index(table).unrouted)
    for (const alias of aliases) {
      const index = this.index(alias.table.id)
      const route = alias.routes
        .map((r) => ({ column: r.column, keys: this.keysOf(r, read.params) }))
        .find((r) => r.keys)
      if (!route) {
        file(index.unrouted)
        continue
      }
      let byColumn = index.byColumn.get(route.column.name)
      if (!byColumn) {
        byColumn = { column: route.column, byValue: new Map() }
        index.byColumn.set(route.column.name, byColumn)
      }
      for (const key of route.keys!) {
        let set = byColumn.byValue.get(key)
        if (!set) {
          set = new Set()
          byColumn.byValue.set(key, set)
        }
        file(set, byColumn.byValue, key)
      }
    }
    this.filings.set(read, filings)
  }

  remove(read: LiveRead): void {
    for (const { set, owner, key } of this.filings.get(read) ?? []) {
      set.delete(read)
      if (owner && set.size === 0) owner.delete(key!)
    }
    this.filings.delete(read)
  }

  route(change: Change): void {
    for (const read of this.readsReachedBy(change)) read.consider(change)
  }

  private readsReachedBy(change: Change): Set<LiveRead> {
    const reached = new Set(this.everyTable)
    const add = (set: Set<LiveRead> | undefined) => set?.forEach((read) => reached.add(read))
    const indexes = change.table === '*' ? [...this.tables.values()] : [this.tables.get(change.table)]
    for (const index of indexes) {
      if (!index) continue
      add(index.unrouted)
      for (const { column, byValue } of index.byColumn.values()) {
        const keys = rowsOf(change).map((row) => keyOf(row[column.name], column))
        // An unknown value may be any of them.
        if (keys.length === 0 || keys.includes(undefined)) byValue.forEach(add)
        else for (const key of keys) add(byValue.get(key!))
      }
    }
    return reached
  }

  private keysOf(route: LiveRead['shape']['aliases'][number]['routes'][number], params: readonly unknown[]) {
    const texts =
      'anyOf' in route
        ? params[route.anyOf]
        : route.values.map((v) => (v.kind === 'param' ? params[v.index] : v.kind === 'const' ? v.text : undefined))
    if (!Array.isArray(texts)) return undefined
    const keys: string[] = []
    for (const value of texts) {
      const text = paramText(value)
      // `NULL` equals nothing: such a read matches no row.
      if (text === null) continue
      const key = text === undefined ? undefined : valueKey(text, route.column)
      if (key === undefined) return undefined
      keys.push(key)
    }
    return keys
  }

  private index(table: string): TableIndex {
    let index = this.tables.get(table)
    if (!index) {
      index = { byColumn: new Map(), unrouted: new Set() }
      this.tables.set(table, index)
    }
    return index
  }
}

function rowsOf(change: Change): Row[] {
  switch (change.kind) {
    case 'insert':
      return [change.new]
    case 'update':
      return [change.old, change.new]
    case 'delete':
      return [change.old]
    case 'unknown':
      return change.row ? [change.row] : []
  }
}

function keyOf(text: string | null | undefined, column: Column): string | null | undefined {
  if (text === null) return null
  if (text === undefined) return undefined
  return valueKey(text, column)
}
