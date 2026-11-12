export { planRead, alsoWrites, ofRows, HIDDEN_PREFIX, READS_EVERYTHING }
export type { Plan, ReadPlan, WritePlan, TransactionAction, ChangedTables }

import {
  actionOf,
  sideEffects,
  tablesChangedBy,
  type Catalog,
  type Table,
  type TableSet,
  type Written,
} from '../catalog.js'
import { analyzeRead, type ReadShape } from '../match/readShape.js'
import type { Node } from '../tree.js'
import type { RoleChange, SettingName } from './roles.js'

type TransactionAction = 'begin' | 'commit' | 'rollback' | 'savepoint' | 'release' | 'rollbackTo'

type ReadPlan = {
  kind: 'read'
  shape: ReadShape
  /** The statement with the hidden columns appended to its select list; `undefined` when it has none. */
  liveText: string | undefined
  /** The index of the caller's value each parameter of `liveText` takes, when they aren't the caller's values. */
  liveValues?: number[]
  /** Identical reads may share one run: it calls no volatile function (`Catalog.volatileFunctions`). */
  shareable: boolean
  /** How its role changes: a function of the app's it calls may set it. */
  role?: RoleChange
}

type WritePlan = {
  kind: 'write'
  table: Table
  statement: 'insert' | 'update' | 'delete'
  /** The statement with images of the written rows appended to its `RETURNING` list. */
  text: string
  /** The app's parameter each of `text`'s takes, when they aren't the app's in order (`Dialect.bindParams()`). */
  params?: number[]
  hadReturning: boolean
  /** The write as a driver whose rows come as objects sends it with `readsFirst` (D1's `batch()`): each column of its
   *  own `RETURNING` aliased `keys[i]` by its position, then the hidden ones; `names`: a read of no rows giving those
   *  columns' names as SQLite gives them. */
  positional?: { text: string; params: number[]; keys: string[]; names?: { text: string; params: number[] } }
  /** Which hidden columns the rows carry, in order: row images, and whether an upsert inserted (Postgres < 18, which
   *  has no `old` image). */
  images: ('new' | 'old' | 'inserted')[]
  /** The columns an `UPDATE` (or an upsert's update) assigns. */
  assigned: string[]
  /** A query of ours reading the rows the write will change as they are before it, an `old` image each, sent right
   *  before it in the same transaction; `params`: the write's parameter each of its own takes (`Dialect.bindParams()`).
   *  SQLite's `RETURNING` has no old row. */
  readsFirst?: { text: string; params: number[] }
  /** What the write changes besides the rows it returns and those a cascade deletes by key (`cascadedDeletes()`): what
   *  the table's triggers write and the rows its foreign key actions change, then what those changes change in turn. */
  alsoWrites: ReadonlySet<string>
  /** SQLite: `alsoWrites` changes only with each row the write changes, as its triggers and foreign key actions act
   *  for each row (Postgres also has statement triggers). */
  alsoWritesPerRow?: true
  /** SQLite: a failure undoes all the write changed, since no `OR FAIL` can apply to it. */
  undoneByFailure?: true
}

/** What a statement may have changed: rows of these tables, or anything (`'all'`, schema changes included).
 *  `'unnamed'`: rows only, of tables the catalog can't name (what a rule or a function of the app's writes); the locks
 *  its transaction holds name them (`isUnnamed()`). */
type ChangedTables = TableSet | 'unnamed'

type Plan =
  | ReadPlan
  | WritePlan
  /** `chain`: a `COMMIT` or `ROLLBACK` `AND CHAIN`, which starts the next transaction at once. */
  | { kind: 'transaction'; action: TransactionAction; savepoint?: string; chain?: boolean }
  /** A statement whose changes aren't captured row by row: every listed table may have changed. `changesSession`: it
   *  may also change settings of its session (`set_config()`, `DISCARD`), or those new sessions start with; `role`:
   *  its role among them, or `setConfigs`: the settings its `set_config()` calls set, which may name it. `unread`: the
   *  parser couldn't read it, so it may be a read too. */
  | {
      kind: 'unknownWrite'
      tables: ChangedTables
      schemaChange: boolean
      changesSession?: boolean
      role?: RoleChange
      setConfigs?: SettingName[]
      unread?: true
      /** As `WritePlan.undoneByFailure`. */
      undoneByFailure?: true
    }
  /** `SET`, `RESET`: the settings of its session change (a role, a `search_path`, what a row level security policy
   *  reads). `role`: how its role changes. */
  | { kind: 'session'; role?: RoleChange }
  /** `SET LOCAL` is one too, until its transaction ends. `changesPath`: it sets the `search_path`. */
  | { kind: 'none'; role?: RoleChange; changesPath?: boolean }
  | { kind: 'several'; plans: Plan[] }
  /** A write whose rows its connection's change log holds (SQLite): those of `tables`; `also`: the tables it may
   *  change besides. */
  | { kind: 'logged'; tables: ChangedTables; also: ChangedTables }

const HIDDEN_PREFIX = '__live_'

/** A read of every table, whose result can't be bounded. */
const READS_EVERYTHING: ReadShape = { kind: 'other', aliases: [], alsoReads: 'all', hiddenColumns: [], operands: [] }

function planRead(select: Node, text: string | undefined, catalog: Catalog): ReadPlan {
  const shape = analyzeRead(select, catalog, text)
  const shareable = !catalog.calls(select).some((name) => catalog.volatileFunctions.has(name))
  if (shape.hiddenColumns.length === 0 || !text) return { kind: 'read', shape, liveText: undefined, shareable }
  const { dialect } = catalog
  const splices = shape.hiddenAt ?? [{ from: dialect.topLevelFrom(text), columns: shape.hiddenColumns }]
  if (splices.some((s) => s.from === undefined)) return { kind: 'read', shape, liveText: undefined, shareable }
  let liveText = text
  // From the last: a splice leaves the byte offsets before it as they were.
  for (const { from, columns } of splices.toSorted((a, b) => b.from! - a.from!)) {
    const hidden = columns.map((expression, i) => `${expression} AS "${HIDDEN_PREFIX}${i}"`)
    liveText = dialect.splice(liveText, from!, `, ${hidden.join(', ')} `)
  }
  return { kind: 'read', shape, liveText, shareable }
}

/** What `writes` to `table` change besides their rows: `WritePlan.alsoWrites`. */
function alsoWrites(
  table: Table,
  writes: ('insert' | 'update' | 'delete')[],
  assigned: string[],
  catalog: Catalog,
): TableSet {
  const written: Written[] = []
  for (const write of writes) {
    const also = table.alsoWrites[write]
    if (also === 'all') return 'all'
    for (const id of also) written.push([id, undefined])
    for (const key of table.referencedBy) {
      const child = catalog.tables.get(key.table)!
      if (write === 'delete' && key.onDelete === 'c' && key.column) {
        // Its deleted rows are found by key; what deleting them changes counts as a whole.
        const effects = sideEffects(child, 'delete')
        if (effects === 'all') return 'all'
        written.push(...effects)
        continue
      }
      // A trigger or a generated column may change a key the `SET` list doesn't name.
      const action = actionOf(key, write, table.updateChangesOtherColumns ? undefined : assigned)
      if (action) written.push([child.id, action])
    }
  }
  return tablesChangedBy(written, catalog)
}

/** What a statement that changes rows only may change: `'unnamed'` for every table. */
function ofRows(tables: TableSet): ChangedTables {
  return tables === 'all' ? 'unnamed' : tables
}
