export { accessAnswered, accessOf, actionOf, sideEffects, tablesChangedBy, union, ACCESS_QUERY, ROLE_QUERY }
export { ACCESS_ROWS, WRITES_OF_ANY_KIND }
export type { Access, AccessRow, Catalog, Table, TableSet, ForeignKey, Write, Written }

import type { Dialect } from './dialect.js'
import type { Column } from './match/values.js'
import type { RangeVar } from './tree.js'

/** Tables by id (`schema.name`), or every table. */
type TableSet = ReadonlySet<string> | 'all'

/** The kinds of write that fire a table's triggers and rules. */
type Write = 'insert' | 'update' | 'delete' | 'truncate'

/** A table written, by a kind of write (`undefined`: any). */
type Written = readonly [table: string, write: Write | undefined]

/** What a role sees of a table: what it may `SELECT`, the whole table or each of its columns (system columns such as
 *  `xmax` aren't among them), and whether row level security filters the rows it reads, and so those a `RETURNING`
 *  gives it. */
type Access = { readable: 'table' | 'columns' | undefined; rowSecurity: boolean }

type Table = {
  id: string
  /** Quoted, schema-qualified. */
  sqlName: string
  columns: Column[]
  column: Map<string, Column>
  primaryKey: string[]
  isView: boolean
  /** A read of this table also reads these: a view's tables, the tables its row-level security policies read. */
  alsoReads: TableSet
  /** What each kind of write to this table also changes: the tables its triggers write. Every table (`'all'`) when the
   *  catalog can't name them: a rule, a column default or a trigger calling a function of the app's, SQL a trigger
   *  builds at run time, a view's tables. */
  alsoWrites: Record<Write, TableSet>
  /** Foreign keys of other tables referencing this one with an action that changes them (`ON DELETE CASCADE`, ...). */
  referencedBy: ForeignKey[]
  /** Single-column foreign keys checked at the end of each statement: every non-`NULL` `column` value is the
   *  `parentColumn` of a row of `parent`. */
  references: { column: string; parent: string; parentColumn: string }[]
  /** An `UPDATE` can change columns its `SET` list doesn't name (`BEFORE` row triggers, generated columns). */
  updateChangesOtherColumns: boolean
  /** SQLite: a constraint's conflict clause is `REPLACE`, so a write may delete the rows it conflicts with. */
  conflictReplace?: boolean
  /** SQLite: a virtual table, whose writes no trigger sees. */
  virtual?: boolean
}

/** `table.column` references the parent's `parentColumn`; on a parent's delete or key update the child rows are
 *  deleted (`CASCADE`) or set to `NULL` or their default. */
type ForeignKey = { table: string; column?: string; parentColumn?: string; onDelete: string; onUpdate: string }

type Catalog = {
  /** How the matcher reads and writes the database's SQL. */
  dialect: Dialect
  database: string
  /** The role that loaded the catalog, which connections start as, and what it sees of each table, by id. */
  role: string
  access: Map<string, Access>
  serverVersion: number
  tables: Map<string, Table>
  /** User-defined functions: a statement calling one may read any table, and write any table when it's one of
   *  `writingFunctions`. */
  functions: Set<string>
  /** The functions in SQL or a procedural language: a write calling one, or firing a trigger, rule or default that
   *  calls one, may write any table, whatever the function's volatility (a `STABLE` function's body may call a
   *  volatile one). A function in C (`internal` included: `uuid_generate_v4()`) is taken to compute a value. */
  writingFunctions: Set<string>
  /** The volatile `writingFunctions`: a read calling one may write any table. Postgres refuses a write in a `STABLE`
   *  or `IMMUTABLE` function's own body, and a read calling one stays a read. */
  volatileWritingFunctions: Set<string>
  /** The functions `node` calls, with those the defaults of the arguments they leave out call: the caller evaluates
   *  those. */
  calls(node: unknown): string[]
  /** The volatile functions of every schema, built-in ones included (`random()`, `nextval()`, `clock_timestamp()`):
   *  two calls may return different values. */
  volatileFunctions: Set<string>
  /** The `search_path` of the session that loaded the catalog, which unqualified names resolve against while every
   *  session has it; `undefined` once a statement may have changed a session's (`LiveDatabase`). */
  searchPath: string[] | undefined
  /** The types the domains a type name may mean are over, through domains over domains: of the domain of that name in
   *  every schema when it's unqualified; none when no domain has it. */
  domainBases(typeNames: string[]): (readonly [schema: string, name: string])[]
  /** The table a name means; `undefined` when unknown, or when it may mean several (`named()`). */
  find(table: RangeVar): Table | undefined
  /** Every table a name may mean: in any schema, unless qualified or resolved by `searchPath`. */
  named(table: RangeVar): Table[]
  /** SQLite: each connection logs the rows its statements change (`LOGGED_SQLITE_ENGINE`). */
  changeLog?: boolean
}

type AccessRow = [table: string, readable: 'table' | 'columns' | null, rowSecurity: boolean]

/** What the current role sees of each table (`AccessRow`). */
const ACCESS_ROWS = `select coalesce(json_agg(json_build_array(n.nspname || '.' || c.relname,
    case when has_table_privilege(c.oid, 'SELECT') then 'table'
      when not exists (select from pg_attribute a where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
        and not has_column_privilege(c.oid, a.attnum, 'SELECT')) then 'columns' end,
    row_security_active(c.oid))), '[]')
  from pg_class c join pg_namespace n on n.oid = c.relnamespace
  where c.relkind in ('r', 'p', 'v', 'm', 'f') and n.nspname not in ('pg_catalog', 'information_schema') and n.nspname !~ '^pg_toast'`

/** The current role, and what it sees of each table: run as the role it asks about (`row_security_active()` takes
 *  none). */
const ACCESS_QUERY = `select json_build_object('role', current_user, 'access', (${ACCESS_ROWS}))::text`

/** `ACCESS_QUERY`, without what the role sees when it's one of those its parameter lists (a JSON array): `null`. */
const ROLE_QUERY = `select json_build_object('role', current_user, 'access', case when current_user::text in
  (select json_array_elements_text($1::json)) then null else (${ACCESS_ROWS}) end)::text`

const WRITES_OF_ANY_KIND: Write[] = ['insert', 'update', 'delete', 'truncate']

/** What writing `table` changes at once besides its own rows: the tables its triggers and rules write, and the
 *  referencing tables whose foreign key actions change rows. `'all'` when the catalog can't name them. */
function sideEffects(table: Table, write: Write | undefined): Written[] | 'all' {
  const effects: Written[] = []
  for (const kind of write ? [write] : WRITES_OF_ANY_KIND) {
    const also = table.alsoWrites[kind]
    if (also === 'all') return 'all'
    for (const id of also) effects.push([id, undefined])
    for (const key of table.referencedBy) {
      const action = actionOf(key, kind)
      if (action) effects.push([key.table, action])
    }
  }
  return effects
}

/** What a foreign key's action does to the referencing rows when their parent gets `write` (`assigned`: the columns
 *  an update assigns, when known). */
function actionOf(key: ForeignKey, write: Write, assigned?: string[]): Write | undefined {
  if (write === 'delete')
    return key.onDelete === 'c' ? 'delete' : ['n', 'd'].includes(key.onDelete) ? 'update' : undefined
  const keyAssigned = !assigned || !key.parentColumn || assigned.includes(key.parentColumn)
  return write === 'update' && keyAssigned && ['c', 'n', 'd'].includes(key.onUpdate) ? 'update' : undefined
}

/** The tables of `written`, and every table writing them may change: their side effects, then those of writing those,
 *  and so on. */
function tablesChangedBy(written: Written[], catalog: Catalog): TableSet {
  const reached = new Set<string>()
  const seen = new Set<string>()
  const pending = [...written]
  while (pending.length > 0) {
    const [id, write] = pending.pop()!
    const key = `${write}\0${id}`
    if (seen.has(key)) continue
    seen.add(key)
    reached.add(id)
    const table = catalog.tables.get(id)
    if (!table) continue
    const effects = sideEffects(table, write)
    if (effects === 'all') return 'all'
    pending.push(...effects)
  }
  return reached
}

function union(sets: TableSet[]): TableSet {
  if (sets.includes('all')) return 'all'
  return new Set(sets.flatMap((s) => [...s]))
}

/** The role `ACCESS_QUERY` or `ROLE_QUERY` ran as, and what it sees of each table, from its answer. */
function accessAnswered(answer: string): { role: string; access: Map<string, Access> | undefined } {
  const { role, access } = JSON.parse(answer) as { role: string; access: AccessRow[] | null }
  return { role, access: access ? accessOf(access) : undefined }
}

/** `ACCESS_ROWS`' rows by table id. */
function accessOf(rows: AccessRow[]): Map<string, Access> {
  return new Map(
    rows.map(([table, readable, rowSecurity]) => [table, { readable: readable ?? undefined, rowSecurity }]),
  )
}
