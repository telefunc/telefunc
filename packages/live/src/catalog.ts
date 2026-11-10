export { loadCatalog }
export type { Catalog, Table, TableSet, ForeignKey }

import { parsePlPgSQLSync } from 'libpg-query'
import type { Column } from './match/values.js'
import { parseStatements, tablesIn, walk, type Node, type RangeVar } from './sql.js'

/** Tables by id (`schema.name`), or every table. */
type TableSet = ReadonlySet<string> | 'all'

type Table = {
  id: string
  columns: Column[]
  column: Map<string, Column>
  primaryKey: string[]
  isView: boolean
  /** A read of this table also reads these: a view's tables, the tables its row-level security policies read. */
  alsoReads: TableSet
  /** A write to this table may also change these: what its triggers write. */
  alsoWrites: TableSet
  /** Foreign keys of other tables referencing this one with an action that changes them (`ON DELETE CASCADE`, ...). */
  referencedBy: ForeignKey[]
  /** An `UPDATE` can change columns its `SET` list doesn't name (`BEFORE` row triggers, generated columns). */
  updateChangesOtherColumns: boolean
}

/** `table.column` references the parent's `parentColumn`; on a parent's delete or key update the child rows are
 *  deleted (`CASCADE`) or set to `NULL` or their default. */
type ForeignKey = { table: string; column?: string; parentColumn?: string; onDelete: string; onUpdate: string }

type Catalog = {
  database: string
  serverVersion: number
  tables: Map<string, Table>
  /** User-defined functions: a statement calling one may read any table, and write any table when it's volatile. */
  functions: Set<string>
  volatileFunctions: Set<string>
  find(table: RangeVar): Table | undefined
}

type CatalogRow = {
  database: string
  searchPath: string[]
  serverVersion: number
  cOrdering: boolean
  functions: [name: string, volatile: boolean][]
  tables: {
    schema: string
    name: string
    view: string | null
    rls: boolean
    columns: [
      name: string,
      category: string,
      type: string,
      collation: string,
      deterministic: boolean,
      generated: boolean,
    ][]
    primaryKey: string[] | null
    policies: string[] | null
    triggers: [type: number, language: string, source: string][] | null
    referencedBy:
      | [table: string, columns: string[], parentColumns: string[], onDelete: string, onUpdate: string][]
      | null
  }[]
}

// One round trip: everything the matcher needs to know about the database's tables.
const CATALOG_QUERY = `select json_build_object(
  'database', current_database(),
  'searchPath', current_schemas(false),
  'serverVersion', current_setting('server_version_num')::int,
  'cOrdering', (select datlocprovider in ('c', 'b') and datcollate in ('C', 'POSIX') from pg_database where datname = current_database()),
  'functions', (select coalesce(json_agg(json_build_array(p.proname, p.provolatile = 'v')), '[]') from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname not in ('pg_catalog', 'information_schema') and p.prorettype <> 'trigger'::regtype),
  'tables', (select coalesce(json_agg(json_build_object(
    'schema', n.nspname,
    'name', c.relname,
    'view', case when c.relkind in ('v', 'm') then pg_get_viewdef(c.oid) end,
    'rls', c.relrowsecurity,
    'columns', (select json_agg(json_build_array(a.attname, t.typcategory, t.typname, coalesce(co.collname, ''),
        coalesce(co.collisdeterministic, true), a.attgenerated <> '') order by a.attnum)
      from pg_attribute a join pg_type t on t.oid = a.atttypid left join pg_collation co on co.oid = a.attcollation
      where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped),
    'primaryKey', (select json_agg(a.attname order by k.ord) from pg_index i
      cross join unnest(i.indkey) with ordinality k(attnum, ord)
      join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.attnum
      where i.indrelid = c.oid and i.indisprimary),
    'policies', (select json_agg(concat_ws(' and ', pg_get_expr(p.polqual, p.polrelid), pg_get_expr(p.polwithcheck, p.polrelid)))
      from pg_policy p where p.polrelid = c.oid),
    'triggers', (select json_agg(json_build_array(tg.tgtype, l.lanname, pr.prosrc)) from pg_trigger tg
      join pg_proc pr on pr.oid = tg.tgfoid join pg_language l on l.oid = pr.prolang
      where tg.tgrelid = c.oid and not tg.tgisinternal),
    'referencedBy', (select json_agg(json_build_array(rn.nspname || '.' || rc.relname,
        (select json_agg(a.attname order by k.ord) from unnest(f.conkey) with ordinality k(attnum, ord)
          join pg_attribute a on a.attrelid = f.conrelid and a.attnum = k.attnum),
        (select json_agg(a.attname order by k.ord) from unnest(f.confkey) with ordinality k(attnum, ord)
          join pg_attribute a on a.attrelid = f.confrelid and a.attnum = k.attnum),
        f.confdeltype, f.confupdtype))
      from pg_constraint f join pg_class rc on rc.oid = f.conrelid join pg_namespace rn on rn.oid = rc.relnamespace
      where f.confrelid = c.oid and f.contype = 'f' and (f.confdeltype in ('c', 'n', 'd') or f.confupdtype in ('c', 'n', 'd')))
  )), '[]') from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where c.relkind in ('r', 'p', 'v', 'm', 'f') and n.nspname not in ('pg_catalog', 'information_schema') and n.nspname !~ '^pg_toast')
)::text as catalog`

// `pg_trigger.tgtype` bits
const TRIGGER_ROW = 1
const TRIGGER_BEFORE = 2
const TRIGGER_UPDATE = 16

async function loadCatalog(query: (text: string) => Promise<string>): Promise<Catalog> {
  const row = JSON.parse(await query(CATALOG_QUERY)) as CatalogRow
  const tables = new Map<string, Table>()
  const find = (table: RangeVar) => {
    if (table.schemaname) return tables.get(`${table.schemaname}.${table.relname}`)
    for (const schema of row.searchPath) {
      const found = tables.get(`${schema}.${table.relname}`)
      if (found) return found
    }
    return undefined
  }
  const catalog: Catalog = {
    database: row.database,
    serverVersion: row.serverVersion,
    tables,
    functions: new Set(row.functions.map(([name]) => name)),
    volatileFunctions: new Set(row.functions.filter(([, volatile]) => volatile).map(([name]) => name)),
    find,
  }
  for (const t of row.tables) {
    const columns = t.columns.map(
      ([name, category, type, collation, deterministic]): Column => ({
        name,
        category,
        type,
        deterministic,
        orderableText: ['C', 'POSIX', 'ucs_basic'].includes(collation) || (collation === 'default' && row.cOrdering),
      }),
    )
    const triggers = t.triggers ?? []
    tables.set(`${t.schema}.${t.name}`, {
      id: `${t.schema}.${t.name}`,
      columns,
      column: new Map(columns.map((c) => [c.name, c])),
      primaryKey: t.primaryKey ?? [],
      isView: t.view !== null,
      alsoReads: new Set(),
      alsoWrites: new Set(),
      referencedBy: (t.referencedBy ?? []).map(([table, columns, parentColumns, onDelete, onUpdate]) => ({
        table,
        ...(columns.length === 1 ? { column: columns[0], parentColumn: parentColumns[0] } : {}),
        onDelete,
        onUpdate,
      })),
      updateChangesOtherColumns:
        t.columns.some((c) => c[5]) ||
        triggers.some(
          ([type]) => (type & TRIGGER_ROW) !== 0 && (type & TRIGGER_BEFORE) !== 0 && (type & TRIGGER_UPDATE) !== 0,
        ),
    })
  }
  // Second pass: these name other tables, which must all be known first.
  for (const t of row.tables) {
    const table = tables.get(`${t.schema}.${t.name}`)!
    const reads = [...(t.view ? [t.view] : []), ...(t.rls ? (t.policies ?? []).map((p) => `select 1 where ${p}`) : [])]
    table.alsoReads = union(reads.map((sql) => tablesOfStatements(parseStatements(sql), catalog)))
    const triggerWrites = (t.triggers ?? []).map(([, language, source]) =>
      tablesWrittenByFunction(language, source, catalog),
    )
    table.alsoWrites = union([table.alsoWrites, ...triggerWrites])
  }
  return catalog
}

function tablesOfStatements(statements: Node[] | null, catalog: Catalog): TableSet {
  if (!statements) return 'all'
  const ids = new Set<string>()
  for (const table of tablesIn(statements)) {
    const found = catalog.find(table)
    if (found) ids.add(found.id)
  }
  return ids
}

/** What a trigger function may write: the targets of its SQL statements, or every table when that can't be told. */
function tablesWrittenByFunction(language: string, source: string, catalog: Catalog): TableSet {
  if (language !== 'plpgsql') return 'all'
  let parsed: unknown
  try {
    parsed = parsePlPgSQLSync(`create function f() returns trigger language plpgsql as $live$${source}$live$`)
  } catch {
    return 'all'
  }
  const statements: string[] = []
  let dynamic = false
  walk(parsed, (key, value) => {
    if (key === 'PLpgSQL_stmt_dynexecute' || key === 'PLpgSQL_stmt_dynfors') dynamic = true
    if (key === 'PLpgSQL_stmt_execsql') statements.push(value.sqlstmt.PLpgSQL_expr.query)
  })
  if (dynamic) return 'all'
  const written: TableSet[] = statements.map((sql) => {
    const parsedStatements = parseStatements(sql)
    if (!parsedStatements) return 'all'
    const writes = parsedStatements.filter((s) => s.InsertStmt || s.UpdateStmt || s.DeleteStmt || s.MergeStmt)
    return tablesOfStatements(writes, catalog)
  })
  return union(written)
}

function union(sets: TableSet[]): TableSet {
  if (sets.includes('all')) return 'all'
  return new Set(sets.flatMap((s) => [...s]))
}
