export { loadCatalog, tablesChangedWith, union }
export type { Catalog, Table, TableSet, ForeignKey }

import { parsePlPgSQLSync } from 'libpg-query'
import type { Column } from './match/values.js'
import { functionNamesIn, parseStatements, quote, tablesIn, walk, type Node, type RangeVar } from './sql.js'

/** Tables by id (`schema.name`), or every table. */
type TableSet = ReadonlySet<string> | 'all'

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
  /** A write to this table may also change these: what its triggers write. */
  alsoWrites: TableSet
  /** Foreign keys of other tables referencing this one with an action that changes them (`ON DELETE CASCADE`, ...). */
  referencedBy: ForeignKey[]
  /** Single-column foreign keys checked at the end of each statement: every non-`NULL` `column` value is the
   *  `parentColumn` of a row of `parent`. */
  references: { column: string; parent: string; parentColumn: string }[]
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
  /** The `search_path` of the session that loaded the catalog, which unqualified names resolve against while every
   *  session has it; `undefined` once a statement may have changed a session's (`LiveDatabase`). */
  searchPath: string[] | undefined
  /** The table a name means; `undefined` when unknown, or when it may mean several (`named()`). */
  find(table: RangeVar): Table | undefined
  /** Every table a name may mean: in any schema, unless qualified or resolved by `searchPath`. */
  named(table: RangeVar): Table[]
}

type CatalogRow = {
  database: string
  searchPath: string[]
  serverVersion: number
  cOrdering: boolean
  functions: [
    name: string,
    volatile: boolean,
    language: string,
    source: string | null,
    argumentList: string,
    result: string | null,
  ][]
  tables: {
    schema: string
    name: string
    view: string | null
    rls: boolean
    columns: [
      name: string,
      category: string,
      type: string,
      sqlType: string,
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
    references: [column: string, parent: string, parentColumn: string][] | null
  }[]
}

// One round trip: everything the matcher needs to know about the database's tables.
const CATALOG_QUERY = `select json_build_object(
  'database', current_database(),
  'searchPath', current_schemas(false),
  'serverVersion', current_setting('server_version_num')::int,
  'cOrdering', (select datlocprovider in ('c', 'b') and datcollate in ('C', 'POSIX') from pg_database where datname = current_database()),
  'functions', (select coalesce(json_agg(json_build_array(p.proname, p.provolatile = 'v', l.lanname,
      case when p.prosqlbody is null then p.prosrc end, pg_get_function_arguments(p.oid),
      pg_get_function_result(p.oid))), '[]') from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace join pg_language l on l.oid = p.prolang
    where n.nspname not in ('pg_catalog', 'information_schema') and p.prorettype <> 'trigger'::regtype),
  'tables', (select coalesce(json_agg(json_build_object(
    'schema', n.nspname,
    'name', c.relname,
    'view', case when c.relkind in ('v', 'm') then pg_get_viewdef(c.oid) end,
    'rls', c.relrowsecurity,
    'columns', (select json_agg(json_build_array(a.attname, t.typcategory, t.typname,
        quote_ident(tn.nspname) || '.' || quote_ident(t.typname), coalesce(co.collname, ''),
        coalesce(co.collisdeterministic, true), a.attgenerated <> '') order by a.attnum)
      from pg_attribute a join pg_type t on t.oid = a.atttypid join pg_namespace tn on tn.oid = t.typnamespace
      left join pg_collation co on co.oid = a.attcollation
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
      where f.confrelid = c.oid and f.contype = 'f' and (f.confdeltype in ('c', 'n', 'd') or f.confupdtype in ('c', 'n', 'd'))),
    'references', (select json_agg(json_build_array(a.attname, ps.nspname || '.' || p.relname, pa.attname))
      from pg_constraint f join pg_class p on p.oid = f.confrelid join pg_namespace ps on ps.oid = p.relnamespace
      join pg_attribute a on a.attrelid = f.conrelid and a.attnum = f.conkey[1]
      join pg_attribute pa on pa.attrelid = f.confrelid and pa.attnum = f.confkey[1]
      where f.conrelid = c.oid and f.contype = 'f' and not f.condeferrable and cardinality(f.conkey) = 1)
  )), '[]') from pg_class c join pg_namespace n on n.oid = c.relnamespace
    where c.relkind in ('r', 'p', 'v', 'm', 'f') and n.nspname not in ('pg_catalog', 'information_schema') and n.nspname !~ '^pg_toast')
)::text as catalog`

// `pg_trigger.tgtype` bits
const TRIGGER_ROW = 1
const TRIGGER_BEFORE = 2
const TRIGGER_UPDATE = 16

// `PLpgSQL_expr.parseMode`: a whole statement, an expression, an assignment (`target := value`).
const PARSE_STATEMENT = 0
const PARSE_EXPRESSION = 2

/** Statements a function's body may run that change no table but those their nodes name (`WRITES`). */
const KNOWN_STATEMENTS = new Set(['SelectStmt', 'InsertStmt', 'UpdateStmt', 'DeleteStmt', 'MergeStmt', 'NotifyStmt'])
const WRITES = new Set(['InsertStmt', 'UpdateStmt', 'DeleteStmt', 'MergeStmt'])

/** A function of the app's: its body (`null` when the catalog doesn't give it as text), and its header's parts. */
type FunctionBody = { language: string; source: string | null; argumentList: string; result: string | null }

/** How the bodies of the app's functions name what they write and call. */
type Names = { tables: (table: RangeVar) => Table[]; functions: Map<string, FunctionBody[]> }

async function loadCatalog(query: (text: string) => Promise<string>): Promise<Catalog> {
  const row = JSON.parse(await query(CATALOG_QUERY)) as CatalogRow
  const tables = new Map<string, Table>()
  const byName = new Map<string, Table[]>()
  const anySchema = (table: RangeVar): Table[] => {
    if (!table.schemaname) return byName.get(table.relname) ?? []
    const found = tables.get(`${table.schemaname}.${table.relname}`)
    return found ? [found] : []
  }
  const catalog: Catalog = {
    database: row.database,
    serverVersion: row.serverVersion,
    tables,
    functions: new Set(row.functions.map(([name]) => name)),
    volatileFunctions: new Set(row.functions.filter(([, volatile]) => volatile).map(([name]) => name)),
    searchPath: row.searchPath,
    find(table) {
      const named = this.named(table)
      return named.length === 1 ? named[0] : undefined
    },
    named(table) {
      const inPath = this.searchPath?.map((schema) => tables.get(`${schema}.${table.relname}`)).find(Boolean)
      return table.schemaname || !inPath ? anySchema(table) : [inPath]
    },
  }
  for (const t of row.tables) {
    const columns = t.columns.map(
      ([name, category, type, sqlType, collation, deterministic]): Column => ({
        name,
        category,
        type,
        sqlType,
        deterministic,
        orderableText: ['C', 'POSIX', 'ucs_basic'].includes(collation) || (collation === 'default' && row.cOrdering),
      }),
    )
    const triggers = t.triggers ?? []
    const table: Table = {
      id: `${t.schema}.${t.name}`,
      sqlName: `${quote(t.schema)}.${quote(t.name)}`,
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
      references: (t.references ?? []).map(([column, parent, parentColumn]) => ({ column, parent, parentColumn })),
      updateChangesOtherColumns:
        t.columns.some((c) => c[6]) ||
        triggers.some(
          ([type]) => (type & TRIGGER_ROW) !== 0 && (type & TRIGGER_BEFORE) !== 0 && (type & TRIGGER_UPDATE) !== 0,
        ),
    }
    tables.set(table.id, table)
    byName.set(t.name, [...(byName.get(t.name) ?? []), table])
  }
  const functions = new Map<string, FunctionBody[]>()
  for (const [name, , language, source, argumentList, result] of row.functions)
    functions.set(name, [...(functions.get(name) ?? []), { language, source, argumentList, result }])
  // A function resolves names with the `search_path` of the session that calls it: any schema's table may be meant.
  const names: Names = { tables: anySchema, functions }
  // Second pass: these name other tables, which must all be known first.
  for (const t of row.tables) {
    const table = tables.get(`${t.schema}.${t.name}`)!
    const reads = [...(t.view ? [t.view] : []), ...(t.rls ? (t.policies ?? []).map((p) => `select 1 where ${p}`) : [])]
    // Printed for the loading session: its `search_path` resolves them.
    table.alsoReads = union(reads.map((sql) => tablesOfStatements(parseStatements(sql), (t) => catalog.named(t))))
    const triggerWrites = (t.triggers ?? []).map(([, language, source]) =>
      tablesWrittenByFunction({ language, source, argumentList: '', result: 'trigger' }, names),
    )
    table.alsoWrites = union([table.alsoWrites, ...triggerWrites])
  }
  return catalog
}

/** The tables `statements` name, each name resolved by `named`. */
function tablesOfStatements(statements: Node[] | null, named: (table: RangeVar) => Table[]): TableSet {
  if (!statements) return 'all'
  return new Set(tablesIn(statements).flatMap((table) => named(table).map((found) => found.id)))
}

/**
 * What running a function of the app's may write: the tables its statements write, and what the functions of the app's
 * they call write, and so on. Every table when that can't be told: a body run with `EXECUTE`, in a language other than
 * SQL and PL/pgSQL, or that doesn't parse.
 */
function tablesWrittenByFunction(fn: FunctionBody, names: Names, seen = new Set<FunctionBody>()): TableSet {
  if (seen.has(fn)) return new Set()
  seen.add(fn)
  const sql = fn.language === 'sql' ? fn.source && [fn.source] : fn.language === 'plpgsql' ? statementsOf(fn) : null
  if (!sql) return 'all'
  return union(sql.map((text) => tablesWrittenBySql(text, names, seen)))
}

/** The SQL a PL/pgSQL function runs, every expression as a `SELECT`; `null` when it runs SQL built at run time. */
function statementsOf({ source, argumentList, result }: FunctionBody): string[] | null {
  let parsed: unknown
  try {
    parsed = parsePlPgSQLSync(
      `create function f(${argumentList}) returns ${result} language plpgsql as $live$${source}$live$`,
    )
  } catch {
    return null
  }
  const statements: string[] = []
  let dynamic = false
  walk(parsed, (key, value) => {
    if (key === 'PLpgSQL_stmt_dynexecute' || key === 'PLpgSQL_stmt_dynfors' || key === 'dynquery') dynamic = true
    if (key !== 'PLpgSQL_expr') return
    const { query, parseMode } = value as { query: string; parseMode: number }
    if (parseMode === PARSE_STATEMENT) statements.push(query)
    else statements.push(`SELECT ${parseMode === PARSE_EXPRESSION ? query : query.replace(/^[^:=]*:?=/, '')}`)
  })
  return dynamic ? null : statements
}

function tablesWrittenBySql(sql: string, names: Names, seen: Set<FunctionBody>): TableSet {
  const statements = parseStatements(sql)
  if (!statements || statements.some((s) => !KNOWN_STATEMENTS.has(Object.keys(s)[0]!))) return 'all'
  const written: TableSet[] = []
  walk(statements, (key, value) => {
    if (WRITES.has(key)) written.push(new Set(names.tables(value.relation).map((table) => table.id)))
  })
  for (const name of functionNamesIn(statements))
    for (const fn of names.functions.get(name) ?? []) written.push(tablesWrittenByFunction(fn, names, seen))
  return union(written)
}

/** `tables`, and every table a write to them may change too: what their triggers write and what their foreign key
 *  actions change, then what writing those changes, and so on. */
function tablesChangedWith(tables: TableSet, catalog: Catalog): TableSet {
  if (tables === 'all') return 'all'
  const reached = new Set<string>()
  const pending = [...tables]
  while (pending.length > 0) {
    const id = pending.pop()!
    if (reached.has(id)) continue
    reached.add(id)
    const table = catalog.tables.get(id)
    if (!table) continue
    if (table.alsoWrites === 'all') return 'all'
    pending.push(...table.alsoWrites, ...table.referencedBy.map((key) => key.table))
  }
  return reached
}

function union(sets: TableSet[]): TableSet {
  if (sets.includes('all')) return 'all'
  return new Set(sets.flatMap((s) => [...s]))
}
