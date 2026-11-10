export { loadCatalog, actionOf, sideEffects, tablesChangedBy, union }
export type { Catalog, Table, TableSet, ForeignKey, Written }

import { parsePlPgSQLSync } from 'libpg-query'
import type { Column } from './match/values.js'
import { functionNamesIn, parseStatements, quote, tablesIn, walk, type Node, type RangeVar } from './sql.js'

/** Tables by id (`schema.name`), or every table. */
type TableSet = ReadonlySet<string> | 'all'

/** The kinds of write that fire a table's triggers and rules. */
type Write = 'insert' | 'update' | 'delete' | 'truncate'

/** A table written, by a kind of write (`undefined`: any). */
type Written = readonly [table: string, write: Write | undefined]

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
  /** What the role that loaded the catalog may `SELECT`: the whole table, or each of its columns (system columns
   *  such as `xmax` aren't among them). A write's widened `RETURNING` reads them. */
  readable: 'table' | 'columns' | undefined
}

/** `table.column` references the parent's `parentColumn`; on a parent's delete or key update the child rows are
 *  deleted (`CASCADE`) or set to `NULL` or their default. */
type ForeignKey = { table: string; column?: string; parentColumn?: string; onDelete: string; onUpdate: string }

type Catalog = {
  database: string
  /** The role that loaded the catalog: the one `Table.readable` holds the privileges of. */
  role: string
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
  /** The functions `node` calls, with those their default arguments call: the caller evaluates those. */
  calls(node: unknown): string[]
  /** The volatile functions of every schema, built-in ones included (`random()`, `nextval()`, `clock_timestamp()`):
   *  two calls may return different values. */
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
  role: string
  searchPath: string[]
  serverVersion: number
  cOrdering: boolean
  functions: [name: string, writing: boolean, volatile: boolean, defaults: string | null][]
  volatileFunctions: string[]
  tables: {
    schema: string
    name: string
    view: string | null
    rls: boolean
    readable: 'table' | 'columns' | null
    columns: [
      name: string,
      category: string,
      type: string,
      sqlType: string,
      collation: string,
      deterministic: boolean,
      generated: boolean,
      baseSchema: string,
      baseName: string,
      typmod: number,
    ][]
    primaryKey: string[] | null
    policies: string[] | null
    triggers: [type: number, language: string, source: string][] | null
    /** The events its rules fire on (`pg_rewrite.ev_type`). */
    rules: string[] | null
    /** The functions its column defaults call. */
    defaultCalls: string[] | null
    referencedBy:
      | [table: string, columns: string[], parentColumns: string[], onDelete: string, onUpdate: string][]
      | null
    references: [column: string, parent: string, parentColumn: string][] | null
  }[]
}

/** Whether the function `p`, in language `l`, is one of `Catalog.writingFunctions`. */
const WRITING = `l.lanname not in ('c', 'internal')`

// One round trip: everything the matcher needs to know about the database's tables.
const CATALOG_QUERY = `select json_build_object(
  'database', current_database(),
  'role', current_user,
  'searchPath', current_schemas(false),
  'serverVersion', current_setting('server_version_num')::int,
  'cOrdering', (select datlocprovider in ('c', 'b') and datcollate in ('C', 'POSIX') from pg_database where datname = current_database()),
  'functions', (select coalesce(json_agg(json_build_array(p.proname, ${WRITING}, p.provolatile = 'v',
      pg_get_expr(p.proargdefaults, 0))), '[]') from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace join pg_language l on l.oid = p.prolang
    where n.nspname not in ('pg_catalog', 'information_schema') and p.prorettype <> 'trigger'::regtype),
  'volatileFunctions', (select coalesce(json_agg(distinct p.proname), '[]') from pg_proc p where p.provolatile = 'v'),
  'tables', (select coalesce(json_agg(json_build_object(
    'schema', n.nspname,
    'name', c.relname,
    'view', case when c.relkind in ('v', 'm') then pg_get_viewdef(c.oid) end,
    'rls', c.relrowsecurity,
    'readable', case when has_table_privilege(c.oid, 'SELECT') then 'table'
      when not exists (select from pg_attribute a where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
        and not has_column_privilege(c.oid, a.attnum, 'SELECT')) then 'columns' end,
    'columns', (select json_agg(json_build_array(a.attname, t.typcategory, t.typname,
        quote_ident(tn.nspname) || '.' || quote_ident(t.typname), coalesce(co.collname, ''),
        coalesce(co.collisdeterministic, true), a.attgenerated <> '', bn.nspname, bt.typname,
        case when t.typtype = 'd' then t.typtypmod else a.atttypmod end) order by a.attnum)
      from pg_attribute a join pg_type t on t.oid = a.atttypid join pg_namespace tn on tn.oid = t.typnamespace
      join pg_type bt on bt.oid = case when t.typtype = 'd' then t.typbasetype else t.oid end
      join pg_namespace bn on bn.oid = bt.typnamespace
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
    'rules', (select json_agg(r.ev_type) from pg_rewrite r where r.ev_class = c.oid and r.ev_type <> '1'),
    'defaultCalls', (select json_agg(p.proname) from pg_attrdef d
      join pg_depend dp on dp.classid = 'pg_attrdef'::regclass and dp.objid = d.oid and dp.refclassid = 'pg_proc'::regclass
      join pg_proc p on p.oid = dp.refobjid join pg_namespace ns on ns.oid = p.pronamespace
      where d.adrelid = c.oid and ns.nspname not in ('pg_catalog', 'information_schema')),
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
const TRIGGER_EVENTS: [bit: number, write: Write][] = [
  [4, 'insert'],
  [8, 'delete'],
  [TRIGGER_UPDATE, 'update'],
  [32, 'truncate'],
]
/** `pg_rewrite.ev_type` */
const RULE_EVENTS: Record<string, Write> = { '2': 'update', '3': 'insert', '4': 'delete' }
const WRITES_OF_ANY_KIND: Write[] = ['insert', 'update', 'delete', 'truncate']

// `PLpgSQL_expr.parseMode`: a whole statement, an expression, an assignment (`target := value`).
const PARSE_STATEMENT = 0
const PARSE_EXPRESSION = 2

/** Statements a trigger may run that change no table but those their nodes name (`WRITES`). */
const KNOWN_STATEMENTS = new Set(['SelectStmt', 'InsertStmt', 'UpdateStmt', 'DeleteStmt', 'MergeStmt', 'NotifyStmt'])
const WRITES = new Set(['InsertStmt', 'UpdateStmt', 'DeleteStmt', 'MergeStmt'])

async function loadCatalog(query: (text: string) => Promise<string>): Promise<Catalog> {
  const row = JSON.parse(await query(CATALOG_QUERY)) as CatalogRow
  const tables = new Map<string, Table>()
  const byName = new Map<string, Table[]>()
  const anySchema = (table: RangeVar): Table[] => {
    if (!table.schemaname) return byName.get(table.relname) ?? []
    const found = tables.get(`${table.schemaname}.${table.relname}`)
    return found ? [found] : []
  }
  const writingFunctions = new Set(row.functions.filter(([, writing]) => writing).map(([name]) => name))
  const volatileWritingFunctions = new Set(
    row.functions.filter(([, writing, volatile]) => writing && volatile).map(([name]) => name),
  )
  const defaultCalls = new Map<string, string[]>()
  for (const [name, , , defaults] of row.functions) {
    if (defaults === null) continue
    const parsed = parseStatements(`SELECT ${defaults}`)
    if (parsed) {
      defaultCalls.set(name, [...(defaultCalls.get(name) ?? []), ...functionNamesIn(parsed)])
      continue
    }
    // Defaults that don't parse may call anything.
    writingFunctions.add(name)
    volatileWritingFunctions.add(name)
  }
  /** `called`, with the functions their default arguments call, theirs too. */
  const withDefaultCalls = (called: string[]): string[] => {
    const all = new Set(called)
    for (const name of all) for (const inner of defaultCalls.get(name) ?? []) all.add(inner)
    return [...all]
  }
  const catalog: Catalog = {
    database: row.database,
    role: row.role,
    serverVersion: row.serverVersion,
    tables,
    functions: new Set(row.functions.map(([name]) => name)),
    writingFunctions,
    volatileWritingFunctions,
    calls(node) {
      return withDefaultCalls(functionNamesIn(node))
    },
    volatileFunctions: new Set(row.volatileFunctions),
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
      ([name, category, type, sqlType, collation, deterministic, , baseSchema, baseName, typmod]): Column => ({
        name,
        category,
        type,
        sqlType,
        base: [baseSchema, baseName],
        // A `char(n)`'s modifier is `n` plus a 4-byte header.
        ...(baseName === 'bpchar' && typmod >= 4 ? { charLength: typmod - 4 } : {}),
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
      alsoWrites: { insert: new Set(), update: new Set(), delete: new Set(), truncate: new Set() },
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
      readable: t.readable ?? undefined,
    }
    tables.set(table.id, table)
    byName.set(t.name, [...(byName.get(t.name) ?? []), table])
  }
  // Second pass: these name other tables, which must all be known first.
  for (const t of row.tables) {
    const table = tables.get(`${t.schema}.${t.name}`)!
    const reads = [...(t.view ? [t.view] : []), ...(t.rls ? (t.policies ?? []).map((p) => `select 1 where ${p}`) : [])]
    // Printed for the loading session: its `search_path` resolves them.
    table.alsoReads = union(reads.map((sql) => tablesOfStatements(parseStatements(sql), (t) => catalog.named(t))))
    const { alsoWrites } = table
    for (const [type, language, source] of t.triggers ?? []) {
      // A trigger resolves names with the `search_path` of the session that fires it: any schema's table may be meant.
      const written = tablesWrittenByTrigger(language, source, anySchema, catalog)
      for (const [bit, write] of TRIGGER_EVENTS) if (type & bit) alsoWrites[write] = union([alsoWrites[write], written])
    }
    for (const event of t.rules ?? []) alsoWrites[RULE_EVENTS[event]!] = 'all'
    if (withDefaultCalls(t.defaultCalls ?? []).some((name) => catalog.writingFunctions.has(name)))
      alsoWrites.insert = alsoWrites.update = 'all'
    // A write to a view writes its tables, or what its rules and `INSTEAD OF` triggers write.
    if (t.view) for (const write of WRITES_OF_ANY_KIND) alsoWrites[write] = 'all'
  }
  return catalog
}

/** The tables `statements` name, each name resolved by `named`. */
function tablesOfStatements(statements: Node[] | null, named: (table: RangeVar) => Table[]): TableSet {
  if (!statements) return 'all'
  return new Set(tablesIn(statements).flatMap((table) => named(table).map((found) => found.id)))
}

/**
 * What a trigger may write: the tables its statements write. Every table when that can't be told: it calls one of
 * `Catalog.writingFunctions`, runs SQL built at run time or a statement other than a query, a write or `NOTIFY`, writes a
 * table the catalog doesn't know, isn't written in PL/pgSQL, or doesn't parse.
 */
function tablesWrittenByTrigger(
  language: string,
  source: string,
  named: (table: RangeVar) => Table[],
  catalog: Catalog,
): TableSet {
  const statements = language === 'plpgsql' ? statementsOf(source) : null
  if (!statements) return 'all'
  return union(statements.map((sql) => tablesWrittenBySql(sql, named, catalog)))
}

/** The SQL a trigger runs, every expression as a `SELECT`; `null` when it runs SQL built at run time. */
function statementsOf(source: string): string[] | null {
  let parsed: unknown
  try {
    parsed = parsePlPgSQLSync(`create function f() returns trigger language plpgsql as $live$${source}$live$`)
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

function tablesWrittenBySql(sql: string, named: (table: RangeVar) => Table[], catalog: Catalog): TableSet {
  const statements = parseStatements(sql)
  if (!statements || statements.some((s) => !KNOWN_STATEMENTS.has(Object.keys(s)[0]!))) return 'all'
  if (catalog.calls(statements).some((name) => catalog.writingFunctions.has(name))) return 'all'
  const written: Table[][] = []
  walk(statements, (key, value) => {
    if (WRITES.has(key)) written.push(named(value.relation))
  })
  if (written.some((tables) => tables.length === 0)) return 'all'
  return new Set(written.flat().map((table) => table.id))
}

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
