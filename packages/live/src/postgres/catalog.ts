export { loadCatalog }

import { parsePlPgSQLSync } from 'libpg-query'
import {
  ACCESS_ROWS,
  accessOf,
  union,
  WRITES_OF_ANY_KIND,
  type AccessRow,
  type Catalog,
  type Table,
  type TableSet,
  type Write,
} from '../catalog.js'
import type { Column } from '../match/values.js'
import { names, quote, tablesIn, walk, type Node, type RangeVar } from '../tree.js'
import { POSTGRES } from './dialect.js'
import { loadParser, parseStatements } from './sql.js'

type CatalogRow = {
  database: string
  role: string
  access: AccessRow[]
  searchPath: string[]
  serverVersion: number
  cOrdering: boolean
  functions: [
    name: string,
    writing: boolean,
    volatile: boolean,
    defaults: string | null,
    argumentNames: string[] | null,
    argumentModes: string[] | null,
    inputCount: number,
    variadic: boolean,
  ][]
  volatileFunctions: string[]
  aggregates: string[]
  domains: [schema: string, name: string, baseSchema: string, baseName: string][]
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

/** A function of the app's, among those of its name a call may mean: its input arguments' names (`''` for one without),
 *  and the expressions of its defaults, those of its last arguments. */
type Overload = { inputs: string[]; defaults: Node[]; variadic: boolean }

/** Whether the function `p`, in language `l`, is one of `Catalog.writingFunctions`. */
const WRITING = `l.lanname not in ('c', 'internal')`

// One round trip: everything the matcher needs to know about the database's tables. `domain_bases`: each domain, the
// type it is over through domains over domains, and the innermost modifier on the way (`char(4)` under two domains).
const CATALOG_QUERY = `with recursive domains (oid, base, typmod) as (
    select t.oid, t.typbasetype, t.typtypmod from pg_type t where t.typtype = 'd'
  union all
    select d.oid, t.typbasetype, case when t.typtypmod <> -1 then t.typtypmod else d.typmod end
    from domains d join pg_type t on t.oid = d.base where t.typtype = 'd'
), domain_bases as (
  select d.oid, d.base, d.typmod from domains d join pg_type b on b.oid = d.base where b.typtype <> 'd'
)
select json_build_object(
  'database', current_database(),
  'role', current_user,
  'access', (${ACCESS_ROWS}),
  'searchPath', current_schemas(false),
  'serverVersion', current_setting('server_version_num')::int,
  'cOrdering', (select datlocprovider in ('c', 'b') and datcollate in ('C', 'POSIX') from pg_database where datname = current_database()),
  'functions', (select coalesce(json_agg(json_build_array(p.proname, ${WRITING}, p.provolatile = 'v',
      pg_get_expr(p.proargdefaults, 0), p.proargnames, p.proargmodes, p.pronargs, p.provariadic <> 0)), '[]') from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace join pg_language l on l.oid = p.prolang
    where n.nspname not in ('pg_catalog', 'information_schema') and p.prorettype <> 'trigger'::regtype),
  'volatileFunctions', (select coalesce(json_agg(distinct p.proname), '[]') from pg_proc p where p.provolatile = 'v'),
  'aggregates', (select coalesce(json_agg(distinct p.proname), '[]') from pg_proc p where p.prokind = 'a'),
  'domains', (select coalesce(json_agg(json_build_array(n.nspname, t.typname, bn.nspname, bt.typname)), '[]')
    from domain_bases db join pg_type t on t.oid = db.oid join pg_namespace n on n.oid = t.typnamespace
    join pg_type bt on bt.oid = db.base join pg_namespace bn on bn.oid = bt.typnamespace),
  'tables', (select coalesce(json_agg(json_build_object(
    'schema', n.nspname,
    'name', c.relname,
    'view', case when c.relkind in ('v', 'm') then pg_get_viewdef(c.oid) end,
    'rls', c.relrowsecurity,
    'columns', (select json_agg(json_build_array(a.attname, t.typcategory, t.typname,
        quote_ident(tn.nspname) || '.' || quote_ident(t.typname), coalesce(co.collname, ''),
        coalesce(co.collisdeterministic, true), a.attgenerated <> '', bn.nspname, bt.typname,
        case when t.typtype = 'd' then db.typmod else a.atttypmod end) order by a.attnum)
      from pg_attribute a join pg_type t on t.oid = a.atttypid join pg_namespace tn on tn.oid = t.typnamespace
      left join domain_bases db on db.oid = t.oid join pg_type bt on bt.oid = coalesce(db.base, t.oid)
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
// `PLpgSQL_expr.parseMode`: a whole statement, an expression, an assignment (`target := value`).
const PARSE_STATEMENT = 0
const PARSE_EXPRESSION = 2

/** Statements a trigger may run that change no table but those their nodes name (`WRITES`). */
const KNOWN_STATEMENTS = new Set(['SelectStmt', 'InsertStmt', 'UpdateStmt', 'DeleteStmt', 'MergeStmt', 'NotifyStmt'])
const WRITES = new Set(['InsertStmt', 'UpdateStmt', 'DeleteStmt', 'MergeStmt'])
/** The argument modes (`pg_proc.proargmodes`) of input arguments: `IN`, `INOUT`, `VARIADIC`. */
const INPUT_MODES = new Set(['i', 'b', 'v'])

async function loadCatalog(query: (text: string) => Promise<string>): Promise<Catalog> {
  // Function bodies and defaults are parsed below: the parser loads while the query runs.
  const [answer] = await Promise.all([query(CATALOG_QUERY), loadParser()])
  const row = JSON.parse(answer) as CatalogRow
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
  const overloads = new Map<string, Overload[]>()
  for (const [name, , , defaultsText, argumentNames, argumentModes, inputCount, variadic] of row.functions) {
    const parsed = defaultsText === null ? [] : parseStatements(`SELECT ${defaultsText}`)
    // Defaults that don't parse may call anything.
    if (!parsed) {
      writingFunctions.add(name)
      volatileWritingFunctions.add(name)
    }
    const defaults = ((parsed?.[0]?.SelectStmt.targetList ?? []) as Node[]).map((target) => target.ResTarget.val)
    const inputs = argumentNames
      ? argumentNames.filter((_, i) => !argumentModes || INPUT_MODES.has(argumentModes[i]!))
      : Array<string>(inputCount).fill('')
    overloads.set(name, [...(overloads.get(name) ?? []), { inputs, defaults, variadic }])
  }
  /** `called`, with the functions the defaults of any of their overloads call: their calls aren't known. */
  const withDefaultCalls = (called: string[]): string[] => [
    ...called,
    ...callsIn(
      called.flatMap((name) => (overloads.get(name) ?? []).flatMap((overload) => overload.defaults)),
      overloads,
    ),
  ]
  const domains = new Map<string, { schema: string; base: readonly [string, string] }[]>()
  for (const [schema, name, baseSchema, baseName] of row.domains)
    domains.set(name, [...(domains.get(name) ?? []), { schema, base: [baseSchema, baseName] }])
  const catalog: Catalog = {
    dialect: POSTGRES,
    database: row.database,
    role: row.role,
    access: accessOf(row.access),
    serverVersion: row.serverVersion,
    tables,
    functions: new Set(row.functions.map(([name]) => name)),
    writingFunctions,
    volatileWritingFunctions,
    calls(node) {
      return callsIn(node, overloads)
    },
    volatileFunctions: new Set(row.volatileFunctions),
    aggregates: new Set(row.aggregates),
    searchPath: row.searchPath,
    domainBases(typeNames) {
      const [schema, name] = typeNames.length === 2 ? typeNames : [undefined, typeNames[0]!]
      return (domains.get(name!) ?? []).filter((d) => schema === undefined || d.schema === schema).map((d) => d.base)
    },
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

/** The functions called below `node`, with those the defaults of the arguments they leave out call, theirs too. */
function callsIn(node: unknown, overloads: Map<string, Overload[]>, walked = new Set<Node>()): string[] {
  const called: string[] = []
  walk(node, (key, call) => {
    if (key !== 'FuncCall') return
    const name = names(call.funcname).at(-1)!
    called.push(name)
    for (const fallback of defaultsEvaluated(call, overloads.get(name) ?? [])) {
      if (walked.has(fallback)) continue
      walked.add(fallback)
      called.push(...callsIn(fallback, overloads, walked))
    }
  })
  return called
}

/** The defaults `call` evaluates: those of the arguments it leaves out, when one overload of its name takes its
 *  arguments and none is variadic; otherwise those of every overload, as the one it calls isn't known. */
function defaultsEvaluated(call: Node, overloads: Overload[]): Node[] {
  const args = (call.args ?? []) as Node[]
  const named = args.flatMap((arg) => (arg.NamedArgExpr ? [arg.NamedArgExpr.name as string] : []))
  const positional = args.length - named.length
  const taking = overloads.filter((overload) => takes(overload, positional, named))
  if (call.func_variadic || overloads.some((overload) => overload.variadic) || taking.length !== 1)
    return overloads.flatMap((overload) => overload.defaults)
  const { inputs, defaults } = taking[0]!
  const firstDefault = inputs.length - defaults.length
  return defaults.filter((_, i) => firstDefault + i >= positional && !named.includes(inputs[firstDefault + i]!))
}

/** Whether `overload` takes `positional` arguments followed by `named` ones: each named one is an argument after the
 *  positional ones, and every argument without a default is given. */
function takes({ inputs, defaults }: Overload, positional: number, named: string[]): boolean {
  if (positional > inputs.length || named.some((name) => inputs.indexOf(name) < positional)) return false
  return inputs.every((name, i) => i < positional || i >= inputs.length - defaults.length || named.includes(name))
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
