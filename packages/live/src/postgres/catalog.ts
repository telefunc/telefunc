export { loadCatalog }

import { parsePlPgSQLSync } from 'libpg-query'
import {
  ACCESS_ROWS,
  accessOf,
  callsVolatile,
  readsOfCalls,
  tablesChangedBy,
  writesOfCalls,
  union,
  WRITES_OF_ANY_KIND,
  type AccessRow,
  type Catalog,
  type Table,
  type TableSet,
  type Write,
  type Written,
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
    defaults: string | null,
    argumentNames: string[] | null,
    argumentModes: string[] | null,
    inputCount: number,
    variadic: boolean,
    /** A SQL function's `CREATE FUNCTION`. */
    definition: string | null,
    /** An aggregate's support functions (`pg_aggregate`): each one's name, whether it is Postgres's own, in C, and
     *  whether it is volatile. */
    support: [name: string, ownC: boolean, volatile: boolean][] | null,
    /** A PL/pgSQL function's `CREATE FUNCTION`. */
    plpgsql: string | null,
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
    /** Its triggers: the events each fires on (`pg_trigger.tgtype`), its function's language and `CREATE FUNCTION`. */
    triggers: [type: number, language: string, definition: string][] | null
    /** Its rules: the event each fires on (`pg_rewrite.ev_type`), and its `CREATE RULE`. */
    rules: [event: string, definition: string][] | null
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
  'functions', (select coalesce(json_agg(json_build_array(p.proname, ${WRITING},
      pg_get_expr(p.proargdefaults, 0), p.proargnames, p.proargmodes, p.pronargs, p.provariadic <> 0,
      case when l.lanname = 'sql' and p.prokind = 'f' then pg_get_functiondef(p.oid) end,
      (select json_agg(json_build_array(s.proname, sn.nspname = 'pg_catalog' and sl.lanname in ('internal', 'c'),
          s.provolatile = 'v'))
        from pg_aggregate a
        cross join unnest(array[a.aggtransfn, a.aggfinalfn, a.aggcombinefn, a.aggmtransfn, a.aggminvtransfn,
          a.aggmfinalfn]) f (oid)
        join pg_proc s on s.oid = f.oid join pg_namespace sn on sn.oid = s.pronamespace
        join pg_language sl on sl.oid = s.prolang
        where a.aggfnoid = p.oid),
      case when l.lanname = 'plpgsql' then pg_get_functiondef(p.oid) end)), '[]') from pg_proc p
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
    'triggers', (select json_agg(json_build_array(tg.tgtype, l.lanname, pg_get_functiondef(pr.oid))) from pg_trigger tg
      join pg_proc pr on pr.oid = tg.tgfoid join pg_language l on l.oid = pr.prolang
      where tg.tgrelid = c.oid and not tg.tgisinternal),
    'rules', (select json_agg(json_build_array(r.ev_type, pg_get_ruledef(r.oid))) from pg_rewrite r
      where r.ev_class = c.oid and r.ev_type <> '1'),
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

/** A function's or a trigger's statements, parsed, and whether they name every table it reads (`readsNamed`). */
type Body = { statements: Node[]; readsNamed: boolean }

/** Statements a trigger or a function may run that change no table but those their nodes name (`WRITES`): a SQL-standard
 *  body's `RETURN …` returns a value. */
const KNOWN_STATEMENTS = new Set([
  'SelectStmt',
  'InsertStmt',
  'UpdateStmt',
  'DeleteStmt',
  'MergeStmt',
  'NotifyStmt',
  'ReturnStmt',
])
/** The PL/pgSQL statements whose reads are all in the SQL of their expressions (`PLpgSQL_expr`), which `statementsOf()`
 *  reads. A body running any other reads tables its SQL may not name (`Body.readsNamed`): `FETCH` and `MOVE` run the
 *  query of a cursor that may have been opened elsewhere and passed in, and `EXECUTE` and `FOR … IN EXECUTE` build
 *  theirs at run time. */
const READ_PLPGSQL_STATEMENTS = new Set(
  [
    'block',
    'assign',
    'if',
    'case',
    'loop',
    'while',
    'fori',
    'fors',
    'forc',
    'foreach_a',
    'exit',
    'return',
    'return_next',
    'return_query',
    'raise',
    'assert',
    'execsql',
    'getdiag',
    'open',
    'close',
    'perform',
    'call',
    'commit',
    'rollback',
  ].map((kind) => `PLpgSQL_stmt_${kind}`),
)
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
  /** The functions whose defaults don't parse: a call of one may run anything. */
  const unreadDefaults = new Set<string>()
  const overloads = new Map<string, Overload[]>()
  for (const [name, , defaultsText, argumentNames, argumentModes, inputCount, variadic] of row.functions) {
    const parsed = defaultsText === null ? [] : parseStatements(`SELECT ${defaultsText}`)
    // Defaults that don't parse may call anything.
    if (!parsed) {
      writingFunctions.add(name)
      unreadDefaults.add(name)
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
    calls(node) {
      return callsIn(node, overloads)
    },
    // An aggregate is as volatile as its support functions: Postgres records every one as immutable.
    volatileFunctions: new Set([
      ...row.volatileFunctions,
      ...row.functions
        .filter(([, , , , , , , , support]) => support?.some(([, , volatile]) => volatile))
        .map(([name]) => name),
      ...unreadDefaults,
    ]),
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
  // Second pass: these name other tables, which must all be known first. A function's body resolves names by the
  // `search_path` of the session that calls it: any schema's table may be meant.
  const { reads: functionReads, writes: functionWrites } = effectsOfFunctions(
    row.functions,
    unreadDefaults,
    anySchema,
    catalog,
  )
  catalog.functionReads = functionReads
  catalog.functionWrites = functionWrites
  for (const t of row.tables) {
    const { alsoWrites } = tables.get(`${t.schema}.${t.name}`)!
    for (const [type, language, definition] of t.triggers ?? []) {
      // A trigger resolves names with the `search_path` of the session that fires it: any schema's table may be meant.
      const written = tablesWrittenBy(
        statementsOfBody(language, definition)?.statements ?? null,
        anySchema,
        catalog,
        functionWrites,
      )
      for (const [bit, write] of TRIGGER_EVENTS) if (type & bit) alsoWrites[write] = union([alsoWrites[write], written])
    }
    for (const [event, definition] of t.rules ?? []) {
      const write = RULE_EVENTS[event]!
      // Printed for the loading session: its `search_path` resolves them.
      alsoWrites[write] = union([alsoWrites[write], tablesWrittenByRule(definition, catalog, functionWrites)])
    }
    const defaultsWrite = withDefaultCalls(t.defaultCalls ?? [])
      .filter((name) => catalog.writingFunctions.has(name))
      .map((name) => functionWrites.get(name) ?? 'all')
    alsoWrites.insert = union([alsoWrites.insert, ...defaultsWrite])
    alsoWrites.update = union([alsoWrites.update, ...defaultsWrite])
    // A write to a view writes its tables, or what its rules and `INSTEAD OF` triggers write.
    if (t.view) for (const write of WRITES_OF_ANY_KIND) alsoWrites[write] = 'all'
  }
  // A function's writes change what writing those tables changes, as a write of the app's does: what their triggers and
  // rules write, and the rows their foreign keys' actions change. The views and policies calling one write that too.
  for (const [name, written] of functionWrites) {
    if (written === 'all') continue
    const changes: Written[] = [...written].map((id) => [id, undefined])
    functionWrites.set(name, tablesChangedBy(changes, catalog))
  }
  for (const t of row.tables) {
    const table = tables.get(`${t.schema}.${t.name}`)!
    const reads = [...(t.view ? [t.view] : []), ...(t.rls ? (t.policies ?? []).map((p) => `select 1 where ${p}`) : [])]
    // Printed for the loading session: its `search_path` resolves them.
    const parsed = reads.map((sql) => parseStatements(sql))
    table.alsoReads = union(parsed.map((statements) => tablesReadBy(statements, (t) => catalog.named(t), catalog)))
    table.volatile = parsed.some((statements) => !statements || callsVolatile(statements, catalog))
    const writes = union(parsed.map((statements) => (statements ? writesOfCalls(statements, catalog) : 'all')))
    if (writes === 'all' || writes.size > 0) table.writes = writes
  }
  return catalog
}

/** What a call of each function of the app's reads and writes (`Catalog.functionReads`, `Catalog.functionWrites`):
 *  what the bodies of the functions of its name read and write, in SQL or PL/pgSQL (`tablesReadByBody()`,
 *  `tablesWrittenBy()`), with what the app's functions they call read and write. Any table for one in another language,
 *  and any table written for one whose defaults don't parse (`unreadDefaults`). An aggregate reads and writes what its
 *  support functions do, and is one of `Catalog.writingFunctions` when it writes a table: Postgres records every
 *  aggregate as written in C. Calls may go round: each function's tables grow from none until no call adds one. */
function effectsOfFunctions(
  functions: CatalogRow['functions'],
  unreadDefaults: ReadonlySet<string>,
  named: (table: RangeVar) => Table[],
  catalog: Catalog,
): { reads: Map<string, TableSet>; writes: Map<string, TableSet> } {
  const reads = new Map<string, TableSet>()
  const writes = new Map<string, TableSet>()
  /** What each function reads and writes, given what the functions it calls read and write so far. */
  const effects: [name: string, read: () => TableSet, written: () => TableSet][] = []
  for (const [name, writing, , , , , , definition, support, plpgsql] of functions) {
    // Parsed once: each pass walks them again.
    const body =
      definition !== null ? bodyOf(definition) : plpgsql !== null ? statementsOfBody('plpgsql', plpgsql) : null
    const read = support ? () => readBySupport(support, reads) : () => tablesReadByBody(body, named, catalog, reads)
    const written: () => TableSet = unreadDefaults.has(name)
      ? () => 'all'
      : support
        ? () => writtenBySupport(support, catalog, writes)
        : writing
          ? () => tablesWrittenBy(body?.statements ?? null, named, catalog, writes)
          : () => new Set()
    effects.push([name, read, written])
  }
  for (const [name] of effects) {
    reads.set(name, new Set())
    writes.set(name, new Set())
  }
  for (let grew = true; grew; ) {
    grew = false
    for (const [name, read, written] of effects) {
      if (grown(reads, name, read())) grew = true
      if (grown(writes, name, written())) {
        catalog.writingFunctions.add(name)
        grew = true
      }
    }
  }
  return { reads, writes }
}

/** Adds `tables` to the tables `sets` holds for `name`: whether they grew. */
function grown(sets: Map<string, TableSet>, name: string, tables: TableSet): boolean {
  const before = sets.get(name)!
  const after = union([before, tables])
  if (after === before || (after !== 'all' && before !== 'all' && after.size === before.size)) return false
  sets.set(name, after)
  return true
}

/** What an aggregate's support functions of the app's read: Postgres's own C functions read nothing. */
function readBySupport(
  support: NonNullable<CatalogRow['functions'][number][8]>,
  reads: Map<string, TableSet>,
): TableSet {
  return union(support.map(([called, ownC]) => (ownC ? new Set<string>() : (reads.get(called) ?? 'all'))))
}

/** What an aggregate's support functions of the app's write. */
function writtenBySupport(
  support: NonNullable<CatalogRow['functions'][number][8]>,
  catalog: Catalog,
  writes: Map<string, TableSet>,
): TableSet {
  const writers = support.filter(([called, ownC]) => !ownC && catalog.writingFunctions.has(called))
  return union(writers.map(([called]) => writes.get(called) ?? 'all'))
}

/** What a rule writes besides its table: what its `DO ALSO` actions write. Every table for a `DO INSTEAD` rule, whose
 *  write's `RETURNING` becomes the rule's, which a widened one can't match. */
function tablesWrittenByRule(definition: string, catalog: Catalog, functionWrites: Map<string, TableSet>): TableSet {
  const rule = parseStatements(definition)?.[0]?.RuleStmt
  if (!rule || rule.instead) return 'all'
  return tablesWrittenBy((rule.actions ?? []) as Node[], (table) => catalog.named(table), catalog, functionWrites)
}

/** The statements of a SQL function's `CREATE FUNCTION`: its SQL-standard body (`RETURN …`, `BEGIN ATOMIC …`), or
 *  those of its `AS` text. */
function bodyOf(definition: string): Body | null {
  const create = parseStatements(definition)?.[0]?.CreateFunctionStmt
  if (!create) return null
  const body = create.sql_body
  // `BEGIN ATOMIC`'s statements are in a list in a list.
  if (body)
    return {
      statements: body.List ? (body.List.items as Node[]).flatMap((list) => list.List.items ?? []) : [body],
      readsNamed: true,
    }
  const as = ((create.options ?? []) as Node[]).find((option) => option.DefElem.defname === 'as')
  const source = as?.DefElem.arg.List.items[0]?.String?.sval
  const statements = typeof source === 'string' ? parseStatements(source) : null
  return statements && { statements, readsNamed: true }
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

/** The tables `statements` read: those they name, each name resolved by `named`, and those the app's functions they
 *  call read. */
function tablesReadBy(statements: Node[] | null, named: (table: RangeVar) => Table[], catalog: Catalog): TableSet {
  if (!statements) return 'all'
  const tables = new Set(tablesIn(statements).flatMap((table) => named(table).map((found) => found.id)))
  return union([tables, readsOfCalls(statements, catalog)])
}

/** The tables a function's `body` reads, with what the app's functions it calls read (`functionReads`). Every table
 *  when that can't be told: it doesn't parse, reads tables its SQL may not name, or names a table the catalog doesn't
 *  know (or a `WITH` query's name). */
function tablesReadByBody(
  body: Body | null,
  named: (table: RangeVar) => Table[],
  catalog: Catalog,
  functionReads: Map<string, TableSet>,
): TableSet {
  if (!body?.readsNamed) return 'all'
  const { statements } = body
  const tables = tablesIn(statements).map(named)
  if (tables.some((found) => found.length === 0)) return 'all'
  const called = catalog.calls(statements).filter((name) => catalog.functions.has(name))
  return union([
    new Set(tables.flat().map((table) => table.id)),
    ...called.map((name) => functionReads.get(name) ?? 'all'),
  ])
}

/**
 * The statements a function in `language` runs, a trigger's or one of the app's, by its `CREATE FUNCTION` (`definition`),
 * parsed. `null`, every table to `tablesReadByBody()` and `tablesWrittenBy()`, when they can't be told: it runs SQL built
 * at run time, isn't written in PL/pgSQL, or one of its statements doesn't parse.
 */
function statementsOfBody(language: string, definition: string): Body | null {
  const shown = language === 'plpgsql' ? statementsOf(definition) : null
  if (!shown) return null
  const parsed = shown.sql.map((sql) => parseStatements(sql))
  return parsed.includes(null) ? null : { statements: (parsed as Node[][]).flat(), readsNamed: shown.readsNamed }
}

/** The SQL a PL/pgSQL function runs, every expression as a `SELECT`, from its `CREATE FUNCTION`, whose signature
 *  declares its parameters, its result columns and how it returns, and whether it names every table the function reads
 *  (`READ_PLPGSQL_STATEMENTS`). `null` when it runs SQL built at run time. */
function statementsOf(definition: string): { sql: string[]; readsNamed: boolean } | null {
  let parsed: unknown
  try {
    parsed = parsePlPgSQLSync(definition)
  } catch {
    return null
  }
  const statements: string[] = []
  let dynamic = false
  let readsNamed = true
  walk(parsed, (key, value) => {
    if (key === 'PLpgSQL_stmt_dynexecute' || key === 'PLpgSQL_stmt_dynfors' || key === 'dynquery') dynamic = true
    if (key.startsWith('PLpgSQL_stmt_') && !READ_PLPGSQL_STATEMENTS.has(key)) readsNamed = false
    if (key !== 'PLpgSQL_expr') return
    const { query, parseMode } = value as { query: string; parseMode: number }
    if (parseMode === PARSE_STATEMENT) statements.push(query)
    else statements.push(`SELECT ${parseMode === PARSE_EXPRESSION ? query : query.replace(/^[^:=]*:?=/, '')}`)
  })
  return dynamic ? null : { sql: statements, readsNamed }
}

/**
 * The tables `statements` write, with what the writing functions they call write (`functionWrites`). Every table when
 * that can't be told: they don't parse, one is other than a query, a write or `NOTIFY`, writes a table the catalog doesn't
 * know, or calls one of `Catalog.writingFunctions` whose writes `functionWrites` doesn't hold.
 */
function tablesWrittenBy(
  statements: Node[] | null,
  named: (table: RangeVar) => Table[],
  catalog: Catalog,
  functionWrites: Map<string, TableSet>,
): TableSet {
  if (!statements || statements.some((s) => !KNOWN_STATEMENTS.has(Object.keys(s)[0]!))) return 'all'
  const written: Table[][] = []
  walk(statements, (key, value) => {
    if (WRITES.has(key)) written.push(named(value.relation))
  })
  if (written.some((tables) => tables.length === 0)) return 'all'
  const calls = catalog.calls(statements).filter((name) => catalog.writingFunctions.has(name))
  return union([
    new Set(written.flat().map((table) => table.id)),
    ...calls.map((name) => functionWrites.get(name) ?? 'all'),
  ])
}
