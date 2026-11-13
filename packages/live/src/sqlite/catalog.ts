export { catalogQuery, catalogOf, changeLogSetup, CHANGE_LOG_TRIGGERS, CHANGE_COUNT, READ_CHANGES, changesOfLog }
export { hasConflictReplace }

import type { Access, Catalog, Table, TableSet } from '../catalog.js'
import { callsVolatile, tablesChangedBy, union } from '../catalog.js'
import { rowOf } from '../match/condition.js'
import type { Change } from '../match/liveRead.js'
import type { Column } from '../match/values.js'
import { names, quote, tablesIn, walk } from '../tree.js'
import { SQLITE, tokensOf } from './dialect.js'
import { AGGREGATES, CannotLower, identifier, Lowering, objectName, variant, writeTarget } from './lower.js'
import { parseSqlite, type Ast } from './parser.js'

type CatalogRow = {
  file: string | null
  foreignKeys: number
  tables: {
    name: string
    type: 'table' | 'view'
    sql: string | null
    columns: [name: string, type: string, primaryKey: number][]
    keys: [
      id: number,
      parent: string,
      column: string,
      parentColumn: string | null,
      onUpdate: string,
      onDelete: string,
    ][]
  }[]
  triggers: [table: string, sql: string][]
  /** Aggregate and window functions: a window-only one (`row_number()`) is never called without `OVER`. `null` on D1. */
  aggregates: string[] | null
}

const TABLES = `json((select json_group_array(json_object('name', m.name, 'type', m.type, 'sql', m.sql,
    'columns', json((select json_group_array(json_array(c.name, c.type, c.pk)) from pragma_table_xinfo(m.name) c where c.hidden <> 1)),
    'keys', json((select json_group_array(json_array(f.id, f."table", f."from", f."to", f.on_update, f.on_delete)) from pragma_foreign_key_list(m.name) f))))
  from sqlite_schema m where m.type in ('table', 'view') and m.name not like 'sqlite\\_%' escape '\\' and m.name not like '\\_cf\\_%' escape '\\'))`

/** One query answering everything the catalog needs; `introspects`: the database answers `pragma_database_list` and
 *  `pragma_function_list`, which D1 refuses. A subquery's JSON is embedded through `json()`: SQLite 3.54 embeds it as a
 *  string otherwise. */
const catalogQuery = (introspects: boolean) => `select json_object(
  'file', ${introspects ? `(select file from pragma_database_list where name = 'main')` : 'null'},
  'foreignKeys', (select foreign_keys from pragma_foreign_keys),
  'tables', ${TABLES},
  'triggers', json((select json_group_array(json_array(tbl_name, sql)) from sqlite_schema where type = 'trigger')),
  'aggregates', ${introspects ? `json((select json_group_array(distinct name) from pragma_function_list where type in ('a', 'w')))` : 'null'}
) as catalog`

/** SQLite's own aggregates, for D1, which doesn't tell its functions and runs none of the app's. */
const BUILT_IN_AGGREGATES = [
  'avg',
  'count',
  'group_concat',
  'string_agg',
  'max',
  'min',
  'sum',
  'total',
  'json_group_array',
  'json_group_object',
  'jsonb_group_array',
  'jsonb_group_object',
]

/** The temporary table each connection's triggers log the rows its statements change into. */
const CHANGES = 'temp."__live_changes"'

/** The names of the change log's triggers on a connection, from a previous catalog, as JSON. */
const CHANGE_LOG_TRIGGERS = `select json_group_array(name) from temp.sqlite_schema where type = 'trigger' and name like '\\_\\_live\\_%' escape '\\'`

/** The connection's count of changes, which keys the log's rows (the count once a row is logged): a rollback doesn't
 *  take it back. */
const CHANGE_COUNT = 'SELECT total_changes()'
/** The rows logged after a count, and the count now: a `SELECT`, which leaves `changes()` to the app's statements. */
const READ_CHANGES = `SELECT "id", "table", "kind", "old", "new", total_changes() FROM ${CHANGES} WHERE "id" > ? ORDER BY "id"`
/** A write changing more rows counts as changing its tables whole. Fewer than `MAX_CHANGES`: the log holds about as many
 *  at rest, and its size slows every write (10,000 rows cost a write 20 to 30 %). */
const LOGGED_ROWS = 1_000
/** The log keeps the rows logged within the connection's last this many changes: each trigger deletes the older ones,
 *  a change of its own that `changes()` doesn't count. A logged row takes up to three: its change, its log row and the
 *  log row it deletes. */
const LOGGED_CHANGES = 3 * LOGGED_ROWS

/** SQLite has no roles: a connection reads every table whole. */
const READABLE: Access = { readable: 'table', rowSecurity: false }

const VOLATILE_FUNCTIONS = ['random', 'randomblob', 'changes', 'total_changes', 'last_insert_rowid', 'date', 'time']
  .concat(['datetime', 'julianday', 'unixepoch', 'strftime', 'timediff', 'current_timestamp', 'current_date'])
  .concat(['current_time'])

/** SQLite's names for a column's affinity: its type name decides it, and `ANY` in a `STRICT` table has none (`blob`). */
function affinityOf(type: string, strict: boolean): 'integer' | 'text' | 'blob' | 'real' | 'numeric' {
  const t = type.toUpperCase()
  if (strict && t === 'ANY') return 'blob'
  if (t.includes('INT')) return 'integer'
  if (t.includes('CHAR') || t.includes('CLOB') || t.includes('TEXT')) return 'text'
  if (t === '' || t.includes('BLOB')) return 'blob'
  // spellcheck-ignore: `DOUB` is in SQLite's rule
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB')) return 'real'
  return 'numeric'
}

/** A column's values as the matcher compares them: by its affinity, and for text by its collation. */
function columnOf(name: string, type: string, collation: string | undefined, strict: boolean): Column {
  const affinity = affinityOf(type, strict)
  const nocase = collation === 'nocase'
  const text = affinity === 'text'
  return {
    name,
    // A column without affinity holds values of any storage class, which compare by class first.
    category: text ? 'S' : affinity === 'blob' ? 'X' : 'N',
    // Two columns compare as one type only with the same affinity and collation.
    type: `${affinity}${collation && text ? ` collate ${collation}` : ''}`,
    sqlType: affinity,
    base: ['main', affinity],
    orderableText: true,
    // `RTRIM` and collations of the app's call different strings equal in ways the matcher doesn't reproduce.
    deterministic: !text || collation === undefined || collation === 'binary' || nocase,
    ...(nocase && text ? { fold: 'ascii' as const } : {}),
  }
}

/** `changeLog`: the connections log the rows their statements change. */
function catalogOf(answer: string, changeLog: boolean): Catalog {
  const row = JSON.parse(answer) as CatalogRow
  const tables = new Map<string, Table>()
  const catalog: Catalog = {
    dialect: SQLITE,
    // `file` is '' for a database in memory or in a temporary file, and `null` on D1, which doesn't tell.
    database: row.file === '' ? undefined : (row.file ?? 'main'),
    role: '',
    access: new Map(),
    serverVersion: 0,
    tables,
    functions: new Set(),
    writingFunctions: new Set(),
    volatileWritingFunctions: new Set(),
    calls: (node) => {
      const called: string[] = []
      walk(node, (key, call) => {
        if (key === 'FuncCall') called.push(names(call.funcname).at(-1)!)
      })
      return called
    },
    volatileFunctions: new Set(VOLATILE_FUNCTIONS),
    // By the names the lowering calls them, too.
    aggregates: new Set((row.aggregates ?? BUILT_IN_AGGREGATES).flatMap((name) => [name, AGGREGATES[name] ?? name])),
    searchPath: ['main'],
    domainBases: () => [],
    find(table) {
      const named = this.named(table)
      return named.length === 1 ? named[0] : undefined
    },
    named: (table) => {
      const found = (!table.schemaname || table.schemaname === 'main') && tables.get(`main.${table.relname}`)
      return found ? [found] : []
    },
    changeLog,
  }
  const definitions = new Map<string, Definition>()
  for (const t of row.tables) {
    const name = lowerCase(t.name)
    const definition = t.type === 'table' ? definitionOf(t.sql) : undefined
    definitions.set(`main.${name}`, definition ?? { deferred: true })
    const columns = t.columns.map(([column, type]) =>
      columnOf(
        lowerCase(column),
        type,
        definition ? (definition.collations?.get(lowerCase(column)) ?? 'binary') : 'unknown',
        // A table whose definition can't be read may be `STRICT`.
        definition?.strict !== false,
      ),
    )
    const table: Table = {
      id: `main.${name}`,
      sqlName: `${quote('main')}.${quote(name)}`,
      columns,
      column: new Map(columns.map((c) => [c.name, c])),
      primaryKey: t.columns
        .filter(([, , primaryKey]) => primaryKey > 0)
        .sort(([, , a], [, , b]) => a - b)
        .map(([column]) => lowerCase(column)),
      isView: t.type === 'view',
      alsoReads: new Set(),
      alsoWrites: { insert: new Set(), update: new Set(), delete: new Set(), truncate: new Set() },
      referencedBy: [],
      references: [],
      updateChangesOtherColumns: columns.length !== t.columns.length || definition?.generated === true,
      conflictReplace: definition?.conflictReplace !== false,
      failKeepsChanges: definition?.conflictFail !== false,
      virtual: definition?.virtual === true,
    }
    tables.set(table.id, table)
    catalog.access.set(table.id, READABLE)
  }
  // Second pass: these name other tables, which must all be known first.
  for (const t of row.tables) {
    const table = tables.get(`main.${lowerCase(t.name)}`)!
    if (t.type === 'view') {
      Object.assign(table, readOfView(t.sql, catalog))
      for (const write of ['insert', 'update', 'delete'] as const) table.alsoWrites[write] = 'all'
    }
    // Foreign keys act only while `PRAGMA foreign_keys` is on.
    for (const key of row.foreignKeys ? keysOf(t.keys) : []) {
      const parent = tables.get(`main.${lowerCase(key.parent)}`)
      if (!parent) continue
      const parentColumn = key.column && (key.parentColumn ?? parent.primaryKey[0])
      const columns = key.column && parentColumn ? { column: key.column, parentColumn } : {}
      parent.referencedBy.push({ table: table.id, ...columns, onDelete: key.onDelete, onUpdate: key.onUpdate })
      // A deferred key is checked when its transaction commits: until then a row may reference one not inserted yet.
      if (columns.column && !definitions.get(table.id)?.deferred)
        table.references.push({ column: columns.column, parent: parent.id, parentColumn: columns.parentColumn! })
    }
  }
  for (const [tableName, sql] of row.triggers) {
    const table = tables.get(`main.${lowerCase(tableName)}`)
    if (!table) continue
    const { events, written } = triggerOf(sql, catalog)
    if (wordsAfter(wordsOf(sql), 'raise').includes('fail')) table.failKeepsChanges = true
    for (const write of events) table.alsoWrites[write] = union([table.alsoWrites[write], written])
    if (events.includes('update') && written !== 'all' && written.has(table.id)) table.updateChangesOtherColumns = true
  }
  return catalog
}

type Definition = {
  collations?: Map<string, string>
  generated?: boolean
  /** A constraint's conflict clause is `REPLACE`: an `INSERT` or `UPDATE` may delete the rows it conflicts with. */
  conflictReplace?: boolean
  /** A constraint's conflict clause is `FAIL`: a write that fails keeps the rows it changed before. */
  conflictFail?: boolean
  virtual?: boolean
  /** A foreign key may be `DEFERRABLE INITIALLY DEFERRED`. */
  deferred?: boolean
  /** `STRICT`: an `ANY` column has no affinity. */
  strict?: boolean
}

/** What a table's `CREATE TABLE` tells beyond the pragmas; `undefined` when it can't be read. */
function definitionOf(sql: string | null): Definition | undefined {
  if (!sql) return undefined
  const words = wordsOf(sql)
  const conflicts = wordsAfter(words, 'conflict')
  const conflictReplace = conflicts.includes('replace')
  const conflictFail = conflicts.includes('fail')
  const deferred = words.includes('deferred')
  if (words[1] === 'virtual') return { virtual: true, conflictReplace, conflictFail, deferred }
  const table = parseSqlite(sql)?.[0]?.CreateTable
  if (!table) return undefined
  const collations = new Map<string, string>()
  let generated = false
  for (const column of table.columns as Ast[]) {
    for (const { option } of column.options as Ast[]) {
      if (option.Collation) collations.set(identifier(column.name), objectName(option.Collation).join('.'))
      if (option.Generated) generated = true
    }
  }
  return { collations, generated, conflictReplace, conflictFail, deferred, strict: table.strict === true }
}

/** The words of `sql`, in lower case. */
function wordsOf(sql: string): string[] {
  return tokensOf(sql).flatMap((t) => (t.word ? [t.word] : []))
}

/** The word right after each `word` (`ON CONFLICT FAIL`, `RAISE(FAIL, …)`). */
function wordsAfter(words: string[], word: string): string[] {
  return words.flatMap((w, i) => (w === word && words[i + 1] ? [words[i + 1]!] : []))
}

/** `column` and `parentColumn` only for a key of one column. */
type Key = { parent: string; column?: string; parentColumn?: string; onDelete: string; onUpdate: string }

const ACTIONS: Record<string, string> = { CASCADE: 'c', 'SET NULL': 'n', 'SET DEFAULT': 'd' }

/** A table's foreign keys of one column (`pragma_foreign_key_list` lists a key of several columns once per column). */
function keysOf(rows: CatalogRow['tables'][number]['keys']): Key[] {
  const columnsPerKey = new Map<number, number>()
  for (const [id] of rows) columnsPerKey.set(id, (columnsPerKey.get(id) ?? 0) + 1)
  return rows.flatMap(([id, parent, column, parentColumn, onUpdate, onDelete], i): Key[] => {
    if (rows.findIndex(([other]) => other === id) !== i) return []
    const single = columnsPerKey.get(id) === 1
    return [
      {
        parent,
        ...(single ? { column: lowerCase(column) } : {}),
        ...(single && parentColumn ? { parentColumn: lowerCase(parentColumn) } : {}),
        onDelete: ACTIONS[onDelete] ?? 'a',
        onUpdate: ACTIONS[onUpdate] ?? 'a',
      },
    ]
  })
}

/** What a read of a view also reads (`Table.alsoReads`), and whether it calls a volatile function (`Table.volatile`). */
function readOfView(sql: string | null, catalog: Catalog): { alsoReads: TableSet; volatile: boolean } {
  const view = sql ? parseSqlite(sql)?.[0]?.CreateView : undefined
  if (!view) return { alsoReads: 'all', volatile: true }
  try {
    const query = new Lowering(sql!).query(view.query)
    const tables = tablesIn(query).map((t) => catalog.find(t))
    const alsoReads = tables.every(Boolean) ? new Set(tables.map((t) => t!.id)) : 'all'
    return { alsoReads, volatile: callsVolatile(query, catalog) }
  } catch (error) {
    if (error instanceof CannotLower) return { alsoReads: 'all', volatile: true }
    throw error
  }
}

/** The writes that fire a trigger, and the tables its statements write (`'all'` when that can't be told). */
function triggerOf(sql: string, catalog: Catalog): { events: ('insert' | 'update' | 'delete')[]; written: TableSet } {
  const trigger = parseSqlite(sql)?.[0]?.CreateTrigger
  const statements = trigger?.statements?.BeginEnd?.statements as Ast[] | undefined
  if (!statements) return { events: ['insert', 'update', 'delete'], written: 'all' }
  const events = (trigger.events as Ast[]).map((e) => variant(e)[0].toLowerCase() as 'insert' | 'update' | 'delete')
  const written = new Set<string>()
  for (const body of statements) {
    if (body.Query) continue
    const target = writeTarget(body)
    const table = target && catalog.find(target)
    if (!table) return { events, written: 'all' }
    written.add(table.id)
  }
  return {
    events,
    written: tablesChangedBy(
      [...written].map((id) => [id, undefined]),
      catalog,
    ),
  }
}

/** The statements creating the change log on a connection, and its triggers on every table but virtual ones; `ours`:
 *  the connection's triggers of a previous catalog (`CHANGE_LOG_TRIGGERS`), dropped first. A row's image names its
 *  columns: one the table no longer has, or doesn't have yet, is unknown. */
function changeLogSetup(catalog: Catalog, ours: string[]): string[] {
  const drops = ours.map((name) => `DROP TRIGGER IF EXISTS temp.${quote(name)}`)
  const creates = [...catalog.tables.values()].flatMap((table) => {
    if (table.isView || table.virtual) return []
    const image = (row: string) =>
      `json_object(${table.columns.map((c) => `${sqlString(c.name)}, ${SQLITE.text(`${row}.${quote(c.name)}`, c)}`).join(', ')})`
    const images = {
      insert: ['NULL', image('NEW')],
      update: [image('OLD'), image('NEW')],
      delete: [image('OLD'), 'NULL'],
    }
    return Object.entries(images).map(
      ([kind, [old, now]]) =>
        `CREATE TEMP TRIGGER ${quote(`__live_${table.id}_${kind}`)} AFTER ${kind.toUpperCase()} ON ${table.sqlName} BEGIN ` +
        `INSERT INTO ${CHANGES} ("id", "table", "kind", "old", "new") VALUES (total_changes() + 1, ${sqlString(table.id)}, '${kind}', ${old}, ${now}); ` +
        `DELETE FROM ${CHANGES} WHERE "id" <= total_changes() - ${LOGGED_CHANGES}; END`,
    )
  })
  return [
    `CREATE TEMP TABLE IF NOT EXISTS ${CHANGES} ("id" INTEGER PRIMARY KEY, "table" TEXT, "kind" TEXT, "old" TEXT, "new" TEXT)`,
    ...drops,
    ...creates,
  ]
}

/** The changes `READ_CHANGES` gave after the count `after`; `undefined` past `LOGGED_ROWS`, or when the log may no
 *  longer hold some of them. */
function changesOfLog(rows: (string | null)[][], after: number, catalog: Catalog): Change[] | undefined {
  const last = rows.at(-1)
  if (rows.length > LOGGED_ROWS || (last && Number(last[0]) - after > LOGGED_CHANGES)) return undefined
  return rows.map(([, table, kind, old, now]): Change => {
    const image = (text: string | null | undefined) => rowOf(Object.entries(JSON.parse(text!)))
    if (!catalog.tables.has(table!)) return { table: table!, kind: 'unknown' }
    if (kind === 'insert') return { table: table!, kind: 'insert', new: image(now) }
    if (kind === 'delete') return { table: table!, kind: 'delete', old: image(old) }
    return { table: table!, kind: 'update', old: image(old), new: image(now) }
  })
}

/** Whether a write to `table` may delete rows it conflicts with (`REPLACE`), which neither the change log nor
 *  `RETURNING` reports. */
function hasConflictReplace(table: Table): boolean {
  return table.conflictReplace === true
}

function sqlString(text: string): string {
  return `'${text.replace(/'/g, "''")}'`
}

function lowerCase(name: string): string {
  return name.replace(/[A-Z]/g, (c) => c.toLowerCase())
}
