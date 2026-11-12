export { planSqlite }

import { tablesChangedBy, type Catalog, type Table } from '../catalog.js'
import {
  alsoWrites,
  HIDDEN_PREFIX,
  planRead,
  READS_EVERYTHING,
  type ChangedTables,
  type Plan,
  type WritePlan,
} from '../capture/statement.js'
import { quote } from '../tree.js'
import { SQLITE, numberParams, tokensOf } from './dialect.js'
import { hasConflictReplace } from './catalog.js'
import { CannotLower, identifier, Lowering, objectName, variant, writeTarget } from './lower.js'
import { parseSqlite, type Ast } from './parser.js'

/** Pragmas that only read the schema. */
const READING_PRAGMAS = new Set(['table_info', 'table_xinfo', 'table_list', 'index_list', 'index_info', 'index_xinfo'])
  .add('foreign_key_list')
  .add('database_list')
/** Pragmas that set how a connection runs, changing no row and no statement's meaning; `foreign_keys` too, turned on:
 *  the catalog of a connection without them knows no key, and every row a key's action changes is in the change log. */
const SETTING_PRAGMAS = new Set([
  'busy_timeout',
  'cache_size',
  'journal_mode',
  'synchronous',
  'temp_store',
  'mmap_size',
])
const ON = new Set(['on', 'true', 'yes', '1'])

/** What a statement the parser can't read may have changed: anything, a table or the schema. */
const ANYTHING: Plan = { kind: 'unknownWrite', tables: 'all', schemaChange: true, unread: true }

/** What a SQLite statement may change, and how to bound a read by its result. */
function planSqlite(text: string, catalog: Catalog): Plan {
  const pragma = planPragma(text)
  if (pragma) return pragma
  const numbered = numberParams(text)
  const statements = parseSqlite(numbered)
  // A statement the parser can't read may still run: it may have changed anything.
  if (!statements) return ANYTHING
  const lowering = new Lowering(numbered)
  if (statements.length !== 1)
    return { kind: 'several', plans: statements.map((s) => planOne(s, undefined, lowering, catalog)) }
  return planOne(statements[0]!, numbered, lowering, catalog)
}

/** A single `PRAGMA` statement, read from its tokens: the parser takes some of SQLite's spellings only (not
 *  `foreign_keys = ON`, nor `table_info(t)`). `undefined` for other text. */
function planPragma(text: string): Plan | undefined {
  const tokens = tokensOf(text).map((t) => t.word ?? t.text)
  if (tokens[0] !== 'pragma') return undefined
  if (tokens.at(-1) === ';') tokens.pop()
  const [name, ...rest] = tokens[2] === '.' ? tokens.slice(3) : tokens.slice(1)
  // `name`, `name = value` or `name(value)`.
  const argument = /^(?:=([^;]+)|\(([^;]+)\))?$/.exec(rest.join(''))
  if (!name || !argument) return undefined
  const [, assigned, called] = argument
  if (assigned === undefined && READING_PRAGMAS.has(name)) return { kind: 'none' }
  if (SETTING_PRAGMAS.has(name)) return { kind: 'none' }
  const value = assigned ?? called
  if (name === 'foreign_keys' && (value === undefined || ON.has(value))) return { kind: 'none' }
  return { kind: 'unknownWrite', tables: 'all', schemaChange: true }
}

/** `text`: the statement's whole text, its parameters numbered, when it can be sent rewritten. */
function planOne(statement: Ast, text: string | undefined, lowering: Lowering, catalog: Catalog): Plan {
  const [kind, node] = variant(statement)
  switch (kind) {
    case 'Query':
      return planSelect(node, text, lowering, catalog)
    case 'Insert':
    case 'Update':
    case 'Delete':
      return planWrite(statement, text, catalog)
    case 'StartTransaction':
      return { kind: 'transaction', action: 'begin' }
    case 'Commit':
      return { kind: 'transaction', action: 'commit' }
    case 'Rollback':
      if (!node.savepoint) return { kind: 'transaction', action: 'rollback' }
      return { kind: 'transaction', action: 'rollbackTo', savepoint: identifier(node.savepoint) }
    case 'Savepoint':
      return { kind: 'transaction', action: 'savepoint', savepoint: identifier(node.name) }
    case 'ReleaseSavepoint':
      return { kind: 'transaction', action: 'release', savepoint: identifier(node.name) }
    case 'Explain':
    case 'ExplainTable':
      return { kind: 'none' }
    case 'Pragma':
      if (!node.is_eq && READING_PRAGMAS.has(objectName(node.name).join('.'))) return { kind: 'none' }
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true }
    default:
      // Schema changes, `ATTACH`, `VACUUM`, …: anything may have changed.
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true }
  }
}

function planSelect(query: Ast, text: string | undefined, lowering: Lowering, catalog: Catalog): Plan {
  let select
  try {
    select = lowering.query(query)
  } catch (error) {
    if (!(error instanceof CannotLower)) throw error
    // A read all the same, of anything.
    return { kind: 'read', shape: READS_EVERYTHING, liveText: undefined, shareable: false }
  }
  const plan = planRead(select, text, catalog)
  if (!plan.liveText) return plan
  const { text: liveText, params } = SQLITE.bindParams(plan.liveText)
  return { ...plan, liveText, liveValues: params }
}

/** A write: its rows and those of its triggers and foreign keys are in the connection's change log, or, without one
 *  (D1), its `RETURNING` gives its rows. What `REPLACE` deletes is in neither: its tables count as changed whole. */
function planWrite(statement: Ast, text: string | undefined, catalog: Catalog): Plan {
  const target = writeTarget(statement)
  const table = target && catalog.find(target)
  const [, write] = variant(statement)
  const replaces = write.or === 'Replace' || !table || hasConflictReplace(table)
  const whole: ChangedTables = table ? tablesChangedBy([[table.id, undefined]], catalog) : 'all'
  if (catalog.changeLog) return { kind: 'logged', also: replaces || table?.virtual ? whole : new Set() }
  const widened =
    !replaces && table && text && !table.virtual && !table.isView && widenedWrite(statement, text, table, catalog)
  return widened || { kind: 'unknownWrite', tables: whole, schemaChange: false }
}

/** `statement` with an image of each row it writes appended to its `RETURNING` list: the new row, or the deleted one.
 *  An `UPDATE`'s old rows, and the rows an upsert conflicts with, are read first when its rows can be named the same
 *  way (`readsFirst`); otherwise an `UPDATE`'s assigned columns had unknown values, as on Postgres 17, and an upsert,
 *  whose `RETURNING` doesn't tell an inserted row from an updated one, counts its table whole. */
function widenedWrite(statement: Ast, text: string, table: Table, catalog: Catalog): WritePlan | undefined {
  const [kind, write] = variant(statement)
  const written = kind === 'Insert' ? 'insert' : kind === 'Update' ? 'update' : 'delete'
  const update = write.on?.OnConflict?.action?.DoUpdate
  if (write.on && !update && write.on.OnConflict?.action !== 'DoNothing') return undefined
  const assigned: string[] = []
  for (const { target } of (update ?? write).assignments ?? ([] as Ast[])) {
    const columns = target.ColumnName ? [target.ColumnName] : (target.Tuple as Ast[][] | undefined)
    if (!columns) return undefined
    for (const name of columns) assigned.push(objectName(name).at(-1)!)
  }
  const also = alsoWrites(table, update ? ['insert', 'update'] : [written], assigned, catalog)
  if (also === 'all') return undefined
  // `UPDATE t AS a … RETURNING a.x` fails: the image names its columns bare.
  const image = `json_array(${table.columns.map((c) => SQLITE.text(quote(c.name), c)).join(', ')}) AS "${HIDDEN_PREFIX}0"`
  const where =
    written === 'update' ? updatedRows(write, text, table, catalog) : update ? conflictingRows(write, table) : undefined
  if (update && where === undefined) return undefined
  const hadReturning = write.returning?.length > 0
  const appended = `${hadReturning ? ',' : ' RETURNING'} ${image}`
  const sent = SQLITE.bindParams(SQLITE.splice(text, SQLITE.statementEnd(text), appended))
  return {
    kind: 'write',
    table,
    statement: written,
    text: sent.text,
    params: sent.params,
    hadReturning,
    images: [written === 'delete' ? 'old' : 'new'],
    assigned,
    alsoWrites: also,
    ...(where !== undefined && { readsFirst: SQLITE.bindParams(`SELECT ${image} FROM ${where}`) }),
  }
}

/** The rows an `UPDATE` of `table` writes, named in its own words (`AS a WHERE …`), unless it has more than a `WHERE`
 *  (`FROM`, `ORDER BY`, `LIMIT`) or its `WHERE` calls a volatile function, which a second read could answer otherwise. */
function updatedRows(write: Ast, text: string, table: Table, catalog: Catalog): string | undefined {
  if (write.from || write.limit || write.order_by?.length) return undefined
  const tokens = tokensOf(text)
  let depth = 0
  let where: number | undefined
  let end = tokens.length
  for (const [i, token] of tokens.entries()) {
    if (token.text === '(') depth++
    else if (token.text === ')') depth--
    else if (depth > 0) continue
    else if (token.word === 'where') where = i
    else if (where !== undefined && (token.text === ';' || token.word === 'returning')) {
      end = i
      break
    }
  }
  const condition = where === undefined ? [] : tokens.slice(where, end)
  if (condition.some((t, i) => t.word && catalog.volatileFunctions.has(t.word) && condition[i + 1]?.text === '('))
    return undefined
  const { alias } = write.table.relation.Table
  const named = table.sqlName + (alias ? ` AS ${quote(identifier(alias.name))}` : '')
  return where === undefined ? named : `${named} ${text.slice(condition[0]!.start, condition.at(-1)!.end)}`
}

/** The rows an upsert of `VALUES` conflicts with, by the columns of its conflict target: each value a parameter or a
 *  literal. */
function conflictingRows(write: Ast, table: Table): string | undefined {
  const target = write.on.OnConflict.conflict_target?.Columns as Ast[] | undefined
  const rows = write.source?.body?.Values?.rows as Ast[] | undefined
  if (!target || !rows || write.source.order_by || write.source.limit_clause) return undefined
  const columns = (write.columns as Ast[][]).map((name) => objectName(name).at(-1)!)
  const at = target.map((column) => columns.indexOf(identifier(column)))
  if (at.includes(-1)) return undefined
  const values = rows.map((row) => at.map((i) => literalOf((row.content as Ast[])[i]!)))
  if (values.flat().includes(undefined)) return undefined
  const keys = target.map((column) => quote(identifier(column))).join(', ')
  const tuples = values.map((row) => `(${row.join(', ')})`).join(', ')
  return `${table.sqlName} WHERE (${keys}) IN (VALUES ${tuples})`
}

/** A parameter or a literal value, as written. */
function literalOf(expression: Ast): string | undefined {
  const value = expression.Value?.value
  if (typeof value?.Placeholder === 'string') return value.Placeholder
  if (value?.Number) return String(value.Number[0])
  if (typeof value?.SingleQuotedString === 'string') return `'${value.SingleQuotedString.replace(/'/g, "''")}'`
  if (value === 'Null') return 'NULL'
  return undefined
}
