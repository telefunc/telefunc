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
import { quote } from '../sql.js'
import { SQLITE, numberParams } from './dialect.js'
import { hasConflictReplace } from './catalog.js'
import { CannotLower, identifier, lowerSelect, parseSqlite, type Cst } from './lower.js'

/** Pragmas that only read the schema. */
const READING_PRAGMAS = new Set(['table_info', 'table_xinfo', 'table_list', 'index_list', 'index_info', 'index_xinfo'])
  .add('foreign_key_list')
  .add('database_list')

/** What a statement the parser can't read may have changed: anything, a table or the schema. */
const ANYTHING: Plan = { kind: 'unknownWrite', tables: 'all', schemaChange: true, unread: true }

/** What a SQLite statement may change, and how to bound a read by its result. */
function planSqlite(text: string, catalog: Catalog): Plan {
  const numbered = numberParams(text)
  const statements = parseSqlite(numbered.text)?.filter((s) => s.type !== 'empty')
  // A statement the parser can't read may still run: it may have changed anything.
  if (!statements) return ANYTHING
  if (statements.length !== 1) return { kind: 'several', plans: statements.map((s) => planOne(s, undefined, catalog)) }
  // A driver binds named parameters by name: the text sent with hidden columns would take them by position.
  return planOne(statements[0]!, numbered.named ? undefined : numbered.text, catalog)
}

/** `text`: the statement's whole text, its parameters numbered, when it can be sent rewritten. */
function planOne(statement: Cst, text: string | undefined, catalog: Catalog): Plan {
  switch (statement.type) {
    case 'select_stmt':
    case 'compound_select_stmt':
      return planSelect(statement, text, catalog)
    case 'insert_stmt':
    case 'update_stmt':
    case 'delete_stmt':
      return planWrite(statement, text, catalog)
    case 'start_transaction_stmt':
      return { kind: 'transaction', action: 'begin' }
    case 'commit_transaction_stmt':
      return { kind: 'transaction', action: 'commit' }
    case 'rollback_transaction_stmt':
      if (!statement.savepoint) return { kind: 'transaction', action: 'rollback' }
      return { kind: 'transaction', action: 'rollbackTo', savepoint: identifier(statement.savepoint.savepoint) }
    case 'savepoint_stmt':
      return { kind: 'transaction', action: 'savepoint', savepoint: identifier(statement.savepoint) }
    case 'release_savepoint_stmt':
      return { kind: 'transaction', action: 'release', savepoint: identifier(statement.savepoint) }
    case 'explain_stmt':
      return { kind: 'none' }
    case 'pragma_stmt':
      if (statement.pragma.type === 'pragma_func_call' && READING_PRAGMAS.has(identifier(statement.pragma.name)))
        return { kind: 'none' }
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true }
    default:
      // Schema changes, `ATTACH`, `VACUUM`, …: anything may have changed.
      return { kind: 'unknownWrite', tables: 'all', schemaChange: true }
  }
}

function planSelect(statement: Cst, text: string | undefined, catalog: Catalog): Plan {
  let select
  try {
    select = lowerSelect(statement)
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
function planWrite(statement: Cst, text: string | undefined, catalog: Catalog): Plan {
  const clause = statement.clauses[0] as Cst
  const target: Cst | undefined = clause.table ?? clause.tables?.items?.[0]
  const named = target?.type === 'alias' ? target.expr : target
  const table =
    named?.type === 'identifier'
      ? catalog.find({ relname: identifier(named) })
      : named?.type === 'member_expr' && named.object.type === 'identifier' && named.property.type === 'identifier'
        ? catalog.find({ schemaname: identifier(named.object), relname: identifier(named.property) })
        : undefined
  const replaces =
    clause.insertKw?.name === 'REPLACE' ||
    clause.orAction?.actionKw?.name === 'REPLACE' ||
    !table ||
    hasConflictReplace(table)
  const whole: ChangedTables = table ? tablesChangedBy([[table.id, undefined]], catalog) : 'all'
  if (catalog.changeLog) return { kind: 'logged', also: replaces || table?.virtual ? whole : new Set() }
  const widened =
    !replaces && table && text && !table.virtual && !table.isView && widenedWrite(statement, text, table, catalog)
  return widened || { kind: 'unknownWrite', tables: whole, schemaChange: false }
}

/** `statement` with an image of each row it writes appended to its `RETURNING` list: the new row, or the deleted one.
 *  Without the old row, an `UPDATE`'s assigned columns had unknown values, as on Postgres 17. */
function widenedWrite(statement: Cst, text: string, table: Table, catalog: Catalog): WritePlan | undefined {
  const kind = statement.type === 'insert_stmt' ? 'insert' : statement.type === 'update_stmt' ? 'update' : 'delete'
  const clauses = statement.clauses as Cst[]
  // An upsert's `RETURNING` doesn't tell an inserted row from an updated one.
  if (clauses.some((c) => c.type === 'upsert_clause' && c.action.type !== 'upsert_action_nothing')) return undefined
  const assigned: string[] = []
  for (const assignment of clauses.find((c) => c.type === 'set_clause')?.assignments.items ?? []) {
    const { column } = assignment as Cst
    const named = column.type === 'paren_expr' ? column.expr.items : [column]
    for (const c of named as Cst[]) {
      if (c.type !== 'identifier') return undefined
      assigned.push(identifier(c))
    }
  }
  const also = alsoWrites(table, [kind], assigned, catalog)
  if (also === 'all') return undefined
  const hadReturning = clauses.some((c) => c.type === 'returning_clause')
  // `UPDATE t AS a … RETURNING a.x` fails: the image names its columns bare.
  const image = `json_array(${table.columns.map((c) => SQLITE.text(quote(c.name))).join(', ')})`
  const appended = `${hadReturning ? ',' : ' RETURNING'} ${image} AS "${HIDDEN_PREFIX}0"`
  return {
    kind: 'write',
    table,
    statement: kind,
    text: SQLITE.bindParams(SQLITE.splice(text, SQLITE.statementEnd(text), appended)).text,
    hadReturning,
    images: [kind === 'delete' ? 'old' : 'new'],
    assigned,
    alsoWrites: also,
  }
}
