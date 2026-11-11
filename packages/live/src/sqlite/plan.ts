export { planSqlite }

import { tablesChangedBy, type Catalog } from '../catalog.js'
import { planRead, READS_EVERYTHING, type ChangedTables, type Plan } from '../capture/statement.js'
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
      return planWrite(statement, catalog)
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

/** A write: its rows and those of its triggers and foreign keys are in the connection's change log, or, without one,
 *  its tables count as changed whole. What `REPLACE` deletes is in neither. */
function planWrite(statement: Cst, catalog: Catalog): Plan {
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
  if (!catalog.changeLog) return { kind: 'unknownWrite', tables: whole, schemaChange: false }
  return { kind: 'logged', also: replaces || table?.virtual ? whole : new Set() }
}
