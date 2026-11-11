export { POSTGRES }

import type { Dialect } from './dialect.js'
import { postgresOperand } from './match/operands.js'
import { fromClauseAt, parenthesizedAt, renumberParams, spliceBytes, statementEnd, topLevelFrom } from './sql.js'

/** Postgres's SQL, read with libpg-query's scanner: offsets are UTF-8 byte offsets. */
const POSTGRES: Dialect = {
  topLevelFrom,
  fromClauseAt,
  parenthesizedAt,
  statementEnd,
  splice: spliceBytes,
  bindParams: renumberParams,
  param: (number) => `$${number}`,
  likeEscape: '\\',
  text: (expression) => `${expression}::text`,
  jsonArray: (expressions) => `to_json(ARRAY[${expressions.join(', ')}])::text`,
  jsonAgg: (expression) => `coalesce(to_json(array_agg(${expression})), '[]')::text`,
  operand: postgresOperand,
}
