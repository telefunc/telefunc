export { Operands, operandValues, isOperand }
export type { Operand, OperandValue, Comparison }

import { names, quote, renumberParams, type Node } from '../sql.js'

/**
 * A parameter or constant a predicate compares a column with, casts included. Its value is the one Postgres gives it:
 * the same text can be several values (`'{A0EE…}'` and `'a0ee…'` are one `uuid`, `'ab  '` and `'ab'` one `char(4)`,
 * `$1::varchar(3)` cuts its parameter short), so the read asks Postgres, once, for each operand's text form.
 */
type Operand = {
  /** As written: what a query of ours compares a column with. */
  expression: string
  /** Its value in Postgres's text form, coerced as the comparison coerces it; for `= ANY(…)`, a JSON array of its
   *  elements' (`NULL` for a `NULL` array). */
  evaluated: string
  list: boolean
}

/** An operand's value as Postgres gave it: its text, `null`, or for `= ANY(…)` the values listed (`null` for a `NULL`
 *  array); `undefined` while unknown. */
type OperandValue = string | null | readonly (string | null)[] | undefined

/** How a predicate compares an operand: with a value of SQL type `as` (an untyped operand, a parameter or a quoted
 *  constant, is read as that type), as a `LIKE` pattern, element by element (`= ANY(…)`), or only for `NULL`. */
type Comparison = { as?: string; form?: 'pattern' | 'list' | 'nullness' }

/** The operands of one statement, each once. */
class Operands {
  readonly list: Operand[] = []

  /** The index of `node` as an operand compared as `comparison` says, or `undefined` when Postgres can't evaluate it
   *  alone: it reads a column, calls a function, or its type isn't known. */
  of(node: Node, comparison: Comparison): number | undefined {
    const expression = sqlOf(node)
    const evaluated = expression === undefined ? undefined : evaluation(expression, node, comparison)
    if (evaluated === undefined) return undefined
    const known = this.list.findIndex((operand) => operand.evaluated === evaluated)
    if (known !== -1) return known
    return this.list.push({ expression: expression!, evaluated, list: comparison.form === 'list' }) - 1
  }

  /** The query giving every operand's value, with the read's parameters at `params`. */
  query(): { text: string; params: number[] } | undefined {
    if (this.list.length === 0) return undefined
    return renumberParams(`SELECT ${this.list.map((operand) => operand.evaluated).join(', ')}`)
  }
}

/** The operands' values from the row `Operands.query()` returned; `undefined` when it isn't one it returns. */
function operandValues(operands: Operand[], row: readonly (string | null)[]): OperandValue[] | undefined {
  if (row.length !== operands.length) return undefined
  try {
    return operands.map((operand, i) =>
      operand.list && row[i] !== null ? (JSON.parse(row[i]!) as (string | null)[]) : row[i],
    )
  } catch {
    return undefined
  }
}

function evaluation(expression: string, node: Node, { as, form }: Comparison): string | undefined {
  if (form === 'nullness') return `CASE WHEN (${expression}) IS NULL THEN NULL ELSE '' END`
  if (form === 'list') {
    if (!isTyped(node) && !as) return undefined
    const array = isTyped(node) ? `(${expression})` : `(${expression})::${as}[]`
    // Each element's text form, cast or not; a `NULL` array gives no row: `NULL`.
    return `(SELECT to_json(array(SELECT "v"::text FROM unnest("a") AS "v"))::text FROM (SELECT ${array} AS "a") AS "l" WHERE "a" IS NOT NULL)`
  }
  if (form === 'pattern' || isTyped(node)) return `(${expression})::text`
  if (!as) return undefined
  return `((${expression})::${as})::text`
}

/** A parameter or constant, under casts or not. */
function isOperand(node: Node): boolean {
  if (node.TypeCast) return isOperand(node.TypeCast.arg)
  return Boolean(node.ParamRef || node.A_Const)
}

/** A cast, or a number or boolean constant: its type doesn't come from what it's compared with. */
function isTyped(node: Node): boolean {
  if (node.TypeCast) return true
  const constant = node.A_Const
  return Boolean(constant && (constant.ival || constant.fval || constant.boolval))
}

/** `node` as SQL, when it's a parameter, a constant or a cast of one. */
function sqlOf(node: Node): string | undefined {
  if (node.ParamRef) return `$${node.ParamRef.number}`
  if (node.A_Const) return constantSql(node.A_Const)
  if (!node.TypeCast) return undefined
  const value = sqlOf(node.TypeCast.arg)
  const type = typeSql(node.TypeCast.typeName)
  return value === undefined || type === undefined ? undefined : `(${value})::${type}`
}

function constantSql(constant: Node): string | undefined {
  if (constant.isnull) return 'NULL'
  if (constant.ival) return String(constant.ival.ival ?? 0)
  if (constant.fval) return constant.fval.fval
  if (constant.boolval) return constant.boolval.boolval ? 'TRUE' : 'FALSE'
  if (constant.sval) return `'${(constant.sval.sval ?? '').replace(/'/g, "''")}'`
  return undefined
}

function typeSql(type: Node): string | undefined {
  if (type.setof || type.pct_type) return undefined
  const name = names(type.names)
  const modifiers = (type.typmods ?? []) as Node[]
  // An interval's modifiers name its fields, which a type name can't spell back.
  if (modifiers.length > 0 && name.at(-1) === 'interval') return undefined
  if (modifiers.some((m) => !m.A_Const?.ival)) return undefined
  const spelled = modifiers.length > 0 ? `(${modifiers.map((m) => m.A_Const.ival.ival ?? 0).join(', ')})` : ''
  return `${name.map(quote).join('.')}${spelled}${'[]'.repeat((type.arrayBounds ?? []).length)}`
}
