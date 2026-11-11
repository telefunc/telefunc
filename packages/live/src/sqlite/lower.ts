export { parseSqlite, lowerSelect, lowerExpression, identifier, CannotLower }
export type { Cst }

import { parse } from 'sql-parser-cst'
import type { Node } from '../sql.js'

/** A node of sql-parser-cst's SQLite tree. */
type Cst = Record<string, any>

/** A construct the lowering doesn't know: the statement is read as reading, or writing, anything. */
class CannotLower extends Error {}

/** The statements of `text` (its parameters numbered, `numberParams()`), or `null` when the parser can't read it. */
function parseSqlite(text: string): Cst[] | null {
  try {
    return parse(text, { dialect: 'sqlite', paramTypes: ['?nr'], includeRange: true }).statements
  } catch {
    return null
  }
}

/**
 * Lowers a SQLite `SELECT` into the Postgres parse tree the matcher reads (`SelectStmt`). A construct maps to the
 * Postgres node that means the same, and one whose meaning differs (`GLOB`, a cast, scalar `max(a, b)`, …) to a call
 * of a function the catalog doesn't know, which the matcher can't evaluate: its column references still count as
 * read. Any other construct throws `CannotLower`.
 *
 * | SQLite | Postgres node | Why it means the same |
 * |---|---|---|
 * | `x LIKE p`, `x NOT LIKE p` | `A_Expr AEXPR_ILIKE` | SQLite's `LIKE` ignores ASCII case (the matcher folds ASCII only) |
 * | `ORDER BY x [ASC]`, `ORDER BY x DESC` | `SortBy` with `SORTBY_NULLS_FIRST`, `SORTBY_NULLS_LAST` | SQLite sorts `NULL` first |
 * | `a op b op c` (compound) | `SelectStmt` nested to the left | SQLite runs compound operators from left to right |
 * | the last `ORDER BY` and `LIMIT` of a compound | on the compound | they order and limit the whole compound |
 * | `total(x)`, `group_concat`, `json_group_array`, `json_group_object` | `sum`, `string_agg`, `json_agg`, `json_object_agg` | aggregates |
 * | `max(a, b, …)`, `min(a, b, …)` | an unknown function | scalar functions on SQLite |
 * | `"name"`, `` `name` ``, `[name]`, `name` | `ColumnRef`, `RangeVar` with the name in ASCII lower case | SQLite names ignore ASCII case |
 */
function lowerSelect(cst: Cst): Node {
  if (cst.type === 'compound_select_stmt') return lowerCompound(cst)
  if (cst.type === 'paren_expr' && isSelect(cst.expr)) return lowerSelect(cst.expr)
  if (cst.type !== 'select_stmt') throw new CannotLower(cst.type)
  const select: Node = { op: 'SETOP_NONE' }
  for (const clause of cst.clauses as Cst[]) {
    switch (clause.type) {
      case 'with_clause':
        select.withClause = {
          recursive: clause.recursiveKw !== undefined,
          ctes: (clause.tables.items as Cst[]).map((cte) => {
            if (cte.columns) throw new CannotLower('cte columns')
            return {
              CommonTableExpr: { ctename: identifier(cte.table), ctequery: { SelectStmt: lowerSelect(cte.expr) } },
            }
          }),
        }
        break
      case 'select_clause':
        for (const modifier of clause.modifiers as Cst[]) {
          if (modifier.type === 'select_distinct') select.distinctClause = [{}]
          else if (modifier.type !== 'select_all') throw new CannotLower(modifier.type)
        }
        select.targetList = (clause.columns.items as Cst[]).map((item) =>
          item.type === 'alias'
            ? { ResTarget: { name: identifier(item.alias), val: lowerExpression(item.expr) } }
            : { ResTarget: { val: lowerExpression(item) } },
        )
        break
      case 'from_clause':
        select.fromClause = fromItems(clause.expr)
        break
      case 'where_clause':
        select.whereClause = lowerExpression(clause.expr)
        break
      case 'group_by_clause':
        select.groupClause = (clause.columns.items as Cst[]).map(lowerExpression)
        break
      case 'having_clause':
        select.havingClause = lowerExpression(clause.expr)
        break
      case 'window_clause':
        select.windowClause = [{}]
        break
      case 'order_by_clause':
        select.sortClause = (clause.specifications.items as Cst[]).map(sortBy)
        break
      case 'limit_clause':
        select.limitCount = lowerExpression(clause.count)
        if (clause.offset) select.limitOffset = lowerExpression(clause.offset)
        break
      default:
        throw new CannotLower(clause.type)
    }
  }
  return select
}

const SET_OPERATIONS: Record<string, string> = {
  UNION: 'SETOP_UNION',
  INTERSECT: 'SETOP_INTERSECT',
  EXCEPT: 'SETOP_EXCEPT',
}

/** `a op b op c`: SQLite runs it from the left, whatever the parser nested. */
function lowerCompound(cst: Cst): Node {
  const branches: Cst[] = []
  const operators: string[][] = []
  const flatten = (node: Cst) => {
    if (node.type !== 'compound_select_stmt') return void branches.push(node)
    flatten(node.left)
    operators.push(keywords(node.operator))
    flatten(node.right)
  }
  flatten(cst)
  // The last branch's `ORDER BY` and `LIMIT` order and limit the whole compound.
  const last = branches.at(-1)!
  const ofCompound = (clause: Cst) => clause.type === 'order_by_clause' || clause.type === 'limit_clause'
  branches[branches.length - 1] = { ...last, clauses: last.clauses.filter((c: Cst) => !ofCompound(c)) }
  let tree = lowerSelect(branches[0]!)
  operators.forEach((operator, i) => {
    const op = SET_OPERATIONS[operator[0]!]
    if (!op || operator.length > 2 || (operator.length === 2 && operator[1] !== 'ALL'))
      throw new CannotLower(operator.join(' '))
    tree = { op, all: operator[1] === 'ALL', larg: tree, rarg: lowerSelect(branches[i + 1]!) }
  })
  for (const clause of last.clauses as Cst[]) {
    if (clause.type === 'order_by_clause') tree.sortClause = (clause.specifications.items as Cst[]).map(sortBy)
    if (clause.type !== 'limit_clause') continue
    tree.limitCount = lowerExpression(clause.count)
    if (clause.offset) tree.limitOffset = lowerExpression(clause.offset)
  }
  return tree
}

function fromItems(cst: Cst): Node[] {
  // `a, b`: each its own item.
  if (cst.type === 'join_expr' && cst.operator === ',') return [...fromItems(cst.left), ...fromItems(cst.right)]
  return [fromItem(cst)]
}

function fromItem(cst: Cst): Node {
  const location = cst.range[0] as number
  switch (cst.type) {
    case 'identifier':
      return { RangeVar: { relname: identifier(cst), location } }
    case 'member_expr':
      if (cst.object.type !== 'identifier' || cst.property.type !== 'identifier') throw new CannotLower('table name')
      return { RangeVar: { schemaname: identifier(cst.object), relname: identifier(cst.property), location } }
    case 'indexed_table':
      return fromItem(cst.table)
    case 'alias': {
      if (cst.columnAliases) throw new CannotLower('column aliases')
      const item = fromItem(cst.expr)
      const alias = { aliasname: identifier(cst.alias) }
      if (item.RangeVar) return { RangeVar: { ...item.RangeVar, alias } }
      if (item.RangeSubselect) return { RangeSubselect: { ...item.RangeSubselect, alias } }
      return { RangeFunction: { ...item.RangeFunction, alias } }
    }
    case 'paren_expr':
      if (isSelect(cst.expr))
        return { RangeSubselect: { lateral: false, subquery: { SelectStmt: lowerSelect(cst.expr) } } }
      return fromItem(cst.expr)
    case 'func_call':
      // A table-valued function: its rows aren't a table's.
      return { RangeFunction: { functions: [lowerExpression(cst)] } }
    case 'join_expr': {
      const operator = keywords(cst.operator).filter((k) => k !== 'OUTER')
      const jointype = JOINS[operator.join(' ')]
      if (!jointype) throw new CannotLower(operator.join(' '))
      const join: Node = { jointype, larg: fromItem(cst.left), rarg: fromItem(cst.right) }
      const specification = cst.specification as Cst | undefined
      if (specification?.type === 'join_on_specification') join.quals = lowerExpression(specification.expr)
      else if (specification?.type === 'join_using_specification')
        join.usingClause = (specification.expr.expr.items as Cst[]).map((c) => ({ String: { sval: identifier(c) } }))
      else if (specification) throw new CannotLower(specification.type)
      return { JoinExpr: join }
    }
    default:
      throw new CannotLower(cst.type)
  }
}

const JOINS: Record<string, string> = {
  JOIN: 'JOIN_INNER',
  'INNER JOIN': 'JOIN_INNER',
  'CROSS JOIN': 'JOIN_INNER',
  'LEFT JOIN': 'JOIN_LEFT',
  'RIGHT JOIN': 'JOIN_RIGHT',
  'FULL JOIN': 'JOIN_FULL',
}

function sortBy(cst: Cst): Node {
  const specification = cst.type === 'sort_specification' ? cst : { expr: cst }
  const descending = specification.direction?.type === 'sort_direction_desc'
  const nulls = specification.nullHandlingKw ? keywords(specification.nullHandlingKw)[1] : undefined
  // SQLite sorts `NULL` before every value.
  const nullsFirst = nulls ? nulls === 'FIRST' : !descending
  return {
    SortBy: {
      node: lowerExpression(specification.expr),
      sortby_dir: descending ? 'SORTBY_DESC' : 'SORTBY_ASC',
      sortby_nulls: nullsFirst ? 'SORTBY_NULLS_FIRST' : 'SORTBY_NULLS_LAST',
    },
  }
}

const COMPARISONS: Record<string, string> = {
  '=': '=',
  '==': '=',
  '<>': '<>',
  '!=': '<>',
  '<': '<',
  '<=': '<=',
  '>': '>',
  '>=': '>=',
}

const AGGREGATES: Record<string, string> = {
  total: 'sum',
  group_concat: 'string_agg',
  json_group_array: 'json_agg',
  jsonb_group_array: 'jsonb_agg',
  json_group_object: 'json_object_agg',
  jsonb_group_object: 'jsonb_object_agg',
}

function lowerExpression(cst: Cst): Node {
  const location = cst.range[0] as number
  switch (cst.type) {
    case 'identifier':
      return { ColumnRef: { fields: [{ String: { sval: identifier(cst) } }], location } }
    case 'member_expr':
      return { ColumnRef: { fields: memberFields(cst), location } }
    case 'all_columns':
      return { ColumnRef: { fields: [{ A_Star: {} }], location } }
    case 'parameter':
      return { ParamRef: { number: Number(cst.text.slice(1)), location } }
    case 'number_literal':
      return /^\d+$/.test(cst.text) && Number.isSafeInteger(cst.value)
        ? { A_Const: { ival: { ival: cst.value } } }
        : { A_Const: { fval: { fval: cst.text } } }
    case 'string_literal':
      return { A_Const: { sval: { sval: cst.value } } }
    case 'null_literal':
      return { A_Const: { isnull: true } }
    case 'boolean_literal':
      // SQLite's `TRUE` and `FALSE` are the integers 1 and 0.
      return { A_Const: { ival: { ival: cst.value ? 1 : 0 } } }
    case 'paren_expr':
      if (isSelect(cst.expr)) return sublink('EXPR_SUBLINK', cst.expr, location)
      if (cst.expr.type === 'list_expr') return unknown('row', cst.expr.items, location)
      return lowerExpression(cst.expr)
    case 'prefix_op_expr': {
      const operator = keywords(cst.operator).join(' ')
      if (operator === 'EXISTS') return sublink('EXISTS_SUBLINK', cst.expr.expr, location)
      if (operator === 'NOT') return { BoolExpr: { boolop: 'NOT_EXPR', args: [lowerExpression(cst.expr)] } }
      if (operator === '-' && cst.expr.type === 'number_literal') {
        const number = lowerExpression(cst.expr).A_Const
        return number.ival
          ? { A_Const: { ival: { ival: -number.ival.ival } } }
          : { A_Const: { fval: { fval: `-${number.fval.fval}` } } }
      }
      return unknown(operator, [cst.expr], location)
    }
    case 'postfix_op_expr': {
      const operator = keywords(cst.operator).join(' ')
      if (operator !== 'ISNULL' && operator !== 'NOTNULL' && operator !== 'NOT NULL') throw new CannotLower(operator)
      return {
        NullTest: { arg: lowerExpression(cst.expr), nulltesttype: operator === 'ISNULL' ? 'IS_NULL' : 'IS_NOT_NULL' },
      }
    }
    case 'binary_expr':
      return binary(cst, location)
    case 'between_expr': {
      const kind = keywords(cst.betweenKw).length === 2 ? 'AEXPR_NOT_BETWEEN' : 'AEXPR_BETWEEN'
      const bounds = { List: { items: [lowerExpression(cst.begin), lowerExpression(cst.end)] } }
      return {
        A_Expr: {
          kind,
          name: [{ String: { sval: 'BETWEEN' } }],
          lexpr: lowerExpression(cst.left),
          rexpr: bounds,
          location,
        },
      }
    }
    case 'func_call':
      return funcCall(cst, location)
    case 'cast_expr':
      return unknown('cast', [cst.args.expr.expr], location)
    case 'case_expr':
      return unknown(
        'case',
        [
          ...(cst.expr ? [cst.expr] : []),
          ...(cst.clauses as Cst[]).flatMap((c) => (c.type === 'case_when' ? [c.condition, c.result] : [c.result])),
        ],
        location,
      )
    case 'blob_literal':
      return unknown('blob', [], location)
    default:
      throw new CannotLower(cst.type)
  }
}

function binary(cst: Cst, location: number): Node {
  const operator = keywords(cst.operator).join(' ')
  const lexpr = () => lowerExpression(cst.left)
  const expression = (kind: string, name: string, rexpr: Node): Node => ({
    A_Expr: { kind, name: [{ String: { sval: name } }], lexpr: lexpr(), rexpr, location },
  })
  if (operator === 'AND' || operator === 'OR')
    return { BoolExpr: { boolop: `${operator}_EXPR`, args: [lexpr(), lowerExpression(cst.right)] } }
  if (COMPARISONS[operator]) return expression('AEXPR_OP', COMPARISONS[operator], lowerExpression(cst.right))
  if (operator === 'IS' || operator === 'IS NOT') {
    if (cst.right.type === 'null_literal')
      return { NullTest: { arg: lexpr(), nulltesttype: operator === 'IS' ? 'IS_NULL' : 'IS_NOT_NULL' } }
    return expression(operator === 'IS' ? 'AEXPR_NOT_DISTINCT' : 'AEXPR_DISTINCT', '=', lowerExpression(cst.right))
  }
  if (operator === 'LIKE' || operator === 'NOT LIKE') {
    // `LIKE p ESCAPE e`: the pattern is a call the matcher doesn't evaluate.
    const escape = cst.right.type === 'binary_expr' && keywords(cst.right.operator).join(' ') === 'ESCAPE'
    const pattern = escape
      ? unknown('like_escape', [cst.right.left, cst.right.right], location)
      : lowerExpression(cst.right)
    return expression('AEXPR_ILIKE', operator === 'LIKE' ? '~~*' : '!~~*', pattern)
  }
  if (operator === 'IN' || operator === 'NOT IN') {
    const right = cst.right.type === 'paren_expr' ? cst.right.expr : cst.right
    if (isSelect(right)) {
      const any = sublink('ANY_SUBLINK', right, location, lexpr())
      return operator === 'IN' ? any : { BoolExpr: { boolop: 'NOT_EXPR', args: [any] } }
    }
    // `x IN table`, `x IN (…)` of one item that isn't a list.
    if (cst.right.type !== 'paren_expr') throw new CannotLower('in table')
    const items = right.type === 'list_expr' ? (right.items as Cst[]) : [right]
    return expression('AEXPR_IN', operator === 'IN' ? '=' : '<>', { List: { items: items.map(lowerExpression) } })
  }
  return unknown(operator, [cst.left, cst.right], location)
}

function funcCall(cst: Cst, location: number): Node {
  if (cst.name.type !== 'identifier') throw new CannotLower('function name')
  const name = identifier(cst.name)
  const args = cst.args?.expr as Cst | undefined
  if (args?.orderBy || args?.limit || args?.having || args?.nullHandlingKw) throw new CannotLower('function arguments')
  const items = (args?.args?.items ?? []) as Cst[]
  const star = items.length === 1 && items[0]!.type === 'all_columns'
  const scalar = (name === 'max' || name === 'min') && items.length > 1
  const call: Node = {
    funcname: [{ String: { sval: scalar ? `sqlite:${name}` : (AGGREGATES[name] ?? name) } }],
    args: star ? [] : items.map(lowerExpression),
    location,
  }
  if (star) call.agg_star = true
  if (args?.distinctKw) call.agg_distinct = true
  if (cst.filter) throw new CannotLower('filter')
  if (cst.over) call.over = {}
  return { FuncCall: call }
}

/** A call of a function the catalog doesn't know: the matcher can't evaluate it. */
function unknown(name: string, args: Cst[], location: number): Node {
  return {
    FuncCall: {
      funcname: [{ String: { sval: `sqlite:${name.toLowerCase()}` } }],
      args: args.map(lowerExpression),
      location,
    },
  }
}

function sublink(subLinkType: string, select: Cst, location: number, testexpr?: Node): Node {
  const link: Node = { subLinkType, subselect: { SelectStmt: lowerSelect(select) }, location }
  if (testexpr) Object.assign(link, { testexpr, operName: [{ String: { sval: '=' } }] })
  return { SubLink: link }
}

function memberFields(cst: Cst): Node[] {
  const property = cst.property.type === 'all_columns' ? { A_Star: {} } : { String: { sval: identifier(cst.property) } }
  if (cst.object.type === 'identifier') return [{ String: { sval: identifier(cst.object) } }, property]
  if (cst.object.type === 'member_expr') return [...memberFields(cst.object), property]
  throw new CannotLower('column name')
}

/** A name as SQLite matches it: ASCII letters in any case are the same. */
function identifier(cst: Cst): string {
  if (cst.type !== 'identifier') throw new CannotLower(cst.type)
  return (cst.name as string).replace(/[A-Z]/g, (c) => c.toLowerCase())
}

function keywords(operator: Cst | Cst[] | string): string[] {
  if (typeof operator === 'string') return [operator.toUpperCase()]
  return (Array.isArray(operator) ? operator : [operator]).map((k) => (k.name as string).toUpperCase())
}

function isSelect(cst: Cst | undefined): boolean {
  return cst?.type === 'select_stmt' || cst?.type === 'compound_select_stmt'
}
