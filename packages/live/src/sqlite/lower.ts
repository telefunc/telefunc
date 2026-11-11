export { Lowering, identifier, objectName, variant, writeTarget, CannotLower }

import type { Node } from '../sql.js'
import { offsetOf, type Ast, type Span } from './parser.js'

/** A construct the lowering doesn't know: the statement is read as reading, or writing, anything. */
class CannotLower extends Error {}

/**
 * Lowers a SQLite query, as sqlparser-rs reads it, into the Postgres parse tree the matcher reads (`SelectStmt`). A
 * construct maps to the Postgres node that means the same, and one whose meaning differs (`GLOB`, a cast, scalar
 * `max(a, b)`, …) to a call of a function the catalog doesn't know, which the matcher can't evaluate: its column
 * references still count as read. Any other construct throws `CannotLower`.
 *
 * | SQLite | Postgres node | Why it means the same |
 * |---|---|---|
 * | `x LIKE p`, `x NOT LIKE p` | `A_Expr AEXPR_ILIKE` | SQLite's `LIKE` ignores ASCII case (the matcher folds ASCII only) |
 * | `ORDER BY x [ASC]`, `ORDER BY x DESC` | `SortBy` with `SORTBY_NULLS_FIRST`, `SORTBY_NULLS_LAST` | SQLite sorts `NULL` first |
 * | `a op b op c` (compound) | `SelectStmt` nested to the left | SQLite runs compound operators from left to right |
 * | `total(x)`, `group_concat`, `json_group_array`, `json_group_object` | `sum`, `string_agg`, `json_agg`, `json_object_agg` | aggregates |
 * | `max(a, b, …)`, `min(a, b, …)` | an unknown function | scalar functions on SQLite |
 * | `"name"`, `` `name` ``, `[name]`, `name` | `ColumnRef`, `RangeVar` with the name in ASCII lower case | SQLite names ignore ASCII case |
 */
class Lowering {
  /** The statement's text: the matcher finds a table's `FROM` clause and a subquery's parentheses in it. */
  private readonly text: string

  constructor(text: string) {
    this.text = text
  }

  query(query: Ast): Node {
    const tree = this.setExpression(query.body)
    if (query.with) {
      tree.withClause = {
        recursive: query.with.recursive === true,
        ctes: (query.with.cte_tables as Ast[]).map((cte) => {
          if (cte.alias.columns?.length || cte.from || cte.materialized) throw new CannotLower('cte')
          return {
            CommonTableExpr: { ctename: identifier(cte.alias.name), ctequery: { SelectStmt: this.query(cte.query) } },
          }
        }),
      }
    }
    const orderBy = query.order_by?.kind?.Expressions as Ast[] | undefined
    if (query.order_by && !orderBy) throw new CannotLower('order by')
    if (orderBy) tree.sortClause = orderBy.map((o) => this.sortBy(o))
    if (query.limit_clause) this.limit(tree, query.limit_clause)
    for (const other of ['fetch', 'for_clause', 'settings', 'format_clause', 'locks', 'pipe_operators'])
      if (present(query[other])) throw new CannotLower(other)
    return tree
  }

  /** `a op b op c`: SQLite runs it from the left, whatever the parser nested. */
  private setExpression(body: Ast): Node {
    const branches: Ast[] = []
    const operators: Ast[] = []
    const flatten = (node: Ast) => {
      if (!node.SetOperation) return void branches.push(node)
      flatten(node.SetOperation.left)
      operators.push(node.SetOperation)
      flatten(node.SetOperation.right)
    }
    flatten(body)
    let tree = this.branch(branches[0]!)
    operators.forEach((operation, i) => {
      const op = SET_OPERATIONS[operation.op]
      const quantifier = operation.set_quantifier
      if (!op || (quantifier !== 'None' && quantifier !== 'All')) throw new CannotLower(operation.op)
      tree = { op, all: quantifier === 'All', larg: tree, rarg: this.branch(branches[i + 1]!) }
    })
    return tree
  }

  private branch(body: Ast): Node {
    if (body.Query) return this.query(body.Query)
    if (!body.Select) throw new CannotLower(Object.keys(body)[0])
    return this.select(body.Select)
  }

  private select(select: Ast): Node {
    const tree: Node = { op: 'SETOP_NONE' }
    if (select.distinct === 'Distinct') tree.distinctClause = [{}]
    else if (select.distinct && select.distinct !== 'All') throw new CannotLower('distinct on')
    tree.targetList = (select.projection as Ast[]).map((item) => this.target(item))
    if (select.from?.length) tree.fromClause = (select.from as Ast[]).map((f) => this.tableWithJoins(f))
    if (select.selection) tree.whereClause = this.expression(select.selection)
    const groupBy = select.group_by?.Expressions as [Ast[], Ast[]] | undefined
    if (!groupBy || groupBy[1].length > 0) throw new CannotLower('group by')
    if (groupBy[0].length > 0) tree.groupClause = groupBy[0].map((e) => this.expression(e))
    if (select.having) tree.havingClause = this.expression(select.having)
    if (select.named_window?.length) tree.windowClause = [{}]
    for (const other of SELECT_CLAUSES) if (present(select[other])) throw new CannotLower(other)
    return tree
  }

  private target(item: Ast): Node {
    if (item.UnnamedExpr) return { ResTarget: { val: this.expression(item.UnnamedExpr) } }
    if (item.ExprWithAlias)
      return {
        ResTarget: { name: identifier(item.ExprWithAlias.alias), val: this.expression(item.ExprWithAlias.expr) },
      }
    if (item.Wildcard) return { ResTarget: { val: { ColumnRef: { fields: [{ A_Star: {} }] } } } }
    if (item.QualifiedWildcard?.[0]?.ObjectName) {
      const fields = objectName(item.QualifiedWildcard[0].ObjectName).map((sval) => ({ String: { sval } }))
      return { ResTarget: { val: { ColumnRef: { fields: [...fields, { A_Star: {} }] } } } }
    }
    throw new CannotLower('select item')
  }

  private tableWithJoins(table: Ast): Node {
    let tree = this.fromItem(table.relation)
    for (const join of table.joins as Ast[]) {
      const [kind, constraint] = variant(join.join_operator)
      const jointype = JOINS[kind]
      if (!jointype || join.global) throw new CannotLower(kind)
      const node: Node = { jointype, larg: tree, rarg: this.fromItem(join.relation) }
      if (constraint?.On) node.quals = this.expression(constraint.On)
      else if (constraint?.Using)
        node.usingClause = (constraint.Using as Ast[][]).map((name) => ({ String: { sval: objectName(name).at(-1)! } }))
      else if (constraint !== 'None' && constraint !== undefined) throw new CannotLower('join constraint')
      tree = { JoinExpr: node }
    }
    return tree
  }

  private fromItem(relation: Ast): Node {
    if (relation.Table) {
      const table = relation.Table
      if (table.alias?.columns?.length) throw new CannotLower('column aliases')
      const alias = table.alias ? { alias: { aliasname: identifier(table.alias.name) } } : {}
      // A table-valued function: its rows aren't a table's.
      if (table.args) {
        const call = this.call(table.name, (table.args.args ?? []) as Ast[])
        return { RangeFunction: { functions: [call], ...alias } }
      }
      const name = objectName(table.name)
      if (name.length > 2) throw new CannotLower('table name')
      const location = offsetOf(this.text, firstIdentifier(table.name).span.start)
      const schema = name.length === 2 ? { schemaname: name[0] } : {}
      return { RangeVar: { ...schema, relname: name.at(-1), location, ...alias } }
    }
    if (relation.Derived) {
      const { subquery, alias, lateral } = relation.Derived
      if (alias?.columns?.length) throw new CannotLower('column aliases')
      const subselect: Node = { lateral: lateral === true, subquery: { SelectStmt: this.query(subquery) } }
      return { RangeSubselect: alias ? { ...subselect, alias: { aliasname: identifier(alias.name) } } : subselect }
    }
    if (relation.NestedJoin && !relation.NestedJoin.alias)
      return this.tableWithJoins(relation.NestedJoin.table_with_joins)
    throw new CannotLower(Object.keys(relation)[0])
  }

  private sortBy(order: Ast): Node {
    if (order.with_fill) throw new CannotLower('with fill')
    const descending = order.options?.sort === 'Desc'
    // SQLite sorts `NULL` before every value.
    const nullsFirst = order.options?.nulls_first ?? !descending
    return {
      SortBy: {
        node: this.expression(order.expr),
        sortby_dir: descending ? 'SORTBY_DESC' : 'SORTBY_ASC',
        sortby_nulls: nullsFirst ? 'SORTBY_NULLS_FIRST' : 'SORTBY_NULLS_LAST',
      },
    }
  }

  private limit(tree: Node, clause: Ast): void {
    const { LimitOffset, OffsetCommaLimit } = clause
    if (LimitOffset?.limit_by?.length) throw new CannotLower('limit by')
    const limit = LimitOffset?.limit ?? OffsetCommaLimit?.limit
    const offset = LimitOffset?.offset?.value ?? OffsetCommaLimit?.offset
    if (!LimitOffset && !OffsetCommaLimit) throw new CannotLower('limit')
    if (limit) tree.limitCount = this.expression(limit)
    if (offset) tree.limitOffset = this.expression(offset)
  }

  expression(expr: Ast): Node {
    const [kind, value] = variant(expr)
    switch (kind) {
      case 'Identifier':
        return { ColumnRef: { fields: [{ String: { sval: identifier(value) } }] } }
      case 'CompoundIdentifier':
        return { ColumnRef: { fields: (value as Ast[]).map((part) => ({ String: { sval: identifier(part) } })) } }
      case 'Value':
        return this.value(value.value)
      case 'Nested':
        return this.expression(value)
      case 'UnaryOp': {
        if (value.op === 'Not') return { BoolExpr: { boolop: 'NOT_EXPR', args: [this.expression(value.expr)] } }
        const number = value.expr.Value?.value?.Number
        if (value.op === 'Minus' && number) return this.number(`-${number[0]}`)
        return this.unknown(value.op, [value.expr])
      }
      case 'BinaryOp':
        return this.binary(value)
      case 'IsNull':
      case 'IsNotNull':
        return {
          NullTest: { arg: this.expression(value), nulltesttype: kind === 'IsNull' ? 'IS_NULL' : 'IS_NOT_NULL' },
        }
      case 'IsDistinctFrom':
      case 'IsNotDistinctFrom': {
        const kindOf = kind === 'IsDistinctFrom' ? 'AEXPR_DISTINCT' : 'AEXPR_NOT_DISTINCT'
        return expression(kindOf, '=', this.expression(value[0]), this.expression(value[1]))
      }
      case 'InList': {
        const items = (value.list as Ast[]).map((e) => this.expression(e))
        return expression('AEXPR_IN', value.negated ? '<>' : '=', this.expression(value.expr), { List: { items } })
      }
      case 'InSubquery': {
        const any = this.sublink('ANY_SUBLINK', value.subquery, this.expression(value.expr))
        return value.negated ? { BoolExpr: { boolop: 'NOT_EXPR', args: [any] } } : any
      }
      case 'Between': {
        const bounds = { List: { items: [this.expression(value.low), this.expression(value.high)] } }
        const kindOf = value.negated ? 'AEXPR_NOT_BETWEEN' : 'AEXPR_BETWEEN'
        return expression(kindOf, 'BETWEEN', this.expression(value.expr), bounds)
      }
      case 'Like': {
        if (value.any) throw new CannotLower('like any')
        // `LIKE p ESCAPE e`: the pattern is a call the matcher doesn't evaluate.
        const pattern = value.escape_char
          ? this.unknown('like_escape', [value.pattern, value.escape_char])
          : this.expression(value.pattern)
        return expression('AEXPR_ILIKE', value.negated ? '!~~*' : '~~*', this.expression(value.expr), pattern)
      }
      case 'Function':
        return this.function(value)
      case 'Exists': {
        const exists = this.sublink('EXISTS_SUBLINK', value.subquery)
        return value.negated ? { BoolExpr: { boolop: 'NOT_EXPR', args: [exists] } } : exists
      }
      case 'Subquery':
        return this.sublink('EXPR_SUBLINK', value)
      case 'Cast':
        return this.unknown('cast', [value.expr])
      case 'Collate':
        return this.unknown('collate', [value.expr])
      case 'Case':
        return this.unknown('case', [
          ...(value.operand ? [value.operand] : []),
          ...(value.conditions as Ast[]).flatMap((c) => [c.condition, c.result]),
          ...(value.else_result ? [value.else_result] : []),
        ])
      case 'Tuple':
        return this.unknown('row', value)
      case 'RLike':
        return this.unknown('regexp', [value.expr, value.pattern])
      default:
        throw new CannotLower(kind)
    }
  }

  private value(value: Ast | string): Node {
    if (value === 'Null') return { A_Const: { isnull: true } }
    const [kind, inner] = variant(value as Ast)
    switch (kind) {
      case 'Placeholder':
        if (!/^\?\d+$/.test(inner)) throw new CannotLower('parameter')
        return { ParamRef: { number: Number(inner.slice(1)) } }
      case 'Number':
        return this.number(inner[0])
      case 'SingleQuotedString':
        return { A_Const: { sval: { sval: inner } } }
      case 'Boolean':
        // SQLite's `TRUE` and `FALSE` are the integers 1 and 0.
        return { A_Const: { ival: { ival: inner ? 1 : 0 } } }
      case 'HexStringLiteral':
        return this.unknown('blob', [])
      default:
        throw new CannotLower(kind)
    }
  }

  private number(text: string): Node {
    const integer = /^-?\d+$/.test(text) && Number.isSafeInteger(Number(text))
    return integer ? { A_Const: { ival: { ival: Number(text) } } } : { A_Const: { fval: { fval: text } } }
  }

  private binary({ left, op, right }: Ast): Node {
    if (op === 'And' || op === 'Or')
      return { BoolExpr: { boolop: `${op.toUpperCase()}_EXPR`, args: [this.expression(left), this.expression(right)] } }
    const comparison = COMPARISONS[op]
    if (comparison) return expression('AEXPR_OP', comparison, this.expression(left), this.expression(right))
    return this.unknown(op, [left, right])
  }

  private function(call: Ast): Node {
    if (call.parameters !== 'None' || call.null_treatment || call.within_group?.length) throw new CannotLower('call')
    if (call.filter) throw new CannotLower('filter')
    const list = call.args === 'None' ? { args: [] } : call.args.List
    if (!list || list.clauses?.length) throw new CannotLower('function arguments')
    const node = this.call(call.name, list.args as Ast[])
    if (list.duplicate_treatment === 'Distinct') node.FuncCall.agg_distinct = true
    else if (list.duplicate_treatment) throw new CannotLower('function arguments')
    if (call.over) node.FuncCall.over = {}
    return node
  }

  /** A call of the function `name` with `args`, as a `FuncCall`. */
  private call(name: Ast[], args: Ast[]): Node {
    const names = objectName(name)
    if (names.length !== 1) throw new CannotLower('function name')
    const star = args.length === 1 && args[0]!.Unnamed === 'Wildcard'
    const values = star
      ? []
      : args.map((arg) => {
          if (!arg.Unnamed?.Expr) throw new CannotLower('function argument')
          return arg.Unnamed.Expr as Ast
        })
    const scalar = (names[0] === 'max' || names[0] === 'min') && values.length > 1
    const call: Node = {
      funcname: [{ String: { sval: scalar ? `sqlite:${names[0]}` : (AGGREGATES[names[0]!] ?? names[0]) } }],
      args: values.map((v) => this.expression(v)),
    }
    if (star) call.agg_star = true
    return { FuncCall: call }
  }

  /** A call of a function the catalog doesn't know: the matcher can't evaluate it. */
  private unknown(name: string, args: Ast[]): Node {
    return {
      FuncCall: {
        funcname: [{ String: { sval: `sqlite:${name.toLowerCase()}` } }],
        args: args.map((a) => this.expression(a)),
      },
    }
  }

  private sublink(subLinkType: string, query: Ast, testexpr?: Node): Node {
    // At its opening parenthesis, which the parser doesn't place: the last one before its first keyword.
    const location = this.text.lastIndexOf('(', offsetOf(this.text, firstSpan(query)!.start))
    const link: Node = { subLinkType, subselect: { SelectStmt: this.query(query) }, location }
    if (testexpr) Object.assign(link, { testexpr, operName: [{ String: { sval: '=' } }] })
    return { SubLink: link }
  }
}

/** Clauses of other dialects a SQLite `SELECT` doesn't have. */
const SELECT_CLAUSES = ['top', 'into', 'prewhere', 'qualify', 'connect_by', 'value_table_mode', 'exclude'].concat([
  'lateral_views',
  'cluster_by',
  'distribute_by',
  'sort_by',
])

const SET_OPERATIONS: Record<string, string> = {
  Union: 'SETOP_UNION',
  Intersect: 'SETOP_INTERSECT',
  Except: 'SETOP_EXCEPT',
}

const JOINS: Record<string, string> = {
  Join: 'JOIN_INNER',
  Inner: 'JOIN_INNER',
  CrossJoin: 'JOIN_INNER',
  Left: 'JOIN_LEFT',
  LeftOuter: 'JOIN_LEFT',
  Right: 'JOIN_RIGHT',
  RightOuter: 'JOIN_RIGHT',
  FullOuter: 'JOIN_FULL',
}

const COMPARISONS: Record<string, string> = { Eq: '=', NotEq: '<>', Lt: '<', LtEq: '<=', Gt: '>', GtEq: '>=' }

const AGGREGATES: Record<string, string> = {
  total: 'sum',
  group_concat: 'string_agg',
  json_group_array: 'json_agg',
  jsonb_group_array: 'jsonb_agg',
  json_group_object: 'json_object_agg',
  jsonb_group_object: 'jsonb_object_agg',
}

function expression(kind: string, name: string, lexpr: Node, rexpr: Node): Node {
  return { A_Expr: { kind, name: [{ String: { sval: name } }], lexpr, rexpr } }
}

/** Whether serde wrote a field the statement has: an absent one is `null`, `false` or empty. */
function present(value: unknown): boolean {
  return value !== null && value !== undefined && value !== false && !(Array.isArray(value) && value.length === 0)
}

/** A serde enum value: `"Unit"` or `{ Variant: value }`. */
function variant(node: Ast | string): [string, any] {
  if (typeof node === 'string') return [node, undefined]
  const [entry] = Object.entries(node)
  return entry as [string, any]
}

/** The table an `INSERT`, `UPDATE` or `DELETE` writes, by name; `undefined` when it writes through a join or a
 *  function. */
function writeTarget(statement: Ast): { schemaname?: string; relname: string } | undefined {
  const [kind, write] = variant(statement)
  const single = (tables: Ast[] | undefined) => {
    const [only, ...more] = tables ?? []
    const table = only?.relation?.Table
    return only && more.length === 0 && !only.joins?.length && table && !table.args ? (table.name as Ast[]) : undefined
  }
  const name =
    kind === 'Insert'
      ? (write.table?.TableName as Ast[] | undefined)
      : kind === 'Update'
        ? single([write.table])
        : kind === 'Delete' && !write.tables?.length && !write.using
          ? single(write.from?.WithFromKeyword ?? write.from?.WithoutKeyword)
          : undefined
  const parts = name && objectName(name)
  if (!parts || parts.length > 2) return undefined
  return parts.length === 2 ? { schemaname: parts[0], relname: parts[1]! } : { relname: parts[0]! }
}

/** A name as SQLite matches it: ASCII letters in any case are the same. */
function identifier(ident: Ast): string {
  if (typeof ident?.value !== 'string') throw new CannotLower('name')
  return ident.value.replace(/[A-Z]/g, (c: string) => c.toLowerCase())
}

/** The parts of a dotted name (`main.todos`). */
function objectName(parts: Ast[]): string[] {
  return parts.map((part) => identifier(part.Identifier))
}

function firstIdentifier(parts: Ast[]): Ast {
  return parts[0]!.Identifier
}

/** The span of a node's first token: serde writes a node's fields in source order, so it is the first span found. */
function firstSpan(node: unknown): Span | undefined {
  if (Array.isArray(node)) {
    for (const item of node) {
      const span = firstSpan(item)
      if (span) return span
    }
  } else if (node && typeof node === 'object') {
    if ('span' in node && (node as { span: Span }).span.start) return (node as { span: Span }).span
    for (const value of Object.values(node)) {
      const span = firstSpan(value)
      if (span) return span
    }
  }
  return undefined
}
