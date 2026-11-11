export type { Dialect }

import type { Catalog } from './catalog.js'
import type { Comparison } from './match/operands.js'
import type { Node } from './sql.js'

/**
 * What the matcher needs to know of the database's SQL: where to find the parts of a statement's text, and how to
 * write the queries of its own that run next to it. A parse tree's `location` is an offset into the text in the form
 * this dialect's own functions read.
 */
type Dialect = {
  /** Offset of the `FROM` keyword of the outermost `SELECT`. */
  topLevelFrom(text: string): number | undefined
  /** The `FROM` naming a table at `location`, up to the end of its `WHERE`: the offset of the keyword, and the text. */
  fromClauseAt(text: string, location: number): { start: number; text: string } | undefined
  /** The text from the first `(` at or after `location` to its matching `)`. */
  parenthesizedAt(text: string, location: number): string | undefined
  /** Offset just after the statement's last token (before a trailing `;` or comment). */
  statementEnd(text: string): number
  /** `text` with `insert` at offset `at`. */
  splice(text: string, at: number, insert: string): string
  /** A query of ours as it is sent, and the index of the read's parameter each of its parameters takes. */
  bindParams(text: string): { text: string; params: number[] }
  /** How a query of ours names the read's parameter `number` (1-based): `bindParams()` reads it back. */
  param(number: number): string
  /** The character escaping `%` and `_` in a `LIKE` pattern without `ESCAPE`, if any. */
  likeEscape: string | undefined
  /** The text form of `expression`'s value, which the matcher compares (`compareValues()`). */
  text(expression: string): string
  /** The text form of each of `expressions`, as a JSON array, as text. */
  jsonArray(expressions: string[]): string
  /** The values of `expression` over the rows of a query, as a JSON array (`[]` without rows), as text. */
  jsonAgg(expression: string): string
  /** What a query of ours selects to get the value of an operand compared as `comparison` says, in its text form;
   *  `undefined` when the matcher doesn't reproduce the comparison. */
  operand(
    expression: string,
    node: Node,
    comparison: Comparison,
    catalog: Pick<Catalog, 'domainBases'>,
  ): string | undefined
}
