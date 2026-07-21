export { imageLayoutOf }
export type { ImageLayout }

import { type Column, SQL, sql } from 'drizzle-orm'
import type { Row } from '../bus/router/events.js'
import type { Images } from './writeChanges.js'
import type { Op } from './writePlan.js'

// THE OLD/NEW BOTH-IMAGES LAYOUT (`RETURNING old.*, new.*`, PostgreSQL 18+), owned in one place.
//
// One substituted statement returns BOTH images of every changed row, interleaved positionally as
// `o0, n0, o1, n1, …`. Four things have to agree on that convention — the RETURNING selection that EMITS
// the aliases, the position math that finds a field's value in a raw row, the split that rebuilds
// `{old, new}` rows, and the delete/update asymmetry deciding which image answers which question. They used
// to live in four functions across three lockstep files — the exact class of spread that produced the
// table-named-`old` bug — so the convention is ONE object, seated on the plan, and every consumer asks it.
//
// The delete/update asymmetry, stated once: PostgreSQL's plain RETURNING on a DELETE is the row that was
// deleted — the OLD image — and its NEW image is all-NULL, which is not a row and is never treated as one.
// On an UPDATE the plain RETURNING is the NEW image, and both images are real rows.

type ImageLayout = {
  /** The table fields, in the order the positional `o<i>`/`n<i>` aliases carry them. */
  fields: string[]
  /** The RETURNING selection asking for BOTH images of every column.
   *
   *  The two images are bound to CORRELATION NAMES via `RETURNING WITH (OLD AS …, NEW AS …)` rather than
   *  written as bare `old.col` / `new.col`. Bare names are resolved against the query's own scope first, so
   *  on a table literally named `old` they silently select that TABLE's post-update row: an update to
   *  `old` returned {old: 11, new: 11} where the truth was {old: 10, new: 11} — a wrong row image reported
   *  as precise. Verified on PGlite. One form, applied unconditionally: a rule that only engages for names
   *  that look dangerous is a rule that is never exercised until it matters.
   *
   *  `WITH (…)` must sit immediately after RETURNING, which drizzle's builder has no clause for — so it
   *  rides the FIRST selected expression, where it lands in exactly that position. The emitted SQL is
   *  `returning with (old as "tf_old__", new as "tf_new__") "tf_old__"."id", …`.
   *
   *  Aliases are POSITIONAL (`o0`/`n0`, …): capture owns every alias here, so positional ones are injective
   *  by construction where a name-derived scheme could be made to collide by a column of that name.
   *
   *  Each expression is decoded through its own column, so values arrive exactly as drizzle would decode
   *  them anywhere else. A decoder that throws is handled by OBSERVING the statement rather than by
   *  decoding differently — see the count-observation tap (writeSubstitution.ts). */
  selection: (columns: Record<string, Column>) => Record<string, SQL.Aliased | SQL>
  /** Where the given table field's DELIVERED image sits in a raw row — `index*2` (old) for a delete,
   *  `index*2+1` (new) otherwise, exactly the image `delivered` picks; `-1` for a field this layout does
   *  not carry. */
  rawPositionOf: (field: string, op: Op) => number
  /** One raw row's `o<i>`/`n<i>` values, split back into field-keyed `{old, new}` rows. */
  split: (row: Row) => Images
  /** The image the caller's own plain RETURNING would have been handed. */
  delivered: (pair: Images, op: Op) => Row
  /** The images that must ADDITIONALLY validate as real rows beyond the old one: the NEW images for an
   *  update, nothing for a delete (its NEW is the all-NULL non-row). */
  decisive: (pairs: Images[], op: Op) => Row[]
}

const OLD_IMAGE = 'tf_old__'
const NEW_IMAGE = 'tf_new__'

function imageLayoutOf(fields: string[]): ImageLayout {
  return {
    fields,
    selection(columns) {
      const selection: Record<string, SQL.Aliased | SQL> = {}
      fields.forEach((field, index) => {
        const column = columns[field] as Column
        const name = sql.identifier(column.name)
        const oldRef = sql`${sql.identifier(OLD_IMAGE)}.${name}`
        selection[`o${index}`] = (
          index === 0
            ? sql`with (old as ${sql.identifier(OLD_IMAGE)}, new as ${sql.identifier(NEW_IMAGE)}) ${oldRef}`
            : oldRef
        ).mapWith(column)
        selection[`n${index}`] = sql`${sql.identifier(NEW_IMAGE)}.${name}`.mapWith(column)
      })
      return selection
    },
    rawPositionOf(field, op) {
      const index = fields.indexOf(field)
      if (index < 0) return -1
      return op === 'delete' ? index * 2 : index * 2 + 1
    },
    split(row) {
      const images: Images = { old: {}, new: {} }
      fields.forEach((field, index) => {
        images.old[field] = row[`o${index}`]
        images.new[field] = row[`n${index}`]
      })
      return images
    },
    delivered: (pair, op) => (op === 'delete' ? pair.old : pair.new),
    decisive: (pairs, op) => (op === 'delete' ? [] : pairs.map((pair) => pair.new)),
  }
}
