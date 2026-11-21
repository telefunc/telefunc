export { takeHiddenFields, takeHiddenValues }

import type { Statement } from '../liveDatabase.js'

/** Removes the statement's hidden columns (the last ones) from `fields`, returning their names. */
function takeHiddenFields(fields: { name: string }[], statement: Statement): string[] {
  const hidden = fields.splice(fields.length - statement.hiddenColumns, statement.hiddenColumns).map((f) => f.name)
  if (!statement.returnsRows) fields.length = 0
  return hidden
}

/** Removes the hidden columns from each row (in place), returning their values. */
function takeHiddenValues(rows: unknown[], hidden: string[], arrayRows: boolean): (string | null)[][] {
  if (hidden.length === 0) return []
  return rows.map((row) => {
    if (arrayRows) return (row as (string | null)[]).splice(-hidden.length, hidden.length)
    const record = row as Record<string, string | null>
    const values = hidden.map((name) => record[name] ?? null)
    for (const name of hidden) delete record[name]
    return values
  })
}
