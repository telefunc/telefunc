export { exclaim }

/** Changes a read's rows in place, as an app may: identical reads sharing a run must each get rows of their own. */
function exclaim<Row extends { text: string }>(rows: Row[]): Row[] {
  for (const row of rows) row.text += '!'
  return rows
}
