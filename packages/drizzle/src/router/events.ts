// The change envelope: a set of TableChanges. It carries no transport position — no LSN, no cursor — so
// ordering and delivery belong to whatever emits one.

export { type Row, type RowChange, type TableChange, type ChangeBatch }

type Row = Record<string, unknown>

/** One captured row change. `old` present ⇒ the full old row is inline (RI FULL / ORM
 *  returning); `key` carries the PK for a key-only retraction (RI DEFAULT); `new` is the
 *  full post-image (insert / update). `kind: 'coarse'` is the image-less marker — it carries only
 *  `{table}` (no old/new/key) and signals a mutation the source cannot represent precisely
 *  (image-less update/delete, generated-key-only insert, onConflict): the router demotes every
 *  affected graph to coarse and NEVER feeds it to a graph's `apply` (fabricating a row would corrupt
 *  exact operator/shadow state). */
/** A row-level change — the coarse-free subset fed to a graph's `apply`. The router routes `coarse`
 *  markers to `coarsen()` and never to `apply`, so the row-space state machine only ever sees these. */
type RowChange = {
  table: string
  kind: 'insert' | 'update' | 'delete'
  old?: Row
  new?: Row
  key?: Row
}

type TableChange = {
  table: string
  kind: 'insert' | 'update' | 'delete' | 'coarse'
  old?: Row
  new?: Row
  key?: Row
}

/** An ordered, atomic set of changes from one committed transaction. */
type ChangeBatch = { changes: TableChange[] }
