// The change envelope: a set of TableChanges. It carries no transport position, so ordering and
// delivery belong to whatever emits one.

export { type Row, type RowChange, type TableChange, type ChangeBatch }

type Row = Record<string, unknown>

/** A row-level change — the coarse-free subset fed to a graph's `apply`. The router routes `coarse`
 *  markers to `coarsen()` and never to `apply`, so the row-space state machine only ever sees these. */
type RowChange = {
  table: string
  kind: 'insert' | 'update' | 'delete'
  old?: Row
  new?: Row
  key?: Row
}

/** One change to one table.
 *
 *  Required of whoever emits one, and enforced by nothing here — the three row fields are optional on
 *  every kind: `old`, when present, is the full old row inline; `key` carries the primary key alone, for
 *  a key-only retraction; `new` is the full post-image. These are obligations on the producer, not
 *  guarantees a reader can lean on.
 *
 *  `kind: 'coarse'` tells the router to demote every affected graph without feeding it a row —
 *  fabricating one would corrupt exact operator/shadow state. */
type TableChange = {
  table: string
  kind: 'insert' | 'update' | 'delete' | 'coarse'
  old?: Row
  new?: Row
  key?: Row
}

type ChangeBatch = { changes: TableChange[] }
