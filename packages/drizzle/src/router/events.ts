// The change envelope (final-plan §5.6). A ChangeSource emits `ChangeBatch`es — ordered, atomic sets
// of TableChanges from one committed transaction. Ordering and delivery reliability are the SOURCE's
// own concern, so the shared shape carries NO transport position (no LSN, no cursor). ORM mode is
// fully in-process (no wire); a future wire-based CDC source (ticket 7) restores a value codec when
// it actually needs to serialize.

export { type Row, type TableChange, type ChangeBatch }

type Row = Record<string, unknown>

/** One captured row change. `old` present ⇒ the full old row is inline (RI FULL / ORM
 *  returning); `key` carries the PK for a key-only retraction (RI DEFAULT); `new` is the
 *  full post-image (insert / update). */
type TableChange = {
  table: string
  kind: 'insert' | 'update' | 'delete'
  old?: Row
  new?: Row
  key?: Row
}

/** An ordered, atomic set of changes from one committed transaction. */
type ChangeBatch = { changes: TableChange[] }
