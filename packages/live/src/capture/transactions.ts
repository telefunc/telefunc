export { Transactions, UNNAMED_TABLES, named }

import type { Change } from '../match/liveRead.js'
import type { TransactionAction } from './statement.js'

/** A part of a transaction's changes: those made before its first savepoint, or after a savepoint and before the
 *  next one. */
type Part = { changes: Change[] }

type OpenTransaction = Part & {
  /** Each savepoint, with the changes made after it: a `ROLLBACK TO SAVEPOINT` drops them, a `RELEASE` keeps them with
   *  the changes before it. */
  savepoints: (Part & { name: string })[]
  /** A statement failed: `COMMIT` will roll back, unless a `ROLLBACK TO SAVEPOINT` recovers it. */
  failed: boolean
  /** A `SAVEPOINT` outside a transaction began it (SQLite): releasing that savepoint commits it. */
  savepointBegan: boolean
}

/** Past this many changes, a statement's changes count as changes to any row of their tables, and so do a
 *  transaction's since its latest savepoint, which a `ROLLBACK TO SAVEPOINT` still drops whole: a part only gets rows
 *  while it's the latest, so a transaction holds the rows of at most this many changes. */
const MAX_CHANGES = 10_000

/** A change to rows of tables the catalog can't name (what a rule or a function of the app's wrote). Before the
 *  transaction commits, the tables it locked name them (`named()`); otherwise it counts as a change to any table. */
const UNNAMED_TABLES: Change = { table: '*', kind: 'unknown' }

/** Holds each connection's changes until its transaction commits; drops them when it rolls back. */
class Transactions {
  private readonly open = new WeakMap<object, OpenTransaction>()
  private readonly announce: (changes: Change[]) => void

  constructor(announce: (changes: Change[]) => void) {
    this.announce = announce
  }

  record(connection: object, changes: Change[]): void {
    if (changes.length === 0) return
    const transaction = this.open.get(connection)
    if (!transaction) return this.announce(changes.length > MAX_CHANGES ? wholeTables(changes) : changes)
    const parts = partsOf(transaction)
    const latest = parts.at(-1)!
    latest.changes.push(...changes)
    if (parts.reduce((count, part) => count + part.changes.length, 0) > MAX_CHANGES)
      latest.changes = wholeTables(latest.changes)
  }

  /** After a transaction statement succeeded on `connection`. */
  apply(connection: object, action: TransactionAction, savepoint?: string): void {
    const transaction = this.open.get(connection)
    switch (action) {
      case 'begin':
        if (!transaction) this.open.set(connection, opened())
        return
      case 'commit':
        this.open.delete(connection)
        if (transaction && !transaction.failed) this.announce(partsOf(transaction).flatMap((part) => part.changes))
        return
      case 'rollback':
        this.open.delete(connection)
        return
    }
    if (!transaction) {
      if (action === 'savepoint') this.open.set(connection, opened(savepoint))
      return
    }
    const index = transaction.savepoints.findLastIndex((s) => s.name === savepoint)
    if (action === 'savepoint') transaction.savepoints.push({ name: savepoint!, changes: [] })
    else if (index === -1) return
    else if (action === 'release') {
      if (index === 0 && transaction.savepointBegan) return this.apply(connection, 'commit')
      const released = transaction.savepoints.splice(index).map((part) => part.changes)
      const before = transaction.savepoints[index - 1] ?? transaction
      before.changes = before.changes.concat(...released)
    } else {
      transaction.savepoints.length = index + 1
      transaction.savepoints[index]!.changes = []
      transaction.failed = false
    }
  }

  /** Whether `connection`'s transaction, about to commit, holds changes to tables the catalog can't name. */
  hasUnnamed(connection: object): boolean {
    const transaction = this.open.get(connection)
    return (
      transaction !== undefined &&
      !transaction.failed &&
      partsOf(transaction).some((part) => part.changes.includes(UNNAMED_TABLES))
    )
  }

  /** Names the tables `connection`'s transaction changed without the catalog naming them: `locked`, right before it
   *  commits. */
  name(connection: object, locked: string[]): void {
    const transaction = this.open.get(connection)
    if (transaction) for (const part of partsOf(transaction)) part.changes = named(part.changes, locked)
  }

  /** Whether a statement failed in `connection`'s transaction, which its `COMMIT` then rolls back. */
  hasFailed(connection: object): boolean {
    return this.open.get(connection)?.failed === true
  }

  failed(connection: object): void {
    const transaction = this.open.get(connection)
    if (transaction) transaction.failed = true
  }

  isOpen(connection: object): boolean {
    return this.open.has(connection)
  }
}

/** A transaction begun by `BEGIN`, or by a `SAVEPOINT` named `savepoint`. */
function opened(savepoint?: string): OpenTransaction {
  const savepoints = savepoint === undefined ? [] : [{ name: savepoint, changes: [] }]
  return { changes: [], savepoints, failed: false, savepointBegan: savepoint !== undefined }
}

/** `transaction`'s changes, before its first savepoint and after each. */
function partsOf(transaction: OpenTransaction): Part[] {
  return [transaction, ...transaction.savepoints]
}

/** `changes`, with the tables its transaction locked (`locked`) for `UNNAMED_TABLES`: every row of each may have
 *  changed. */
function named(changes: Change[], locked: string[]): Change[] {
  if (!changes.includes(UNNAMED_TABLES)) return changes
  const lockedChanges = locked.map((table): Change => ({ table, kind: 'unknown' }))
  return [...changes.filter((change) => change !== UNNAMED_TABLES), ...lockedChanges]
}

/** `changes`, as changes to any row of their tables. */
function wholeTables(changes: Change[]): Change[] {
  return [...new Set(changes.map((c) => c.table))].map((table) => ({ table, kind: 'unknown' }))
}
