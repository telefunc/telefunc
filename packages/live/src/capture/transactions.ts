export { Transactions, UNNAMED_TABLES, named }

import type { Change } from '../match/liveRead.js'
import type { TransactionAction } from './statement.js'

type OpenTransaction = {
  changes: Change[]
  /** Each savepoint with how many changes were made before it. */
  savepoints: { name: string; changeCount: number }[]
  /** A statement failed: `COMMIT` will roll back, unless a `ROLLBACK TO SAVEPOINT` recovers it. */
  failed: boolean
  /** The changes became changes to any row of their tables: a `ROLLBACK TO SAVEPOINT` keeps them. */
  collapsed: boolean
}

/** Past this many changes, a statement's or a transaction's changes count as changes to any row of their tables. */
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
    if (!transaction) return this.announce(collapse(changes))
    transaction.changes.push(...changes)
    if (transaction.changes.length <= MAX_CHANGES) return
    transaction.changes = collapse(transaction.changes)
    transaction.collapsed = true
  }

  /** After a transaction statement succeeded on `connection`. */
  apply(connection: object, action: TransactionAction, savepoint?: string): void {
    const transaction = this.open.get(connection)
    switch (action) {
      case 'begin':
        if (!transaction) this.open.set(connection, { changes: [], savepoints: [], failed: false, collapsed: false })
        return
      case 'commit':
        this.open.delete(connection)
        if (transaction && !transaction.failed) this.announce(transaction.changes)
        return
      case 'rollback':
        this.open.delete(connection)
        return
    }
    if (!transaction) return
    const index = transaction.savepoints.findLastIndex((s) => s.name === savepoint)
    if (action === 'savepoint')
      transaction.savepoints.push({ name: savepoint!, changeCount: transaction.changes.length })
    else if (index === -1) return
    else if (action === 'release') transaction.savepoints.length = index
    else {
      if (!transaction.collapsed) transaction.changes.length = transaction.savepoints[index]!.changeCount
      transaction.savepoints.length = index + 1
      transaction.failed = false
    }
  }

  /** Whether `connection`'s transaction, about to commit, holds changes to tables the catalog can't name. */
  hasUnnamed(connection: object): boolean {
    const transaction = this.open.get(connection)
    return transaction !== undefined && !transaction.failed && transaction.changes.includes(UNNAMED_TABLES)
  }

  /** Names the tables `connection`'s transaction changed without the catalog naming them: `locked`, right before it
   *  commits. */
  name(connection: object, locked: string[]): void {
    const transaction = this.open.get(connection)
    if (transaction) transaction.changes = named(transaction.changes, locked)
  }

  failed(connection: object): void {
    const transaction = this.open.get(connection)
    if (transaction) transaction.failed = true
  }

  isOpen(connection: object): boolean {
    return this.open.has(connection)
  }
}

/** `changes`, with the tables its transaction locked (`locked`) for `UNNAMED_TABLES`: every row of each may have
 *  changed. */
function named(changes: Change[], locked: string[]): Change[] {
  if (!changes.includes(UNNAMED_TABLES)) return changes
  const lockedChanges = locked.map((table): Change => ({ table, kind: 'unknown' }))
  return [...changes.filter((change) => change !== UNNAMED_TABLES), ...lockedChanges]
}

function collapse(changes: Change[]): Change[] {
  if (changes.length <= MAX_CHANGES) return changes
  return [...new Set(changes.map((c) => c.table))].map((table) => ({ table, kind: 'unknown' }))
}
