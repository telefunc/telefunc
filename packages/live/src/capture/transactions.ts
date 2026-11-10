export { Transactions }

import type { Change } from '../match/liveRead.js'
import type { TransactionAction } from './statement.js'

type OpenTransaction = {
  changes: Change[]
  /** Each savepoint with how many changes were made before it. */
  savepoints: { name: string; changeCount: number }[]
  /** A statement failed: `COMMIT` will roll back, unless a `ROLLBACK TO SAVEPOINT` recovers it. */
  failed: boolean
}

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
    if (transaction) transaction.changes.push(...changes)
    else this.announce(changes)
  }

  /** After a transaction statement succeeded on `connection`. */
  apply(connection: object, action: TransactionAction, savepoint?: string): void {
    const transaction = this.open.get(connection)
    switch (action) {
      case 'begin':
        if (!transaction) this.open.set(connection, { changes: [], savepoints: [], failed: false })
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
      transaction.changes.length = transaction.savepoints[index]!.changeCount
      transaction.savepoints.length = index + 1
      transaction.failed = false
    }
  }

  failed(connection: object): void {
    const transaction = this.open.get(connection)
    if (transaction) transaction.failed = true
  }

  isOpen(connection: object): boolean {
    return this.open.has(connection)
  }
}
