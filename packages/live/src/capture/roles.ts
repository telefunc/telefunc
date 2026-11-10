export { ConnectionRoles }
export type { RoleChange }

import type { TransactionAction } from './statement.js'

/**
 * How a statement changes its session's role (`Plan.role`):
 * - `role`: `SET [LOCAL] ROLE`; to the session's authorization when `null` (`NONE`, `RESET ROLE`).
 * - `authorization`: `SET [LOCAL] SESSION AUTHORIZATION`, which sets the role too; to the login role when `null`
 *   (`DEFAULT`, `RESET`).
 * - `login`: back to the login role (`DISCARD ALL`).
 * - `unknown`: to a role it doesn't name (`DO`, `CALL`, `set_config()`).
 */
type RoleChange =
  | { kind: 'role' | 'authorization'; to: string | null; local: boolean }
  | { kind: 'login' }
  | { kind: 'unknown' }

/** A session's role, and the authorization `SET ROLE NONE` goes back to; `null` when unknown. */
type Session = { role: string | null; authorization: string | null }

/** A session in a transaction: as it will be once committed, the role set `LOCAL` until then, and whether the
 *  session's own role changed since it began (a `ROLLBACK TO SAVEPOINT` may then undo some of it). */
type Open = Session & { local: string | null | undefined; changed: boolean }

/** The role each connection's statements run as, from the statements it ran; `null` when a statement may have set one
 *  it doesn't name. A session's settings follow its transactions: `SET LOCAL` lasts until the transaction ends, and a
 *  `SET` in a transaction rolled back is undone. */
class ConnectionRoles {
  /** The role the catalog was loaded as, which every connection starts as. */
  private readonly login: string
  private readonly sessions = new WeakMap<object, { committed: Session; open?: Open }>()

  constructor(login: string) {
    this.login = login
  }

  of(connection: object): string | null {
    const { committed, open } = this.sessionOf(connection)
    if (!open) return committed.role
    return open.local !== undefined ? open.local : open.role
  }

  /** After a statement changing the role succeeded on `connection`. */
  changed(connection: object, change: RoleChange): void {
    const state = this.sessionOf(connection)
    const { open } = state
    const session = open ?? state.committed
    switch (change.kind) {
      case 'role':
      case 'authorization': {
        const authorization = change.kind === 'authorization' ? (change.to ?? this.login) : session.authorization
        const role = change.kind === 'authorization' ? authorization : (change.to ?? session.authorization)
        // `SET LOCAL` outside a transaction does nothing.
        if (change.local) {
          if (open) open.local = role
          return
        }
        session.role = role
        session.authorization = authorization
        if (open) Object.assign(open, { local: undefined, changed: true })
        return
      }
      // Never in a transaction.
      case 'login':
        state.committed = { role: this.login, authorization: this.login }
        return
      case 'unknown':
        session.role = session.authorization = null
        if (open) Object.assign(open, { local: undefined, changed: true })
    }
  }

  /** After a transaction statement on `connection`; a `COMMIT` that failed ended its transaction as a `ROLLBACK`. */
  transaction(connection: object, action: TransactionAction): void {
    const state = this.sessionOf(connection)
    const { open } = state
    switch (action) {
      case 'begin':
        state.open = { ...state.committed, local: undefined, changed: false }
        return
      case 'commit':
        if (open) state.committed = { role: open.role, authorization: open.authorization }
        state.open = undefined
        return
      case 'rollback':
        state.open = undefined
        return
      case 'rollbackTo':
        // What changed after the savepoint is undone, what changed before it isn't: either may be the role now.
        if (open?.local !== undefined) open.local = null
        if (open?.changed) open.role = open.authorization = null
    }
  }

  private sessionOf(connection: object): { committed: Session; open?: Open } {
    let state = this.sessions.get(connection)
    if (!state) {
      state = { committed: { role: this.login, authorization: this.login } }
      this.sessions.set(connection, state)
    }
    return state
  }
}
