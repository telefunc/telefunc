export { ConnectionRoles, roleChangeOf }
export type { RoleChange, SetConfig }

import type { TransactionAction } from './statement.js'

/**
 * How a statement changes its session's role (`Plan.role`):
 * - `role`: `SET [LOCAL] ROLE`; to the session's authorization when `null` (`NONE`, `RESET ROLE`).
 * - `authorization`: `SET [LOCAL] SESSION AUTHORIZATION`, which sets the role too; to the login role when `null`
 *   (`DEFAULT`, `RESET`).
 * - `login`: back to the login role (`DISCARD ALL`).
 * - `unknown`: to a role it doesn't name (`DO`, `CALL`, a `set_config()` of the role whose arguments aren't known).
 */
type RoleChange =
  | { kind: 'role' | 'authorization'; to: string | null; local: boolean }
  | { kind: 'login' }
  | { kind: 'unknown' }

/** An argument of a `set_config()` call: a constant, a parameter (its index in the values), or an expression
 *  (`undefined`). */
type SetConfigArgument = { constant: string | boolean | null } | { param: number } | undefined

/** A `set_config(name, value, local)` call: the role it sets, if any, is known once its parameters' values are. */
type SetConfig = [name: SetConfigArgument, value: SetConfigArgument, local: SetConfigArgument]

const UNKNOWN: RoleChange = { kind: 'unknown' }
/** The settings naming a session's role, by the change setting them makes. */
const ROLE_SETTINGS = new Map<string, 'role' | 'authorization'>([
  ['role', 'role'],
  ['session_authorization', 'authorization'],
])

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

/** How a statement's `set_config()` calls, run with `values`, change its session's role; `undefined` when none sets
 *  it. */
function roleChangeOf(calls: SetConfig[], values: readonly unknown[]): RoleChange | undefined {
  const changes = calls.map((call) => roleChangeOfCall(call, values)).filter((change) => change !== undefined)
  if (changes.length === 0) return undefined
  // Several, say a session's role then a local one: not followed.
  return changes.length === 1 ? changes[0] : UNKNOWN
}

function roleChangeOfCall([name, value, local]: SetConfig, values: readonly unknown[]): RoleChange | undefined {
  const setting = valueOf(name, values)
  if (typeof setting !== 'string') return UNKNOWN
  const kind = ROLE_SETTINGS.get(setting.toLowerCase())
  if (!kind) return undefined
  const to = valueOf(value, values)
  const isLocal = valueOf(local, values)
  if (typeof to !== 'string' || typeof isLocal !== 'boolean') return UNKNOWN
  // `none` goes back to the session's authorization, as `SET ROLE NONE` does.
  return { kind, to: kind === 'role' && to === 'none' ? null : to, local: isLocal }
}

function valueOf(argument: SetConfigArgument, values: readonly unknown[]): unknown {
  if (!argument) return undefined
  return 'constant' in argument ? argument.constant : values[argument.param]
}
