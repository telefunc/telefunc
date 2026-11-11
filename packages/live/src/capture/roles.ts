export { ConnectionRoles, roleChangeOf, ROLE_SETTINGS }
export type { RoleChange, SettingName }

import type { TransactionAction } from './statement.js'

/**
 * How a statement changes its session's role (`Plan.role`):
 * - `set`: to the role `SET [LOCAL] ROLE` names.
 * - `unknown`: to a role it doesn't name: a function of the app's, `DO`, `CALL`, `set_config()` of the role,
 *   `RESET ROLE`, `SET ROLE NONE`, `SESSION AUTHORIZATION`, `RESET ALL`, `DISCARD`.
 */
type RoleChange = { kind: 'set'; role: string; local: boolean } | { kind: 'unknown' }

/** The setting a `set_config()` call sets: written out, a parameter (its index in the values), or an expression
 *  (`undefined`). */
type SettingName = { constant: string | null } | { param: number } | undefined

const UNKNOWN: RoleChange = { kind: 'unknown' }
/** The settings naming a session's role. */
const ROLE_SETTINGS = new Set(['role', 'session_authorization'])

/** A session in a transaction: its role as the transaction will commit it, the role set `LOCAL` until it ends, and
 *  whether its own role changed since it began (a `ROLLBACK TO SAVEPOINT` may then undo some of it). */
type Open = { role: string | null; local: string | null | undefined; changed: boolean }

/** The role each connection's statements run as; `null` when unknown. A statement naming a role sets it once Postgres
 *  confirmed it: the statement succeeded, and its transaction committed. One that may set a role it doesn't name
 *  leaves the role unknown until Postgres is asked (`answered()`). `SET LOCAL` lasts until the transaction ends, and a
 *  transaction rolled back undoes the `SET`s it ran. */
class ConnectionRoles {
  /** The role the catalog was loaded as, which every connection starts as. */
  private readonly login: string
  private readonly sessions = new WeakMap<object, { committed: string | null; open?: Open }>()

  constructor(login: string) {
    this.login = login
  }

  of(connection: object): string | null {
    const { committed, open } = this.sessionOf(connection)
    if (!open) return committed
    return open.local !== undefined ? open.local : open.role
  }

  /** After a statement changing the role succeeded on `connection`. */
  changed(connection: object, change: RoleChange): void {
    const state = this.sessionOf(connection)
    const role = change.kind === 'set' ? change.role : null
    const local = change.kind === 'set' && change.local
    // `SET LOCAL` outside a transaction does nothing.
    if (!state.open) {
      if (!local) state.committed = role
      return
    }
    if (local) state.open.local = role
    else Object.assign(state.open, { role, local: undefined, changed: true })
  }

  /** What `SELECT current_user` answered on `connection`: the role it runs as until a statement changes it, or its
   *  transaction ends. */
  answered(connection: object, role: string): void {
    const state = this.sessionOf(connection)
    if (state.open) state.open.local = role
    else state.committed = role
  }

  /** After a transaction statement on `connection`; a `COMMIT` of a failed transaction is a `rollback`. */
  transaction(connection: object, action: TransactionAction): void {
    const state = this.sessionOf(connection)
    const { open } = state
    switch (action) {
      case 'begin':
        // A `BEGIN` in a transaction does nothing.
        state.open ??= { role: state.committed, local: undefined, changed: false }
        return
      case 'commit':
        if (open) state.committed = open.role
        state.open = undefined
        return
      case 'rollback':
        state.open = undefined
        return
      case 'rollbackTo':
        // What changed after the savepoint is undone, what changed before it isn't: either may be the role now.
        if (open?.local !== undefined) open.local = null
        if (open?.changed) open.role = null
    }
  }

  private sessionOf(connection: object): { committed: string | null; open?: Open } {
    let state = this.sessions.get(connection)
    if (!state) {
      state = { committed: this.login }
      this.sessions.set(connection, state)
    }
    return state
  }
}

/** How a statement's `set_config()` calls, setting `settings` with `values`, change its session's role: to one they
 *  don't name when one may set it; `undefined` when none does. */
function roleChangeOf(settings: SettingName[], values: readonly unknown[]): RoleChange | undefined {
  return settings.some((setting) => setsRole(setting, values)) ? UNKNOWN : undefined
}

function setsRole(setting: SettingName, values: readonly unknown[]): boolean {
  const name = !setting ? undefined : 'constant' in setting ? setting.constant : values[setting.param]
  return typeof name !== 'string' || ROLE_SETTINGS.has(name.toLowerCase())
}
