export { Turns }

import type { Statement } from '../liveDatabase.js'

/** The calls on each connection, in order: a call starts once the one before it on its connection ended, so that its
 *  statement is planned as the database runs it, after those before it changed the session (a `SET ROLE`). After a
 *  read that changes nothing of its session (`Statement.keepsSession`), it starts once that read was planned: an
 *  identical read then joins it. */
class Turns {
  private readonly last = new WeakMap<object, Promise<void>>()

  /** `call` passes each statement it plans to `planned`. */
  take<T>(connection: object, call: (planned: (statement: Statement) => void) => Promise<T>): Promise<T> {
    let next!: () => void
    const nextMayStart = new Promise<void>((resolve) => (next = resolve))
    const previous = this.last.get(connection) ?? Promise.resolve()
    this.last.set(connection, nextMayStart)
    const result = previous.then(() =>
      call((statement) => {
        if (statement.keepsSession) next()
      }),
    )
    result.then(next, next)
    return result
  }
}
