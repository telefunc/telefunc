export { Turns }

/** The calls on each connection, one at a time: a call starts once the one before it on its connection ended, so that
 *  its statement is planned as the database runs it, after those before it changed the session (a `SET ROLE`). */
class Turns {
  private readonly last = new WeakMap<object, Promise<unknown>>()

  take<T>(connection: object, call: () => Promise<T>): Promise<T> {
    const result = (this.last.get(connection) ?? Promise.resolve()).then(call)
    this.last.set(
      connection,
      result.catch(() => {}),
    )
    return result
  }
}
