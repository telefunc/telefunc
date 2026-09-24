export { OrderedStubs }

import { createDeferred, type Deferred } from '../../../../utils/createDeferred.js'

type Line<Stub> = {
  readonly stub: Stub
  /** Set when this line replaced a failed one: its calls start once the failed line's calls settled. */
  readonly after: Promise<void> | null
  readonly idle: Deferred<void>
  calls: number
  failed: boolean
}

/** One Durable Object's calls to others. Calls to one target share a stub while any is in flight: Cloudflare delivers
 *  RPC calls through one stub in call order, and calls through different stubs in no order. A stub that rejected may be
 *  broken, so later calls get a fresh one, which waits until the failed stub's calls settle so it can't overtake them. */
class OrderedStubs<Stub> {
  readonly #lines = new Map<string, Line<Stub>>()

  call<T>(target: string, open: () => Stub, invoke: (stub: Stub) => Promise<T>): Promise<T> {
    let line = this.#lines.get(target)
    if (line === undefined || line.failed) {
      line = { stub: open(), after: line?.idle.promise ?? null, idle: createDeferred(), calls: 0, failed: false }
      this.#lines.set(target, line)
    }
    const current = line
    current.calls++
    const result =
      current.after === null ? invokeNow(invoke, current.stub) : current.after.then(() => invoke(current.stub))
    result.then(
      () => this.#settle(target, current),
      () => {
        current.failed = true
        this.#settle(target, current)
      },
    )
    return result
  }

  #settle(target: string, line: Line<Stub>): void {
    if (--line.calls > 0) return
    line.idle.resolve()
    if (this.#lines.get(target) === line) this.#lines.delete(target)
  }
}

function invokeNow<Stub, T>(invoke: (stub: Stub) => Promise<T>, stub: Stub): Promise<T> {
  try {
    return invoke(stub)
  } catch (error) {
    return Promise.reject(error)
  }
}
