export { OrderedStubs }

import { createDeferred, type Deferred } from '../../../utils/createDeferred.js'

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
 *  broken, so later calls get a fresh one, which waits until the failed stub's calls settle so it can't overtake
 *  them. */
class OrderedStubs<Stub> {
  private readonly lines = new Map<string, Line<Stub>>()

  call<T>(target: string, open: () => Stub, invoke: (stub: Stub) => Promise<T>): Promise<T> {
    let line = this.lines.get(target)
    if (line === undefined || line.failed) {
      line = { stub: open(), after: line?.idle.promise ?? null, idle: createDeferred(), calls: 0, failed: false }
      this.lines.set(target, line)
    }
    line.calls++
    return this.run(target, line, invoke)
  }

  /** With nothing ahead, it invokes in this turn: a stub's calls keep their call order. */
  private async run<T>(target: string, line: Line<Stub>, invoke: (stub: Stub) => Promise<T>): Promise<T> {
    try {
      if (line.after !== null) await line.after
      return await invoke(line.stub)
    } catch (err) {
      line.failed = true
      throw err
    } finally {
      this.settle(target, line)
    }
  }

  private settle(target: string, line: Line<Stub>): void {
    if (--line.calls > 0) return
    line.idle.resolve()
    if (this.lines.get(target) === line) this.lines.delete(target)
  }
}
