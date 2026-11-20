export { OrderedStubs }
export type { Invoke }

import { createDeferred, type Deferred } from '../../../utils/createDeferred.js'

type Line<Stub> = {
  readonly stub: Stub
  /** What the line's next call waits for before it is sent: the failed line's calls settling, where this line replaced
   *  one, or the sending of a call that waited for its invoke. */
  tail: Tail | null
  readonly idle: Deferred<void>
  calls: number
  failed: boolean
}

/** Gone from its line once every call that waits for it was sent, or never had to be. */
type Tail = { readonly done: Promise<void>; waiting: number }

type Invoke<Stub, T> = (stub: Stub) => Promise<T>

/** One Durable Object's calls to others. Calls to one target share a stub while any is in flight: Cloudflare delivers
 *  RPC calls through one stub in call order, and calls through different stubs in no order. A stub that rejected may be
 *  broken, so later calls get a fresh one, which waits until the failed stub's calls settle so it can't overtake
 *  them. */
class OrderedStubs<Stub> {
  private readonly lines = new Map<string, Line<Stub>>()

  /** Never throws: a stub that can't be opened rejects the call, and the next call opens one again. An `invoke` that is
   *  still to come keeps the call's place: the calls made after it wait until it is sent, or isn't. */
  call<T>(target: string, open: () => Stub, invoke: Invoke<Stub, T> | Promise<Invoke<Stub, T>>): Promise<T> {
    // Awaited once the calls ahead of it were sent, or never: a rejection before then is this call's to report.
    if (typeof invoke !== 'function') invoke.catch(() => {})
    let line = this.lines.get(target)
    if (line === undefined || line.failed) {
      let stub: Stub
      try {
        stub = open()
      } catch (err) {
        return Promise.reject(err)
      }
      const tail = line === undefined ? null : { done: line.idle.promise, waiting: 0 }
      line = { stub, tail, idle: createDeferred(), calls: 0, failed: false }
      this.lines.set(target, line)
    }
    line.calls++
    return this.run(target, line, invoke)
  }

  /** With nothing ahead, it invokes in this turn: a stub's calls keep their call order. */
  private async run<T>(
    target: string,
    line: Line<Stub>,
    invoke: Invoke<Stub, T> | Promise<Invoke<Stub, T>>,
  ): Promise<T> {
    const ahead = line.tail
    if (ahead !== null) ahead.waiting++
    const place = typeof invoke === 'function' ? null : createDeferred<void>()
    const own: Tail | null = place === null ? null : { done: place.promise, waiting: 0 }
    if (own !== null) line.tail = own
    let sending = false
    try {
      if (ahead !== null) {
        await ahead.done
        if (--ahead.waiting === 0 && line.tail === ahead) line.tail = null
      }
      const send = typeof invoke === 'function' ? invoke : await invoke
      sending = true
      const sent = send(line.stub)
      this.pass(line, place, own)
      return await sent
    } catch (err) {
      this.pass(line, place, own)
      // What kept it from being sent says nothing of the stub.
      if (sending) line.failed = true
      throw err
    } finally {
      this.settle(target, line)
    }
  }

  /** The calls waiting for this one go on. */
  private pass(line: Line<Stub>, place: Deferred<void> | null, own: Tail | null): void {
    if (place === null) return
    place.resolve()
    if (own !== null && own.waiting === 0 && line.tail === own) line.tail = null
  }

  private settle(target: string, line: Line<Stub>): void {
    if (--line.calls > 0) return
    line.idle.resolve()
    if (this.lines.get(target) === line) this.lines.delete(target)
  }
}
