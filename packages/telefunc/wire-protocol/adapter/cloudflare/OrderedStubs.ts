export { OrderedStubs, CallNotSent, CallTimedOut }
export type { CallOptions, Invoke }

import { createDeferred, type Deferred } from '../../../utils/createDeferred.js'
import { isOverloaded } from './transientRetry.js'

/** How long a target that answered overloaded gets no call that opts into its pause: Cloudflare's runtime makes its own
 *  retries within 10 s of a call, so ours add none to them. */
const OVERLOAD_PAUSE_MS = 10_000

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

type CallOptions = {
  /** Checked when the call would be sent: one no longer wanted isn't. */
  wanted?: () => boolean
  /** When its caller stops waiting: a call not sent by then isn't, and one in flight then rejects, and fails its line. */
  deadline?: number
  /** Not sent while its target is paused after an overloaded answer. */
  paused?: boolean
}

/** A call that was never sent: its caller had stopped wanting it, its deadline had passed, or its target was paused for
 *  the overload that is its `cause`. */
class CallNotSent extends Error {
  readonly reason: 'unwanted' | 'past its deadline' | 'paused'
  constructor(target: string, reason: CallNotSent['reason'], cause?: unknown) {
    super(`A call to the Durable Object '${target}' was not sent: ${reason}`, cause === undefined ? {} : { cause })
    this.reason = reason
  }
}

class CallTimedOut extends Error {
  constructor(target: string) {
    super(`The Durable Object '${target}' didn't answer a call before its deadline`)
  }
}

/** One Durable Object's calls to others. Calls to one target share a stub while any is in flight: Cloudflare delivers
 *  RPC calls through one stub in call order, and calls through different stubs in no order. A stub that rejected may be
 *  broken, so later calls get a fresh one, which waits until the failed stub's calls settle so it can't overtake
 *  them. */
class OrderedStubs<Stub> {
  private readonly lines = new Map<string, Line<Stub>>()
  /** By target: until when it is paused, and the overloaded answer that paused it. */
  private readonly pauses = new Map<string, { until: number; error: unknown }>()

  /** Never throws: a stub that can't be opened rejects the call, and the next call opens one again. An `invoke` that is
   *  still to come keeps the call's place: the calls made after it wait until it is sent, or isn't. */
  call<T>(
    target: string,
    open: () => Stub,
    invoke: Invoke<Stub, T> | Promise<Invoke<Stub, T>>,
    options?: CallOptions,
  ): Promise<T> {
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
    return this.run(target, line, invoke, options)
  }

  /** How long `target` stays paused after an overloaded answer, 0 if it isn't. */
  pausedFor(target: string): number {
    const pause = this.pauses.get(target)
    if (pause === undefined) return 0
    const left = pause.until - Date.now()
    if (left <= 0) this.pauses.delete(target)
    return Math.max(left, 0)
  }

  /** With nothing ahead, it invokes in this turn: a stub's calls keep their call order. */
  private async run<T>(
    target: string,
    line: Line<Stub>,
    invoke: Invoke<Stub, T> | Promise<Invoke<Stub, T>>,
    options: CallOptions = {},
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
      const { wanted, deadline, paused } = options
      if (wanted?.() === false) throw new CallNotSent(target, 'unwanted')
      if (deadline !== undefined && Date.now() >= deadline) throw new CallNotSent(target, 'past its deadline')
      if (paused === true && this.pausedFor(target) > 0) {
        throw new CallNotSent(target, 'paused', this.pauses.get(target)?.error)
      }
      sending = true
      const sent = send(line.stub)
      this.pass(line, place, own)
      return deadline === undefined ? await sent : await settleBy(sent, deadline, target)
    } catch (err) {
      this.pass(line, place, own)
      // What kept it from being sent says nothing of the stub.
      if (sending && !(err instanceof CallNotSent)) line.failed = true
      if (sending && isOverloaded(err)) this.pauses.set(target, { until: Date.now() + OVERLOAD_PAUSE_MS, error: err })
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

/** `sent`'s outcome, or a rejection at `deadline`; what `sent` settles with later goes nowhere. */
function settleBy<T>(sent: Promise<T>, deadline: number, target: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new CallTimedOut(target)), deadline - Date.now())
    sent.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (err: unknown) => {
        clearTimeout(timer)
        reject(err)
      },
    )
  })
}
