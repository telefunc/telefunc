export { WatchedRead, SharedReads, keyOfValues, copyRows }
export type { Joined }

import type { LiveQuery } from './liveQuery.js'
import { LiveRead } from './match/liveRead.js'
import type { OperandValue } from './match/operands.js'
import type { ReadShape } from './match/readShape.js'
import type { Router } from './match/router.js'

/** A read that joined an identical one in flight: a copy of that read's result for the caller, or `undefined` when
 *  it failed or its result can't be copied (the caller then runs its own). */
type Joined = Promise<{ result: unknown } | undefined>

/** A live read in the router and the `.live()` queries watching it: identical reads share one. */
class WatchedRead {
  readonly read: LiveRead
  /** Settles once the other servers' changes to its tables reach this one: until then its result may lack one. */
  readonly unconfirmed: Promise<void> | undefined
  private readonly router: Router
  private readonly watchers = new Set<LiveQuery>()
  private isAffected = false

  /** `operands`: the shape's operands' values, when already known (`undefined`: filed by no value until they are). */
  constructor(
    shape: ReadShape,
    params: readonly unknown[],
    operands: readonly OperandValue[] | undefined,
    router: Router,
    unconfirmed: Promise<void> | undefined,
  ) {
    this.read = new LiveRead(shape, params, operands, () => this.affected())
    this.router = router
    this.unconfirmed = unconfirmed
    router.add(this.read)
  }

  /** The statement's result (`LiveRead.resolve()`); with its operands' values, the read is filed by them. */
  resolve(
    hiddenRows: (string | null)[][] | undefined,
    state?: (string | null)[][],
    operands?: readonly OperandValue[],
  ): void {
    const known = this.read.operands !== undefined
    this.read.resolve(hiddenRows, state, operands)
    if (!known && operands) this.router.refile(this.read)
  }

  /** `query` watches it too: from now on, and for what changed since it started (its result is this read's). */
  watch(query: LiveQuery): void {
    this.watchers.add(query)
    query.add(this)
    if (this.isAffected) query.changed()
  }

  unwatch(query: LiveQuery): void {
    this.watchers.delete(query)
    if (this.watchers.size === 0) this.router.remove(this.read)
  }

  /** The statement failed: nobody watches what it read. */
  drop(): void {
    for (const query of [...this.watchers]) query.remove(this)
  }

  private affected(): void {
    this.isAffected = true
    for (const query of [...this.watchers]) query.changed()
  }
}

/** `connection`: the one it runs on, once it has one. */
type InFlight = {
  read: WatchedRead
  connection: object | undefined
  joiners: ((answer: { result: unknown } | undefined) => void)[]
}

/**
 * The live reads running now on one database, by what makes two of them return the same rows: the statement sent,
 * its parameters, and the form the driver returns rows in. Sessions are part of it too: two connections of one
 * wrapped database start alike, and every statement on them is seen, so a database stops sharing reads for good once
 * any statement may have changed a session (`SET`, `set_config()`, `DISCARD`, a `DO` block, a procedure, a function of
 * the app's, `ALTER ROLE … SET`). A read in a transaction never shares: it sees the transaction's writes and snapshot.
 *
 * The first read runs; one arriving while it runs gets a copy of its result and watches it, so N clients refetching
 * one query send it once. Each telefunction still runs, with its own checks, for each client. A read that something
 * else was planned after on its connection takes no more: what comes after runs after that.
 */
class SharedReads {
  private readonly inFlight = new Map<string, InFlight>()

  /** The read running now with `key`, which `query` joins. */
  join(key: string, query: LiveQuery): Joined | undefined {
    const running = this.inFlight.get(key)
    if (!running) return undefined
    running.read.watch(query)
    return new Promise((resolve) => running.joiners.push(resolve))
  }

  /** `read` runs now with `key`, on `connection`: reads arriving until `answered()`, `failed()` or `stopJoining()` join
   *  it. */
  lead(
    key: string,
    read: WatchedRead,
    connection?: object,
  ): { answered<R>(result: R, copy: (result: R) => R | undefined): void; failed(): void } {
    const running: InFlight = { read, connection, joiners: [] }
    this.inFlight.set(key, running)
    const settle = () => {
      if (this.inFlight.get(key) === running) this.inFlight.delete(key)
    }
    return {
      answered: (result, copy) => {
        settle()
        // Copied before the leader's caller gets it: an ORM may change the rows it was given.
        for (const resolve of running.joiners) {
          const own = copy(result)
          resolve(own === undefined ? undefined : { result: own })
        }
      },
      failed: () => {
        settle()
        for (const resolve of running.joiners) resolve(undefined)
      },
    }
  }

  /** A statement that may change what reads see was planned on `connection`: the reads running there take no more. */
  stopJoining(connection: object): void {
    for (const [key, running] of this.inFlight) if (running.connection === connection) this.inFlight.delete(key)
  }
}

/** The identity of a read's parameters, or `undefined` for one that can't be compared safely. */
function keyOfValues(values: readonly unknown[]): string | undefined {
  try {
    return JSON.stringify(values, function (this: Record<string, unknown>, key, value: unknown) {
      // The value before `toJSON()`: a `Date` and its ISO string are different parameters. Tagged with a NUL, which
      // no Postgres text holds.
      const original = this[key]
      if (original instanceof Date) return `\0date:${original.getTime()}`
      if (typeof original === 'bigint') return `\0bigint:${original}`
      if (original instanceof Uint8Array) return `\0bytes:${Buffer.from(original).toString('base64')}`
      // JSON writes `NaN` and the infinities as `null`, and `-0` as `0`.
      if (typeof original === 'number' && (!Number.isFinite(original) || Object.is(original, -0)))
        return `\0number:${Object.is(original, -0) ? '-0' : original}`
      if (original === null || ['string', 'number', 'boolean', 'undefined'].includes(typeof original)) return value
      if (Array.isArray(original) || Object.getPrototypeOf(original) === Object.prototype) return value
      throw new Error('Not comparable')
    })
  } catch {
    return undefined
  }
}

/** Copies a result's rows so that callers sharing it can't see each other's changes to them: `undefined` when a value
 *  is of a class it can't copy (a custom type parser's). */
function copyRows(rows: unknown[]): unknown[] | undefined {
  try {
    return rows.map(copyValue)
  } catch {
    return undefined
  }
}

function copyValue(value: unknown): unknown {
  if (typeof value !== 'object' || value === null) return value
  const prototype = Object.getPrototypeOf(value)
  if (prototype === Date.prototype) return new Date((value as Date).getTime())
  if (prototype === Buffer.prototype) return Buffer.from(value as Buffer)
  if (prototype === Uint8Array.prototype) return (value as Uint8Array).slice()
  if (prototype === Array.prototype) return (value as unknown[]).map(copyValue)
  if (prototype === Object.prototype || prototype === null) {
    const copy = Object.fromEntries(Object.entries(value).map(([k, v]) => [k, copyValue(v)]))
    return Object.setPrototypeOf(copy, prototype)
  }
  throw new Error('Not copyable')
}
