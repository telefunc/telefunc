export { eachInFlight, allSettledOrThrow, failingAfter }

import { Slice } from '../../shared/Slice.js'
import { ROOM_BACKEND_CALLS_IN_FLIGHT_MAX } from '../constants.js'

// Siblings that each run to their end before the first failure is told: an eviction or a send goes on after another
// failed.

/** Runs `task` on each item in turn, `ROOM_BACKEND_CALLS_IN_FLIGHT_MAX` at a time and a `ROOM_PLAN_SLICE_MS` slice per
 *  turn; throws the first failure once every task ran. Each worker starts with an item of its own: no more run than
 *  there are items, and none for an empty list. */
async function eachInFlight<T>(items: Iterable<T>, task: (item: T) => Promise<unknown>): Promise<void> {
  const pending = items[Symbol.iterator]()
  const first = pending.next()
  if (first.done === true) return
  const slice = new Slice()
  let turn: Promise<void> | null = null
  let failure = null as { error: unknown } | null
  const nextTurn = async () => {
    await slice.next()
    turn = null
  }
  const worker = async (own: IteratorResult<T>) => {
    for (let next = own; next.done !== true; next = pending.next()) {
      if (slice.over) await (turn ??= nextTurn())
      try {
        await task(next.value)
      } catch (error) {
        failure ??= { error }
      }
    }
  }
  const workers: Promise<void>[] = []
  for (let next: IteratorResult<T> = first; next.done !== true; next = pending.next()) {
    workers.push(worker(next))
    if (workers.length === ROOM_BACKEND_CALLS_IN_FLIGHT_MAX) break
  }
  await Promise.all(workers)
  if (failure) throw failure.error
}

/** The value of `work`; else its failure, told once `awaited` settled. Beside a sibling that waits for what it
 *  commits to be handed off, the call doesn't answer while that wait is under way; a failure of `awaited` itself is
 *  told at once, by whoever awaits it. */
async function failingAfter<T>(awaited: Promise<unknown>, work: Promise<T>): Promise<T> {
  try {
    return await work
  } catch (error) {
    await awaited.catch(() => {})
    throw error
  }
}

/** The values of `steps`, which run side by side, in order; else, once every one of them settled, the first failure. */
async function allSettledOrThrow<T extends readonly unknown[]>(
  steps: T,
): Promise<{ -readonly [K in keyof T]: Awaited<T[K]> }> {
  for (const result of await Promise.allSettled(steps)) {
    if (result.status === 'rejected') throw result.reason
  }
  return await Promise.all(steps)
}
