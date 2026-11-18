export { ChangeFeed }

import type { BroadcastTransport } from 'telefunc'
import { getBroadcastPlane, getOrCreate, markHandled, unrefTimer, type BackendSubscription } from 'telefunc/__internal'
import type { Change } from './match/liveRead.js'

type Message = { origin: string; changes: Change[] }

type Listener = (payload: string, ordering: { seq: number }) => void

/** A message not through by then counts as lost: it's sent again (a duplicate only refetches twice). */
const PUBLISH_DEADLINE_MS = 10_000
/** After a message was lost, the pause before it's sent again: it doubles on each loss in a row, up to the last. */
const RETRY_FIRST_MS = 100
const RETRY_LAST_MS = 10_000
/** Past this many changes of a table waiting to be sent, they're sent as a change to any of its rows. */
const MAX_WAITING_CHANGES = 10_000
/** How far below a table's highest sequence number a message is still told apart from one received already: a
 *  transport of the app's may reorder or repeat messages. */
const RECEIVED_WINDOW = 64

/** What the feed publishes and subscribes through: the installed Broadcast backend, or the app's `transport`. */
type Channel = {
  publish(key: string, payload: string): unknown
  subscribe(key: string, listener: Listener): BackendSubscription
}

/** Sends this host's committed changes to the other hosts, and hands theirs to `onRemote`, one key per table. A host
 *  is a process, or a session Durable Object on Cloudflare: several of those in one isolate are other hosts. */
class ChangeFeed {
  private readonly channel: Channel
  private readonly database: string
  private readonly onRemote: (changes: Change[]) => void
  private readonly origin = crypto.randomUUID()
  /** By table. */
  private readonly subscriptions = new Map<string, BackendSubscription>()
  /** Per table: the highest sequence number received, and those of the last `RECEIVED_WINDOW`, each at its number
   *  modulo `RECEIVED_WINDOW`. */
  private readonly received = new Map<string, { highest: number; recent: number[] }>()
  /** Per table, while a message is on the way: the changes committed since, sent together once it's through, or
   *  with its own if it was lost. */
  private readonly waiting = new Map<string, Change[]>()

  constructor(transport: BroadcastTransport | undefined, database: string, onRemote: (changes: Change[]) => void) {
    this.channel = transport ? channelOf(transport) : INSTALLED_BROADCAST
    this.database = database
    this.onRemote = onRemote
  }

  /** Listens to `tables`' changes: while one may not arrive yet, returns a promise settled once it can. */
  watch(tables: Iterable<string>): Promise<void> | undefined {
    const establishing: Promise<void>[] = []
    for (const table of tables) {
      const subscription = this.subscriptions.get(table) ?? this.subscribe(table)
      if (subscription.state() !== 'ready') establishing.push(subscription.ready)
    }
    if (establishing.length === 0) return undefined
    // A read that failed doesn't wait for it.
    return markHandled(Promise.all(establishing).then(() => {}))
  }

  publish(changes: Change[]): void {
    for (const [table, committed] of Map.groupBy(changes, (change) => change.table)) {
      const waiting = this.waiting.get(table)
      if (waiting) enqueue(waiting, committed)
      else void this.send(table, committed)
    }
  }

  /** Sends `changes`, then those committed meanwhile, one message of `table` on the way at a time, until all got
   *  through. */
  private async send(table: string, changes: Change[]): Promise<void> {
    const waiting = [...changes]
    this.waiting.set(table, waiting)
    for (let lost = 0; waiting.length > 0; ) {
      const next = waiting.splice(0)
      if (await this.sent(table, next)) lost = 0
      else {
        // Before those committed since, in the order they committed.
        enqueue(waiting, [...next, ...waiting.splice(0)])
        await pause(Math.min(RETRY_FIRST_MS * 2 ** lost++, RETRY_LAST_MS))
      }
    }
    this.waiting.delete(table)
  }

  /** Whether the message of `changes` got through by `PUBLISH_DEADLINE_MS`. */
  private async sent(table: string, changes: Change[]): Promise<boolean> {
    const message: Message = { origin: this.origin, changes }
    let timer: ReturnType<typeof setTimeout> | undefined
    const deadline = new Promise<never>((_, reject) => {
      const overdue = () =>
        reject(
          new Error(
            `@telefunc/live: ${table}'s changes weren't published within ${PUBLISH_DEADLINE_MS / 1000} s: sending them again.`,
          ),
        )
      timer = unrefTimer(setTimeout(overdue, PUBLISH_DEADLINE_MS))
    })
    try {
      await Promise.race([this.channel.publish(this.key(table), JSON.stringify(message)), deadline])
      return true
    } catch (err) {
      console.error(err)
      return false
    } finally {
      clearTimeout(timer)
    }
  }

  private subscribe(table: string): BackendSubscription {
    const subscription = this.channel.subscribe(this.key(table), (payload, { seq }) =>
      this.receive(table, payload, seq),
    )
    // Closed at once, it failed: the next read subscribes again.
    if (subscription.state() === 'closed') return subscription
    this.subscriptions.set(table, subscription)
    subscription.onStateChange((state) => {
      if (state === 'ready') return
      // It may have missed messages: any row of the table may have changed.
      this.onRemote([{ table, kind: 'unknown' }])
      if (state === 'closed' && this.subscriptions.get(table) === subscription) this.subscriptions.delete(table)
    })
    return subscription
  }

  private receive(table: string, serialized: string, seq: number): void {
    // The first message of a table follows on from none.
    const received = getOrCreate(this.received, table, () => ({ highest: seq - 1, recent: [] }))
    const slot = seq % RECEIVED_WINDOW
    if (seq > received.highest) {
      // A message was lost on the way: any row of the table may have changed.
      if (seq !== received.highest + 1) this.onRemote([{ table, kind: 'unknown' }])
      received.highest = seq
    } else if (received.highest - seq >= RECEIVED_WINDOW) {
      // Too old to tell whether it was received already: any row may have changed.
      return this.onRemote([{ table, kind: 'unknown' }])
    } else if (received.recent[slot] === seq) {
      // Received already.
      return
    }
    received.recent[slot] = seq
    const message = JSON.parse(serialized) as Message
    if (message.origin !== this.origin) this.onRemote(message.changes)
  }

  private key(table: string): string {
    return `@telefunc/live:${this.database}:${table}`
  }
}

/** Adds `changes` to those of a table waiting to be sent; past `MAX_WAITING_CHANGES`, any row of it may have changed. */
function enqueue(waiting: Change[], changes: Change[]): void {
  for (const change of changes) waiting.push(change)
  if (waiting.length > MAX_WAITING_CHANGES)
    waiting.splice(0, waiting.length, { table: waiting[0]!.table, kind: 'unknown' })
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => unrefTimer(setTimeout(resolve, ms)))
}

const INSTALLED_BROADCAST: Channel = {
  publish: (key, payload) => getBroadcastPlane().publish({ key, kind: 'text' }, payload),
  subscribe: (key, listener) => getBroadcastPlane().subscribe({ key, kind: 'text' }, listener),
}

/** An app's transport, as core's Broadcast takes it: a listener misses nothing from the moment `listen()` returns the
 *  function that stops it, or, where it returns `{ unsubscribe, ready }`, from the moment `ready` resolves. */
function channelOf(transport: BroadcastTransport): Channel {
  return {
    publish: (key, payload) => transport.send(key, payload),
    subscribe(key, listener) {
      const listening = transport.listen(key, listener)
      if (typeof listening === 'function')
        return {
          ready: Promise.resolve(),
          state: () => 'ready',
          onStateChange: () => () => {},
          unsubscribe: async () => listening(),
        }
      let state: 'establishing' | 'ready' | 'closed' = 'establishing'
      const listeners = new Set<(state: 'ready' | 'closed') => void>()
      const settle = (next: 'ready' | 'closed') => {
        if (state === 'closed') return
        state = next
        for (const onState of listeners) onState(next)
      }
      const ready = markHandled(
        listening.ready.then(
          () => settle('ready'),
          async (error: unknown) => {
            // Released before the read hears of the refusal, so that its next attempt isn't refused for this one.
            try {
              await listening.unsubscribe()
            } catch (releaseError) {
              console.error(releaseError)
            }
            settle('closed')
            throw error
          },
        ),
      )
      return {
        ready,
        state: () => state,
        onStateChange(onState) {
          listeners.add(onState)
          return () => listeners.delete(onState)
        },
        async unsubscribe() {
          state = 'closed'
          listening.unsubscribe()
        },
      }
    },
  }
}
