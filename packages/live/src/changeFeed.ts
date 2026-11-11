export { ChangeFeed }

import type { BroadcastTransport } from 'telefunc'
import { getBroadcastPlane, hostLocal, markHandled, type BackendSubscription } from 'telefunc/__internal'
import type { Change } from './match/liveRead.js'

type Message = { origin: string; changes: Change[] }

type Listener = (payload: string, ordering: { seq: number }) => void

/** What the feed publishes and subscribes through: the installed Broadcast backend, or the app's `transport`. */
type Channel = {
  publish(key: string, payload: string): unknown
  subscribe(key: string, listener: Listener): BackendSubscription
}

/** Sends this server's committed changes to the other servers, and hands theirs to `onRemote`, one key per table. */
class ChangeFeed {
  private readonly channel: Channel
  private readonly database: string
  private readonly onRemote: (changes: Change[]) => void
  private readonly origin = crypto.randomUUID()
  /** This feed's subscriptions on each host (a Cloudflare session Durable Object, else the process), by table. */
  private readonly subscriptions = Symbol('@telefunc/live subscriptions')
  /** Per table: the last message's sequence number. */
  private readonly lastSeq = new Map<string, number>()

  constructor(transport: BroadcastTransport | undefined, database: string, onRemote: (changes: Change[]) => void) {
    this.channel = transport ? channelOf(transport) : INSTALLED_BROADCAST
    this.database = database
    this.onRemote = onRemote
  }

  /** Listens to `tables`' changes on this host: while one may not arrive yet, returns a promise settled once it can. */
  watch(tables: Iterable<string>): Promise<void> | undefined {
    const subscriptions = hostLocal(this.subscriptions, () => new Map<string, BackendSubscription>())
    const establishing: Promise<void>[] = []
    for (const table of tables) {
      const subscription = subscriptions.get(table) ?? this.subscribe(table, subscriptions)
      if (subscription.state() !== 'ready') establishing.push(subscription.ready)
    }
    if (establishing.length === 0) return undefined
    // A read that failed doesn't wait for it.
    return markHandled(Promise.all(establishing).then(() => {}))
  }

  publish(changes: Change[]): void {
    const byTable = Map.groupBy(changes, (change) => change.table)
    for (const [table, batch] of byTable) {
      const message: Message = { origin: this.origin, changes: batch }
      void this.send(this.key(table), JSON.stringify(message))
    }
  }

  private subscribe(table: string, subscriptions: Map<string, BackendSubscription>): BackendSubscription {
    const subscription = this.channel.subscribe(this.key(table), (payload, { seq }) =>
      this.receive(table, payload, seq),
    )
    // Closed at once, it failed: the next read subscribes again.
    if (subscription.state() === 'closed') return subscription
    subscriptions.set(table, subscription)
    subscription.onStateChange((state) => {
      if (state === 'ready') return
      // It may have missed messages: any row of the table may have changed.
      this.onRemote([{ table, kind: 'unknown' }])
      if (state === 'closed' && subscriptions.get(table) === subscription) subscriptions.delete(table)
    })
    return subscription
  }

  private async send(key: string, serialized: string): Promise<void> {
    try {
      await this.channel.publish(key, serialized)
    } catch (err) {
      // The write stands; the other servers see the gap at this table's next message.
      console.error(err)
    }
  }

  private receive(table: string, serialized: string, seq: number): void {
    const last = this.lastSeq.get(table)
    // Another host of this server delivered it already.
    if (last !== undefined && seq <= last) return
    this.lastSeq.set(table, seq)
    // A message was lost on the way: any row of the table may have changed.
    if (last !== undefined && seq !== last + 1) this.onRemote([{ table, kind: 'unknown' }])
    const message = JSON.parse(serialized) as Message
    if (message.origin !== this.origin) this.onRemote(message.changes)
  }

  private key(table: string): string {
    return `@telefunc/live:${this.database}:${table}`
  }
}

const INSTALLED_BROADCAST: Channel = {
  publish: (key, payload) => getBroadcastPlane().publish({ key, kind: 'text' }, payload),
  subscribe: (key, listener) => getBroadcastPlane().subscribe({ key, kind: 'text' }, listener),
}

/** An app's transport: its `listen()` misses nothing from the moment it returns, as core's Broadcast takes it. */
function channelOf(transport: BroadcastTransport): Channel {
  return {
    publish: (key, payload) => transport.send(key, payload),
    subscribe(key, listener) {
      const stop = transport.listen(key, listener)
      return {
        ready: Promise.resolve(),
        state: () => 'ready',
        onStateChange: () => () => {},
        unsubscribe: async () => stop(),
      }
    },
  }
}
