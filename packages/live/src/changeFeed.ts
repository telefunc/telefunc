export { ChangeFeed }

import type { BroadcastAdapter } from 'telefunc'
import type { Change } from './match/liveRead.js'

type Message = { origin: string; changes: Change[] }

/** Sends this server's committed changes to the other servers, and hands theirs to `onRemote`, one key per table. */
class ChangeFeed {
  private readonly transport: BroadcastAdapter
  private readonly database: string
  private readonly onRemote: (changes: Change[]) => void
  private readonly origin = crypto.randomUUID()
  /** Per table: the last message's sequence number. */
  private readonly lastSeq = new Map<string, number>()

  constructor(transport: BroadcastAdapter, database: string, onRemote: (changes: Change[]) => void) {
    this.transport = transport
    this.database = database
    this.onRemote = onRemote
  }

  /** Listens to a table's changes for as long as the server runs. */
  watch(table: string): void {
    if (this.lastSeq.has(table)) return
    this.lastSeq.set(table, -1)
    this.transport.subscribe(this.key(table), (serialized, { seq }) => this.receive(table, serialized, seq))
  }

  publish(changes: Change[]): void {
    const byTable = Map.groupBy(changes, (change) => change.table)
    for (const [table, batch] of byTable) {
      const message: Message = { origin: this.origin, changes: batch }
      void this.send(this.key(table), JSON.stringify(message))
    }
  }

  private async send(key: string, serialized: string): Promise<void> {
    try {
      await this.transport.publish(key, serialized)
    } catch (err) {
      // The write stands; the other servers see the gap at this table's next message.
      console.error(err)
    }
  }

  private receive(table: string, serialized: string, seq: number): void {
    const last = this.lastSeq.get(table)!
    this.lastSeq.set(table, seq)
    // A message may have been lost (or sent before this server listened): any row of the table may have changed.
    if (seq !== last + 1) this.onRemote([{ table, kind: 'unknown' }])
    const message = JSON.parse(serialized) as Message
    if (message.origin !== this.origin) this.onRemote(message.changes)
  }

  private key(table: string): string {
    return `@telefunc/live:${this.database}:${table}`
  }
}
