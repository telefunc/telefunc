// The change router (final-plan §5.6; T5.G). A ChangeSource emits ordered, atomic TableChange
// batches; the router fans each batch to every affected graph's `apply(union-slice)` EXACTLY ONCE,
// synchronously, in order — one commit = one graph tick, never merged across batches — then notifies
// ≤1 per graph AND per attached identity. Transport reliability (in-order delivery, gap/duplicate
// handling, LSN positions, reconnect) is each SOURCE's own concern behind the ChangeSource
// interface; the shared router carries NONE of it. A graph whose `apply` throws is isolated: its
// identity is coarse-notified so its subscribers re-read, and the other graphs in the batch are
// unaffected — the error is SURFACED (counter + structured log), never swallowed.

export { type RoutableGraph, type ChangeSource, type Router, type RouterInspection, createRouter }

import type { ApplyOutcome } from '../graph/liveGraph.js'
import type { ChangeBatch, TableChange } from './events.js'

/** What the router indexes and drives. Each graph yields its own identity key(s); a batch that
 *  touches one graph through several tables still notifies its identity ≤1. */
type RoutableGraph = {
  readonly tables: readonly string[]
  apply(changes: TableChange[]): ApplyOutcome
  notifyKeys(): Iterable<string>
}

/** ChangeSource: ordered atomic TableChange batches; the source owns its own reliability — a CDC
 *  source's LSN, gap detection, resync and reconnect all live INSIDE it, never here. The
 *  `@telefunc/postgres` integration point — duplicated there for CDC; the ORM binding implements it
 *  in-process. `watchTables` tells the source which relations to observe; `subscribe` delivers each
 *  committed batch and returns an unsubscribe. (Ticket 5 defines the contract; sources arrive in
 *  tickets 6–7.) */
type ChangeSource = {
  watchTables(tables: string[]): void
  subscribe(onBatch: (batch: ChangeBatch) => void): () => void
}

type RouterInspection = { graphs: number; notifies: number; applyErrors: number }

type Router = {
  register(graph: RoutableGraph): void
  unregister(graph: RoutableGraph): void
  ingest(batch: ChangeBatch): void
  inspect(): RouterInspection
}

function createRouter(config: { notify: (identityKey: string) => void }): Router {
  const index = new Map<string, Set<RoutableGraph>>()
  const all = new Set<RoutableGraph>()
  let notifies = 0
  let applyErrors = 0

  return {
    register(graph) {
      all.add(graph)
      for (const table of graph.tables) {
        const set = index.get(table) ?? new Set()
        set.add(graph)
        index.set(table, set)
      }
    },
    unregister(graph) {
      all.delete(graph)
      for (const table of graph.tables) index.get(table)?.delete(graph)
    },
    ingest(batch) {
      const touched = new Set<RoutableGraph>()
      for (const change of batch.changes) for (const graph of index.get(change.table) ?? []) touched.add(graph)
      const keys = new Set<string>()
      for (const graph of touched) {
        const slice = batch.changes.filter((change) => graph.tables.includes(change.table))
        let invalidated: boolean
        try {
          invalidated = graph.apply(slice).invalidated
        } catch (error) {
          // A routed apply threw (a latent graph bug): isolate it — coarse-notify this identity so it
          // re-reads — but SURFACE it (counter + structured log), never swallow, or the query would
          // degrade to coarse forever with no operator-visible signal.
          applyErrors++
          console.error(
            '[@telefunc/drizzle] a routed graph apply threw; coarse-notifying its identity to re-read:',
            error,
          )
          invalidated = true
        }
        if (invalidated) for (const key of graph.notifyKeys()) keys.add(key)
      }
      for (const key of keys) {
        notifies++
        config.notify(key)
      }
    },
    inspect: () => ({ graphs: all.size, notifies, applyErrors }),
  }
}
