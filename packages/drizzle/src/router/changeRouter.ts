// The positioned change router (final-plan §5.6, audit switch #7; T5.G). One `ingest` per
// positioned batch: a predecessor-linked cursor per source suppresses duplicates and detects
// gaps (a non-adjacent-but-LINKED position is NOT a gap). An accepted batch fans to each
// affected graph's `apply(union-slice)` EXACTLY ONCE, synchronously, in source order — never
// merged across batches — then notifies ≤1 per graph AND per attached identity (a multi-table
// identity reached through two per-table sentinels is deduped to one). A detected gap, a
// throwing apply, or an unserializable/oversize batch fails closed to a coarse resync
// (invalidate once + re-warm stateful); the cursor advances ONLY after successful routing, so
// loss is always converted to sound coarse recovery, never silence.

export { type RoutableGraph, type Router, type RouterInspection, createRouter }

import type { ApplyOutcome } from '../graph/liveGraph.js'
import { type ChangeBatch, type SerializeResult, type TableChange, serializeBatch } from './events.js'

/** What the router indexes and drives. A normal graph yields its own identity key; a coarse
 *  sentinel yields every attached identity key (so the batch-level dedup fires each once). */
type RoutableGraph = {
  readonly tables: readonly string[]
  apply(changes: TableChange[]): ApplyOutcome
  notifyKeys(): Iterable<string>
  /** gap / resync → re-warm stateful graphs (coarse graphs no-op). */
  resync(): void
}

type RouterInspection = {
  sources: number
  graphs: number
  counters: { accepted: number; duplicates: number; gaps: number; degraded: number; notifies: number }
}

type Router = {
  register(graph: RoutableGraph): void
  unregister(graph: RoutableGraph): void
  ingest(batch: ChangeBatch): void
  inspect(): RouterInspection
}

function createRouter(config: {
  notify: (identityKey: string) => void
  maxBatchBytes?: number
}): Router {
  const index = new Map<string, Set<RoutableGraph>>()
  const all = new Set<RoutableGraph>()
  const cursors = new Map<string, number>()
  const counters = { accepted: 0, duplicates: 0, gaps: 0, degraded: 0, notifies: 0 }

  const notifyAll = (keys: Set<string>): void => {
    for (const key of keys) {
      counters.notifies++
      config.notify(key)
    }
  }

  const resyncAll = (): void => {
    const keys = new Set<string>()
    for (const graph of all) {
      graph.resync()
      for (const key of graph.notifyKeys()) keys.add(key)
    }
    notifyAll(keys)
  }

  const routeBatch = (batch: ChangeBatch): void => {
    const touched = new Set<RoutableGraph>()
    for (const change of batch.changes) for (const graph of index.get(change.table) ?? []) touched.add(graph)
    const keys = new Set<string>()
    for (const graph of touched) {
      const slice = batch.changes.filter((change) => graph.tables.includes(change.table))
      if (graph.apply(slice).invalidated) for (const key of graph.notifyKeys()) keys.add(key)
    }
    notifyAll(keys)
  }

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
      const last = cursors.get(batch.sourceId)
      if (last !== undefined) {
        // duplicate / stale: already at or behind the cursor → drop, no apply, no advance.
        if (batch.position <= last || (batch.predecessor !== null && batch.predecessor < last)) {
          counters.duplicates++
          return
        }
        // gap: the frame does not link to the last accepted position → coarse resync.
        if (batch.predecessor !== last) {
          counters.gaps++
          resyncAll()
          cursors.set(batch.sourceId, batch.position)
          return
        }
      }
      // Oversize / unserializable payloads degrade to a coarse resync (never a silent drop).
      if (!batch.heartbeat && batch.changes.length > 0) {
        const encoded: SerializeResult = serializeBatch(batch, config.maxBatchBytes)
        if (!encoded.ok) {
          counters.degraded++
          resyncAll()
          cursors.set(batch.sourceId, batch.position)
          return
        }
      }
      // Heartbeat: liveness only — advance the cursor, never apply or notify.
      if (batch.heartbeat) {
        counters.accepted++
        cursors.set(batch.sourceId, batch.position)
        return
      }
      // Accepted data batch: a throwing apply fails closed to coarse and does NOT advance.
      try {
        routeBatch(batch)
      } catch {
        counters.degraded++
        resyncAll()
        return
      }
      counters.accepted++
      cursors.set(batch.sourceId, batch.position)
    },
    inspect: () => ({ sources: cursors.size, graphs: all.size, counters: { ...counters } }),
  }
}
