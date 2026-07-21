// The change router: something emits ordered, atomic TableChange batches; the router fans each
// batch to every affected graph's `apply(union-slice)` EXACTLY ONCE, synchronously, in order — one
// commit = one graph tick, never merged across batches — then notifies ≤1 per graph AND per attached
// identity. A graph whose `apply` throws is isolated: its identity is coarse-notified so its
// subscribers re-read while the other graphs in the batch are unaffected — the error is SURFACED
// (structured log), never swallowed. A batch may also carry an in-batch `kind:'coarse'`
// marker for a table (an image-less mutation the source can't represent precisely): every graph on
// that table is demoted to coarse and notified, never fed a row — precise state is never corrupted
// by a fabricated image.

export { type RoutableGraph, type Router, createRouter }

import type { ApplyOutcome } from '../engine/graph/liveGraph.js'
import { report } from '../live/captureReport.js'
import { describeRelationId, parseRelationId } from '../ir/relation.js'
import type { ChangeBatch, TableChange } from './events.js'

/** What the router indexes and drives. Each graph yields its own identity key; a batch that touches
 *  one graph through several tables still notifies it ≤1. */
type RoutableGraph = {
  readonly tables: readonly string[]
  apply(changes: TableChange[]): ApplyOutcome
  notifyKey(): string
  /** A caught apply() throw permanently demotes this graph to coarse — its precise state is corrupt. */
  fault(): void
  /** An in-batch coarse marker for one of this graph's tables (an image-less mutation the source cannot
   *  represent precisely): invalidate, then REBUILD precise state from the database rather than
   *  surrendering it permanently. Never fed a row. */
  reseed(): void
}

type Router = {
  register(graph: RoutableGraph): void
  unregister(graph: RoutableGraph): void
  ingest(batch: ChangeBatch): void
  /** Every table that currently has at least one registered graph. The write side needs this for a
   *  mutation whose touched tables are UNKNOWABLE (raw SQL): it reaches exactly the tables something is
   *  actually watching, rather than guessing or silently missing them. */
  watchedTables(): string[]
}

function createRouter(config: { notify: (identityKey: string) => void }): Router {
  const index = new Map<string, Set<RoutableGraph>>()
  const watchForSplitDeclaration = splitDeclarationWatch()

  return {
    register(graph) {
      for (const table of graph.tables) {
        watchForSplitDeclaration(table)
        const set = index.get(table) ?? new Set()
        set.add(graph)
        index.set(table, set)
      }
    },
    unregister(graph) {
      for (const table of graph.tables) {
        const set = index.get(table)
        if (!set) continue
        set.delete(graph)
        if (set.size === 0) index.delete(table) // keep watchedTables() free of emptied entries
      }
    },
    watchedTables() {
      return [...index.keys()]
    },
    ingest(batch) {
      const touched = new Set<RoutableGraph>()
      for (const change of batch.changes) {
        watchForSplitDeclaration(change.table)
        for (const graph of index.get(change.table) ?? []) touched.add(graph)
      }
      const keys = new Set<string>()
      for (const graph of touched) {
        const slice = batch.changes.filter((change) => graph.tables.includes(change.table))
        let invalidated: boolean
        if (slice.some((change) => change.kind === 'coarse')) {
          // An image-less marker for one of this graph's tables: the source can't represent the
          // mutation precisely, so demote this graph to coarse and feed it NO row (applying a
          // fabricated/partial row would corrupt exact operator/shadow state). One commit = one tick:
          // any precise changes in the same slice are moot once the graph is coarse.
          graph.reseed()
          invalidated = true
        } else {
          try {
            invalidated = graph.apply(slice).invalidated
          } catch (error) {
            // A routed apply threw (a latent bug leaving state possibly corrupt): PERMANENTLY demote
            // this graph to coarse (fault) so no LATER batch can miss over corrupt precise state, and
            // coarse-notify its identity so it re-reads.
            report('apply-threw', { cause: error })
            graph.fault()
            invalidated = true
          }
        }
        if (invalidated) keys.add(graph.notifyKey())
      }
      for (const key of keys) config.notify(key)
    },
  }
}

/** Watch the identities flowing through one router for the ONE configuration that can silently miss.
 *
 *  A relation declared without a schema gets the unqualified identity; declared with one, it gets the
 *  qualified identity. Those are deliberately different — `a.users` and `b.users` are different physical
 *  relations and conflating them would invalidate queries that are provably unaffected. But if a user
 *  declares ONE physical table BOTH ways and reads through one declaration while writing through the
 *  other, the write routes to an identity the read is not watching, and the invalidation is simply lost.
 *
 *  Routing them together is not the fix: whether bare `users` and `app.users` are the same relation
 *  depends on the session's `search_path`, which resolves each unqualified name against the schemas that
 *  actually contain it — and on a pooled connection that authority is not provable at all. Linking them
 *  unconditionally would re-introduce the cross-schema false invalidation the qualified identity exists to
 *  prevent. So the rule stands, but it does not have to stand SILENTLY: seeing both forms of one name is
 *  exactly the configuration the limit describes, and saying so turns a missed invalidation into a
 *  diagnosable one. Warned once per name — this is a standing misconfiguration, not a per-batch event. */
function splitDeclarationWatch(): (relationId: string) => void {
  const noted = new Set<string>() // identities already accounted for — keeps `ingest` O(1) per distinct id
  const qualified = new Set<string>()
  const unqualified = new Set<string>()
  const warned = new Set<string>()

  return (relationId) => {
    if (noted.has(relationId)) return
    noted.add(relationId)
    let relation: { name: string; schema?: string }
    try {
      relation = parseRelationId(relationId)
    } catch {
      return // a malformed identity is not this watcher's error to raise
    }
    ;(relation.schema === undefined ? unqualified : qualified).add(relation.name)
    if (warned.has(relation.name) || !qualified.has(relation.name) || !unqualified.has(relation.name)) return
    warned.add(relation.name)
    console.warn(
      `[@telefunc/drizzle-experimental] the relation "${relation.name}" is declared both with and without a schema (${describeRelationId(relationId)} and its counterpart). ` +
        'If those are the SAME physical table, a write through one declaration will not invalidate a live query that read through the other — the invalidation is lost, not delayed. ' +
        'Declare the relation once. If they are genuinely different tables in different schemas, this is expected and can be ignored.',
    )
  }
}
