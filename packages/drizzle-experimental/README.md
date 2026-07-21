# `@telefunc/drizzle-experimental`

Experimental reactive Drizzle queries for Telefunc. The package derives live-query invalidation from the SQL a
telefunction reads and writes.

## Relational source seam

The engine has no source plugin registry and does not need a second adapter to justify one. Its source boundary is
the five contracts already used by the Drizzle quarantine:

| Contract | Stable side | Drizzle side |
| --- | --- | --- |
| `QueryShape` | `ir/types.ts` describes source-neutral relations, predicates, joins, grouping, windows, and set operations. | `drizzle/extract/queryShape.ts` reads a Drizzle builder into that IR. |
| Instance key | The graph registry shares state only for an identical `instanceKey`. | `drizzle/extract/identity.ts` derives the key from parameterized SQL, typed bindings, schema fingerprint, dialect, and semantic environment. |
| `HydrationExecutor` | `engine/graph/hydrate.ts` asks only for `scan(descriptor)`. | `drizzle/binding/hydrationExecutor.ts` turns a seed descriptor into a Drizzle query against the bound database. |
| Source facts | The core receives dialect, session provability, RLS status, semantic environment, and schema identity as inputs to its precision decisions. | `drizzle/binding/database.ts` and the extraction modules prove or conservatively report those facts from the live Drizzle objects. |
| `ChangeBatch → ingest` | `bus/router` routes source-neutral table changes into registered graphs. | The write-capture family emits `ChangeBatch` values; `bus/dbRuntime.ts` feeds them locally and the change runtime transports them between instances. |

The dependency is one-way: modules under `drizzle/` may adapt into `ir/`, `engine/`, and `bus/`; those stable-core
directories never import the Drizzle quarantine. `engine/compile/importGraph.spec.ts` enforces that boundary.
