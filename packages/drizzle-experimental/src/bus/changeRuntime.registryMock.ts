// The `./dbRuntime.js` replacement used by the change-runtime specs, and the spy engine behind it.
//
// This is a module of its own, separate from `changeRuntime.testKit.ts`, for one mechanical reason: a
// `vi.mock` factory that reaches the test kit would import `changeRuntime.js`, which imports `dbRuntime.js` —
// the module being mocked. That cycle deadlocks module loading and the run hangs rather than fails. Keeping
// the factory in a file that imports NOTHING from the source tree makes the cycle impossible.
//
// Each spec wires it in one line:
//
//     vi.mock('./dbRuntime.js', async () => (await import('./changeRuntime.registryMock.js')).dbRuntimeMock())

import { vi } from 'vitest'

/** The mocked registry is PER-DB and tracks registered graphs, so watchedTables() answers truthfully — the
 *  coarse-all path depends on it. */
export const engine = {
  ingest: vi.fn(),
  /** Per-receiver spies, so a test can attribute an ingest to the db it was routed to — the shared spy
   *  above also carries every sibling runtime's calls, which makes it unusable for that. */
  perDb: new Map<object, (batch: unknown) => void>(),
  watched: new Map<object, string[]>(),
}

export function dbRuntimeMock() {
  return {
    registryFor: (db: object) => ({
      router: {
        ingest: (batch: unknown) => {
          engine.perDb.get(db)?.(batch)
          engine.ingest(batch)
        },
        register: (graph: { tables: string[] }) =>
          engine.watched.set(db, [...(engine.watched.get(db) ?? []), ...graph.tables]),
        unregister: () => engine.watched.delete(db),
        watchedTables: () => engine.watched.get(db) ?? [],
      },
      acquire: vi.fn(),
    }),
    ingestWrite: vi.fn(),
  }
}

/** Call from each spec's `beforeEach`. */
export function resetEngine() {
  engine.ingest.mockClear()
  engine.watched.clear()
}
