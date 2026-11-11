export { LOGGED_SQLITE_ENGINE, RETURNING_SQLITE_ENGINE }

import type { Engine } from '../liveDatabase.js'
import { catalogOf, catalogQuery, changeLogSetup, changesOfLog, DRAIN_CHANGES } from './catalog.js'
import { planSqlite } from './plan.js'

/** SQLite whose writes give their rows through a `RETURNING` of ours (D1, which refuses temporary tables). */
const RETURNING_SQLITE_ENGINE: Engine = {
  loadCatalog: async (query) => catalogOf(await query(catalogQuery(false))),
  plan: planSqlite,
  failureAbortsTransaction: false,
}

/** SQLite on a connection of its own that logs the rows its statements change, those its triggers and foreign keys
 *  change included (better-sqlite3, bun:sqlite, node:sqlite, a local libSQL file). */
const LOGGED_SQLITE_ENGINE: Engine = {
  loadCatalog: async (query) => catalogOf(await query(catalogQuery(true))),
  loadCatalogSync: (query) => catalogOf(query(catalogQuery(true))),
  setup: changeLogSetup,
  changeLog: { drain: DRAIN_CHANGES, changesOf: changesOfLog },
  plan: planSqlite,
  failureAbortsTransaction: false,
}
