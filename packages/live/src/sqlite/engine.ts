export { LOGGED_SQLITE_ENGINE, RETURNING_SQLITE_ENGINE }

import type { Engine } from '../liveDatabase.js'
import { catalogOf, catalogQuery, CHANGE_LOG_TRIGGERS, changeLogSetup, changesOfLog, DRAIN_CHANGES } from './catalog.js'
import { planSqlite } from './plan.js'

/** SQLite whose writes give their rows through a `RETURNING` of ours (D1, which refuses temporary tables). */
const RETURNING_SQLITE_ENGINE: Engine = {
  loadCatalog: async (query) => catalogOf(await query(catalogQuery(false)), false),
  plan: planSqlite,
  failureAbortsTransaction: false,
}

/** SQLite whose connections each log the rows their statements change, those their triggers and foreign keys change
 *  included (better-sqlite3, bun:sqlite, node:sqlite). */
const LOGGED_SQLITE_ENGINE: Engine = {
  loadCatalog: async (query) => catalogOf(await query(catalogQuery(true)), true),
  loadCatalogSync: (query) => catalogOf(query(catalogQuery(true)), true),
  changeLog: { triggers: CHANGE_LOG_TRIGGERS, setup: changeLogSetup, drain: DRAIN_CHANGES, changesOf: changesOfLog },
  plan: planSqlite,
  failureAbortsTransaction: false,
}
