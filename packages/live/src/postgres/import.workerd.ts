export { importPostgres }

import { assertUsage } from '../assert.js'

/** A bundler takes in every module an import names: here the engine comes from `@telefunc/live/postgres`, which an app
 *  on D1 doesn't import, so its Worker doesn't carry Postgres's parser. */
function importPostgres(): Promise<typeof import('./engine.js')> {
  assertUsage(
    false,
    "On Cloudflare Workers, a Postgres database needs `import '@telefunc/live/postgres'`, e.g. in the module wrapping it.",
  )
}
