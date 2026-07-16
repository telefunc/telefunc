export { EXTENSION_NAME }

import { config } from 'telefunc'
import { _installDbLiveRuntime } from './live/reactiveDrizzle.js'
import { assembleDbLiveRuntime } from './live/dbLiveRuntime.js'

/** The name the drizzle server extension registers under (de-duped by name). */
const EXTENSION_NAME = '@telefunc/drizzle'

// Install the db.live runtime so the reactiveDrizzle client surface can acquire per-request carriers and
// wrap live selects (read-capture — EngineFix's engine + the carrier lifecycle). The write-capture
// settled-flush hook is wired with U4. Idempotent: re-import just re-installs the same runtime.
_installDbLiveRuntime(assembleDbLiveRuntime())

// Self-registration. The app build injects `import '@telefunc/drizzle/server'` (from the
// `"telefunc".server` manifest key), and importing this module pushes the extension into
// telefunc's config — exactly like @telefunc/tanstack-query. `config.extensions.push`
// de-dupes by name (replace-in-place), so re-imports are idempotent. The settled-flush
// hook that publishes buffered invalidations is wired with the write-capture unit.
config.extensions.push({ name: EXTENSION_NAME })
