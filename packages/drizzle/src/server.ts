export { EXTENSION_NAME }

import { config } from 'telefunc'

/** The name the drizzle server extension registers under (de-duped by name). */
const EXTENSION_NAME = '@telefunc/drizzle'

// Self-registration. The app build injects `import '@telefunc/drizzle/server'` (from the
// `"telefunc".server` manifest key), and importing this module pushes the extension into
// telefunc's config — exactly like @telefunc/tanstack-query. `config.extensions.push`
// de-dupes by name (replace-in-place), so re-imports are idempotent. The settled-flush
// hook that publishes buffered invalidations is wired with the runtime layer.
config.extensions.push({ name: EXTENSION_NAME })
