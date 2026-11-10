export { EXTENSION_NAME }
export type { LiveResult }

import type { ClientChannel } from 'telefunc'

const EXTENSION_NAME = '@telefunc/live'

/** What a telefunction call made by `watch()` returns: its result, and a channel saying when it changed. */
type LiveResult = { data: unknown; changes: ClientChannel<never, 'changed'> }
