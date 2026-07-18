export type { TELEFUNC_SHIELDS } from '../shared/transformer/generateShield/shield-key.js'

// The Live CONSUMER seam — a TYPE ONLY, for query adapters that ship to the BROWSER (e.g.
// @telefunc/tanstack-query: invalidate → refetch, data → cache write). Deliberately NOT on the public
// `telefunc` entry, where a Live is only `{ readonly data: T }` + `Live.derived`; and TYPE-only on purpose,
// since a runtime import from this server entry would drag the server graph into a client bundle.
//
// Server-side reactive integrations reach the PRODUCER end and per-request cleanup through the extension
// HOST (`TelefuncServerExtension.setup(host)` → `host.createLive` / `host.onRequestCleanup`), NOT here — so
// this entry no longer exposes `LiveCell` / `onPostSerialize` / `drainPostSerializeDisposers`, which were
// @telefunc/drizzle's only cross-package reach into telefunc internals.
export type { LiveSubscription } from './live/live.js'
