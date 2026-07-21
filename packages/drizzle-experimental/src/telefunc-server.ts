// THE AUTO-LOAD SERVER ENTRY — discovered by telefunc's extension scanner, never imported by hand.
//
// Core scans the project root's package.json for `@telefunc/*` dependencies whose own manifest declares
// `"telefunc": { "server": … }` and injects `import '<specifier>'` AHEAD OF every transformed
// `.telefunc.*` module (node/shared/discoverExtensions.ts — the same seam @telefunc/rxjs registers
// through). The registration therefore runs the moment any telefunc module evaluates on the server —
// before serialization for any SSR-rendered or otherwise-preloaded request. It is NOT eager at process
// start: core's virtual entry glob is lazy, so a DIRECT first request with no prior server import
// evaluates the module only at loadTelefuncFiles — AFTER that request's config snapshot — and its Live
// serializes unreplaced. That first-request race is the standing envelope of every `telefunc.server`
// extension on main (@telefunc/rxjs included); every later request is covered, and `reactiveDrizzle()`'s
// idempotent registration narrows nothing further. Core's snapshot ordering is the ledgered core
// follow-up (TODO ledger: extension registered-during-load blindness); a core fix removes the residual
// with no change here.
//
// ONLY the wire half, deliberately: this module must not import drizzle-orm or the engine. Registering a
// replacer costs a config push; dragging the capture machinery into every server boot would not.
import { installLiveReplacer } from './primitive/wireServer.js'

installLiveReplacer()
