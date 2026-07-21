// THE AUTO-LOAD SERVER ENTRY — discovered by telefunc's extension scanner, never imported by hand.
//
// Core scans the project root's package.json for `@telefunc/*` dependencies whose own manifest declares
// `"telefunc": { "server": … }` and injects `import '<specifier>'` into every transformed `.telefunc.*`
// module (node/shared/discoverExtensions.ts — the same seam @telefunc/rxjs registers through). That runs
// the registration at server BOOT in a production build (the bundle imports telefunc files eagerly) and
// during the first SSR render in dev — in both cases before the first telefunc request resolves its
// config, which is the ordering the wire replacer needs.
//
// ONLY the wire half, deliberately: this module must not import drizzle-orm or the engine. Registering a
// replacer costs a config push; dragging the capture machinery into every server boot would not.
import { installLiveReplacer } from './primitive/wireServer.js'

installLiveReplacer()
