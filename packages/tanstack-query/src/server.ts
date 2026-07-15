import { config } from 'telefunc'
import type { TelefuncServerExtension } from 'telefunc'
import { EXTENSION_NAME, makeLiveResultHook, realDeps } from './liveResult.js'

// Server-owned liveness: the always-run result hook turns a live telefunction's result into a
// wrapper carrying an invalidation channel (see ./liveResult.ts). Registered as a side effect.
const extension = {
  name: EXTENSION_NAME,
  hooks: {
    onTelefunctionResult: makeLiveResultHook(realDeps),
  },
} satisfies TelefuncServerExtension

config.extensions.push(extension)
