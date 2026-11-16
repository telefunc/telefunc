// spellcheck-ignore:file  nodejs_als is Cloudflare's AsyncLocalStorage compatibility flag
// A Worker without the `nodejs_als` flag: telefunc has no AsyncLocalStorage, so no scope names the Durable Object.

import { expect, it } from 'vitest'
import { CloudflareBroadcastTransport } from './broadcast.js'

it('a subscriber with no page, without AsyncLocalStorage, throws an error that names the flag', () => {
  const transport = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc', scale: 1 })
  transport.attachIsolateInfo('weur')
  expect(() =>
    transport.runInObject('telefunc-shard-weur-0', () => transport.subscribe('room:test', () => {})),
  ).toThrow('add "nodejs_als" to the compatibility_flags')
})
