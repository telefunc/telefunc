// Two hosts of one database, as two session Durable Objects of one isolate are: each runs its calls with a channel mux
// of its own in context, as the Cloudflare adapter's session scope does.

import { AsyncLocalStorage } from 'node:async_hooks'
// Installs telefunc's async context, which a host's calls run in.
import 'telefunc'
import { describe, expect, it } from 'vitest'
import type { Catalog } from './catalog.js'
import { type Engine, LiveDatabase } from './liveDatabase.js'

const store = ((globalThis as any)._telefunc['getContext/async.ts'].asyncStore ??= new AsyncLocalStorage())

function onHost<T>(id: string, call: () => T): T {
  return store.run({ [Symbol.for('telefunc.channelMux')]: { hostId: id, hostLocals: new Map() } }, call)
}

describe('a catalog load', () => {
  it("isn't waited for by another host: one reset during it never settles it", async () => {
    const catalog = { role: 'app', access: new Map(), database: 'shop' } as unknown as Catalog
    const engine = {
      async loadCatalog(query: (text: string) => Promise<string>) {
        await query('the catalog query')
        return catalog
      },
    } as Engine
    const live = new LiveDatabase({}, engine)
    void onHost('reset', () => live.ready(() => new Promise<string>(() => {})))
    await onHost('other', () => live.ready(async () => ''))
    expect(live.isReady()).toBe(true)
  })
})
