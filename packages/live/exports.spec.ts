// The export map serves the server entries on Cloudflare Workers: wrangler resolves with the `workerd`, `worker` and
// `browser` conditions, and an entry's `browser` file is the client stub.

import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

type Entry = Record<string, string>
const { exports } = JSON.parse(readFileSync(new URL('./package.json', import.meta.url), 'utf8')) as {
  exports: Record<string, string | Entry>
}
const serverEntries = Object.entries(exports).filter(
  (entry): entry is [string, Entry] => typeof entry[1] === 'object' && 'browser' in entry[1],
)

describe('@telefunc/live exports', () => {
  it('has server entries with a browser stub', () => {
    expect(serverEntries.map(([path]) => path)).toEqual(['.', './drizzle', './kysely', './prisma'])
  })
  for (const [path, entry] of serverEntries) {
    it(`serves ${path} to workerd`, () => {
      const conditions = Object.keys(entry)
      for (const condition of ['workerd', 'worker']) {
        expect(entry[condition]).toBe(entry.default)
        expect(conditions.indexOf(condition)).toBeLessThan(conditions.indexOf('browser'))
      }
    })
  }
})
