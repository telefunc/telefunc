import { PGlite } from '@electric-sql/pglite'

// Booting a PGlite runs initdb, most of the second a `new PGlite()` takes: a spec file boots one as it imports this
// module, before its tests run, and each test starts its own from that one's data.
const empty = await dataOfEmpty()

/** A new empty PGlite, as `new PGlite()` gives one. */
export async function emptyPGlite(): Promise<PGlite> {
  const pglite = new PGlite({ loadDataDir: empty })
  await pglite.waitReady
  return pglite
}

async function dataOfEmpty(): Promise<File | Blob> {
  const pglite = new PGlite()
  // Before it's ready, a dump never settles.
  await pglite.waitReady
  const data = await pglite.dumpDataDir('none')
  await pglite.close()
  return data
}
