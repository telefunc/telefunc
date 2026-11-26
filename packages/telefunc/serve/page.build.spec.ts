import { expect, test } from 'vitest'
import { readFile, writeFile } from 'node:fs/promises'
import { writeFileSync } from 'node:fs'
import { withBuildLock } from './page.build.fixture.js'
import { startPage } from './page.fixture.js'

test("a page that starts while another run's build rewrites the package in place runs its own run's copy, whole", async () => {
  const file = new URL('../dist/wire-protocol/channel/client/ClientChannel.js', import.meta.url)
  // Under the lock that another run's build takes, as tsc writes a file: cut to nothing first, then written.
  const leftBehind = await withBuildLock(async () => {
    const whole = await readFile(file)
    const restore = () => writeFileSync(file, whole)
    process.once('exit', restore)
    await writeFile(file, '')
    try {
      const page = await startPage({ telefuncUrl: 'http://127.0.0.1:9/_telefunc', channelId: crypto.randomUUID() })
      expect((await page.resources()).pageHideListeners).toBeGreaterThanOrEqual(0)
      return await page.end()
    } finally {
      restore()
      process.off('exit', restore)
    }
  })
  expect(leftBehind).toEqual([])
})
