import { expect, it } from 'vitest'

it("`@telefunc/rxjs/client` loads on the server, as a page's server render loads it", async () => {
  await expect(import('@telefunc/rxjs/client')).resolves.toBeDefined()
})
