import { expect, test } from 'vitest'

test("telefunc/__internal's browser half exports what its server half does", async () => {
  const browser = await import('./__internal.js')
  const server = await import('../node/server/__internal.js')
  expect(Object.keys(browser).sort()).toEqual(Object.keys(server).sort())
})
