import { expect, test } from 'vitest'
import { executionAsyncId } from 'node:async_hooks'
import { once } from 'node:events'
import { createServer, get } from 'node:http'
import { Telefunc } from './node.js'

// Its own file, so its own process: on Node 22 the first `AsyncLocalStorage.run()` of a process turns on an async hook
// for the whole process, after which every promise of it gets an async id.

test("an app on `serve({ req, res })` that doesn't call `installWebSocket()` turns on no async hook: its promises get no async id", async () => {
  const telefunc = new Telefunc()
  const server = createServer(async (req, res) => {
    if (!(await telefunc.serve({ req, res }))) res.end('the app')
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('no port')
  // A route of the app's: its requests reach `serve()`, which takes the server's upgrades by itself.
  for (const _ of [1, 2]) {
    await new Promise<void>((resolve) =>
      get(`http://127.0.0.1:${address.port}/page`, (response) => response.resume().once('end', resolve)),
    )
  }
  server.close()
  await once(server, 'close')
  expect(await Promise.all([1, 2].map(() => Promise.resolve().then(() => executionAsyncId())))).toEqual([0, 0])
})
