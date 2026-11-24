// A channel the server ended sends nothing more from the page: what of it waits in the event stream's batch, unsent,
// goes with it. The batch waits a second before it goes, so the server's end comes while the send waits in it.

import { expect, test } from 'vitest'
import { open, until, useRealServer, world } from './realServer.fixture.js'

useRealServer(['sse'], { ssePostIdleFlushDelay: 1_000 })

test("a send waiting in the batch when the server ends its channel isn't uploaded, and its sibling's next send is", async () => {
  const [ended, sibling] = await open(2)
  if (!ended || !sibling) throw new Error('no pairs')
  void ended.page.send('obsolete').catch(() => {})
  ended.channel.abort('server stopped')
  await until(() => ended.ended.length > 0)
  void sibling.page.send('still active')
  await until(() => sibling.got.includes('still active'))
  expect(world.posted).toEqual(['still active'])
})
