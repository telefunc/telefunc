import { expect, test } from 'vitest'
import { REDIS_COMMANDS } from './commands.js'

// ioredis holds a command's arguments while it connects or retries, past the call that made them.
test("a binary publish's arguments hold their own bytes, so the caller reusing its buffer changes nothing sent", () => {
  const payload = new Uint8Array([1, 1, 1])
  const { argv } = REDIS_COMMANDS.publish.invoke('tf:', { route: { key: 'k', kind: 'binary' }, payload })
  payload.set([2, 2, 2])
  expect(Array.from(argv[0] as Buffer)).toEqual([1, 1, 1])
})
