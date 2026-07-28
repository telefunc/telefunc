import { expect, test } from 'vitest'
import { Fanout } from './fanout.js'

test('uses observable TypeScript-private storage on the Durable Object fanout path', () => {
  const fanout = new Fanout(async () => {})
  expect(Object.keys(fanout).sort()).toEqual(
    '_attempts,_chains,_defer,_deliver,_incarnationFences,_tokenSeq'.split(','),
  )
})
