// What a listener may return, as the compiler checks it: `tsc -p tsconfig.spec.json` compiles each case below, which
// is the test; they don't run.

import { expectTypeOf, test } from 'vitest'
import type { ChannelListener } from './channel.js'
import type { Channel } from './server/ServerChannel.js'

test('a listener of messages that get no answer may return anything, also from an async function', () => {
  const compiles = (channel: Channel<never, never>) => {
    channel.listen(() => 42)
    channel.listen(async () => 42)
  }
  expectTypeOf(compiles).toBeFunction()
})

test('a listener of messages that get an answer may answer, or answer some and observe others, also when async', () => {
  const compiles = (channel: Channel<(message: string) => number, never>) => {
    channel.listen(() => 1)
    channel.listen(async () => 1)
    channel.listen((message) => (message ? 1 : undefined))
    channel.listen(async (message) => (message ? 1 : undefined))
  }
  expectTypeOf(compiles).toBeFunction()
})

test('a generic callback forwards to `listen()`', () => {
  const forward = <T>(channel: Channel<T, never>, callback: ChannelListener<T>) => channel.listen(callback)
  expectTypeOf(forward).toBeFunction()
})
