import { expect, test } from 'vitest'
import { ServerChannel } from './channel.js'

test("a channel listener that stops listening itself doesn't make the next one miss the message", () => {
  const channel = new ServerChannel<string, never>()
  const seen: string[] = []
  const unlisten = channel.listen((message) => {
    seen.push(`once:${message}`)
    unlisten()
  })
  channel.listen((message) => void seen.push(`other:${message}`))
  channel._onPeerMessage(JSON.stringify('one'), 5)
  channel._onPeerMessage(JSON.stringify('two'), 5)
  expect(seen).toEqual(['once:one', 'other:one', 'other:two'])
  channel.abort()
})
