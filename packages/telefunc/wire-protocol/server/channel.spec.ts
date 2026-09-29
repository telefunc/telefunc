import { expect, test } from 'vitest'
import { ServerChannel } from './channel.js'
import { ERROR_REASON, ProtocolViolationError, TAG } from '../shared-ws.js'

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

test("a page's ERROR is a protocol violation unless its reason is a replay loss", () => {
  const channel = new ServerChannel()
  const error = (seq: number, reason: number) => ({ tag: TAG.ERROR, index: 0, seq, reason }) as const
  expect(() => channel._dispatchFrame(error(1, ERROR_REASON.BUG))).toThrow(ProtocolViolationError)
  expect(channel.isClosed).toBe(false)
  channel._dispatchFrame(error(2, ERROR_REASON.LOST))
  expect(channel.isClosed).toBe(true)
})
