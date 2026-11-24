// A PING skips the turn the page's other frames wait in; what it names of a closed channel is answered as of the frames
// the page sent before it. On the real mux, with a WebSocket the test holds.

import { expect, test } from 'vitest'
import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { ATTACHABLE_NOW, ServerChannel } from './ServerChannel.js'
import { openWebSocket } from './serverPage.fixture.js'

test("a PING right behind a channel's last frame, handed in the same turn as two frames of one read are, answers that the server has it", async () => {
  const ws = openWebSocket({ reads: true })
  const channel = new ServerChannel<string, string>()
  ws.mux.registerChannel(channel, ATTACHABLE_NOW)
  const open = [{ id: channel.id, ix: 0, lastSeq: 0, initial: true as const }]
  await ws.mux.onWireMessage(ws.wire, encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open }))
  await channel.opened
  const got: string[] = []
  channel.listen((message) => void got.push(message))
  await Promise.all([
    ws.mux.onWireMessage(ws.wire, encode.text(0, '"last"', 1)),
    ws.mux.onWireMessage(ws.wire, encode.ping([{ ix: 0, lastSeq: 1 }])),
  ])
  const answers = ws.frames.flatMap((frame) => {
    const decoded = decode(frame)
    return decoded.tag === TAG.PONG ? decoded.ended : []
  })
  expect(got).toEqual(['last'])
  expect(answers.length).toBeGreaterThan(0)
  for (const { lastSeq } of answers) expect(lastSeq === null || lastSeq >= 1).toBe(true)
  ws.close()
})
