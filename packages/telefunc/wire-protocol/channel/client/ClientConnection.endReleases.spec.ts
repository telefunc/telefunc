// A connection that ended for good lets go of what it held unsent, also while the app keeps one of its channels: a
// REAL ClientChannel and its connection against a scripted server (scriptedServer.fixture.ts).

import { expect, test } from 'vitest'
import { PieceSender } from '../../flowControl/pieces.js'
import { wireEnd } from '../../refusal.js'
import { encode } from '../../wire/frame.js'
import { END_CAUSE } from '../endCause.js'
import { FakeWebSocket, advance, openChannel, useScriptedServer } from './scriptedServer.fixture.js'

useScriptedServer()

/** The piece sender of the WebSocket wire `connection` runs on. */
function senderOf(connection: object): PieceSender {
  const transport: unknown = Reflect.get(connection, 'transport')
  const pieces: unknown = typeof transport === 'object' && transport !== null ? Reflect.get(transport, 'pieces') : null
  const sender: unknown = typeof pieces === 'object' && pieces !== null ? Reflect.get(pieces, 'sender') : null
  if (!(sender instanceof PieceSender)) throw new Error('no piece sender')
  return sender
}

test("a WebSocket channel the server's session end closes, which the app keeps, holds none of the frame it was sending", async () => {
  const page = openChannel(['ws'])
  await advance(500)
  await page.channel.opened
  const connection = page.channel._connection
  const socket = FakeWebSocket.open
  if (socket === undefined) throw new Error('no socket')
  void page.channel.sendBinary(new Uint8Array(256 * 1024)).catch(() => {})
  await advance(1_000)
  // The server acknowledges it before the larger one starts: the sender knows the uplink, and sends that one in pieces.
  socket.deliver(encode.piecesAck(senderOf(connection).transmittedBytes, 0, 1_000_000))
  const size = 8 * 1024 * 1024
  void page.channel.sendBinary(new Uint8Array(size)).catch(() => {})
  expect(connection.bufferedAmount()).toBeGreaterThan(size / 2)
  socket.refuse(wireEnd({ kind: 'frames', cause: END_CAUSE.receiveBudget.code }))
  expect((await page.channel.closed)?.name).toBe('ChannelOverflowError')
  expect(connection.bufferedAmount()).toBe(0)
})
