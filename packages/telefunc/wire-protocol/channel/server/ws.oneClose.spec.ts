// A WebSocket's session end on the real Node server, in the turn its send backlog passed: the page reads the server's
// cause from the socket's close, and no later loss terminates the socket after it. Node writes the close frame before a
// later terminate, so the page alone can't tell; the row counts the transport's terminations too, as a runtime whose
// terminate cuts what the socket holds would show them.

import { createServer } from 'node:http'
import { expect, test, vi } from 'vitest'
import { Telefunc } from '../../../serve/node.js'
import { MAX_CHANNELS_PER_CONNECTION, WIRE_VERSION } from '../../constants.js'
import { encode, lengthBytes } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { END_CAUSE } from '../endCause.js'
import { getChannelMux } from './ChannelMux.js'

/** Each socket the WebSocket transport terminated. */
const terminated: unknown[] = []
vi.mock('./ws.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('./ws.js')>()
  return {
    getTelefuncChannelOptions: (options: NonNullable<Parameters<typeof actual.getTelefuncChannelOptions>[0]>) =>
      actual.getTelefuncChannelOptions({
        ...options,
        terminate: (peer) => {
          terminated.push(peer)
          peer.terminate()
        },
      }),
  }
})

// The send backlog's base at 1 MiB, not 64 MiB: the PONGs that pass it are built in milliseconds.
vi.mock('../../constants.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../constants.js')>()),
  WIRE_SEND_BACKLOG_BASE_BYTES: 2 ** 20,
}))

/** A PING listing as many channels as a page may have, none of them open: its PONG answers each, about 28 KB. */
const widePing = () => encode.ping(Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, (_, ix) => ({ ix, lastSeq: 0 })))

test("a session that ends in the turn its wire's send backlog passed is told by the socket's close, its code and its cause, and nothing terminates the socket after it", async () => {
  const telefunc = new Telefunc()
  const server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('Expected a TCP address')
  // The RECONCILED stays owed, so what the wire sends is held for it and counts against its backlog.
  const owed = vi.spyOn(getChannelMux(), 'sendReconciled').mockImplementation(() => {})
  const page = new WebSocket(`ws://127.0.0.1:${address.port}/_telefunc?wire=${WIRE_VERSION}`)
  page.binaryType = 'arraybuffer'
  const closed = new Promise<CloseEvent>((resolve) => page.addEventListener('close', resolve))
  try {
    await new Promise((resolve) => page.addEventListener('open', resolve))
    page.send(encode.reconcile({ sessionId: crypto.randomUUID(), attempt: 1, open: [] }))
    await new Promise((resolve) => setTimeout(resolve, 100))
    // One message: PINGs whose PONGs pass the backlog as each is answered, then bytes the server can't frame, which end
    // the session in the same turn.
    const frames = [...Array.from({ length: 100 }, widePing), new Uint8Array([TAG.LENGTH, 0, 0, 0])]
    const bytes = frames.reduce((n, f, i) => n + f.byteLength + (i < frames.length - 1 ? lengthBytes(f[0] ?? 0) : 0), 0)
    page.send(encode.message(frames, bytes))
    const { code, reason } = await closed
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect([code, reason, terminated.length]).toEqual([
      4002,
      `text/vnd.telefunc.refusal; kind=frames; cause=${END_CAUSE.unreadable.code}`,
      0,
    ])
  } finally {
    owed.mockRestore()
    page.close()
    server.close()
  }
})
