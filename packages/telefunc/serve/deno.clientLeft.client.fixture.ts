// The call that serve/deno.clientLeft.spec.ts makes, from its own process and, for a client that leaves, from its Deno
// server's (deno.clientLeft.server.fixture.mjs): a binary call to `keepsFile` (deno.clientLeft.telefunc.js).

import { connect, type Socket } from 'node:net'
import { once } from 'node:events'
import { WIRE_VERSION } from '../wire-protocol/constants.js'

export { PART, part, startCall }

/** The File's bytes go in parts of this size. */
const PART = 64 * 1024

const part = () => new Uint8Array(PART).fill(120)

/** A socket that has sent, to an entry's `port`, the head and envelope of a call to `keepsFile` with a File of `parts` ×
 *  64 KiB read `readAfterMs` after the call (never where it is null), and the File's first part: the rest of its bytes
 *  are the caller's to send. Resolves once the answer begins. */
async function startCall(port: number, row: string, parts: number, readAfterMs: number | null): Promise<Socket> {
  const size = parts * PART
  const token = `!TelefuncBlob:${JSON.stringify({ index: 0, size, type: '' })}`
  const metadata = new TextEncoder().encode(
    JSON.stringify({ file: '/deno.clientLeft.telefunc.js', name: 'keepsFile', args: [token, row, readAfterMs] }),
  )
  const envelope = new Uint8Array(4 + metadata.length)
  new DataView(envelope.buffer).setUint32(0, metadata.length)
  envelope.set(metadata, 4)
  const socket = connect(port, '127.0.0.1')
  socket.on('error', () => {})
  await once(socket, 'connect')
  const answered = new Promise<void>((resolve) =>
    socket.once('data', () => {
      socket.resume()
      resolve()
    }),
  )
  socket.write(
    `POST /_telefunc?wire=${WIRE_VERSION} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nx-telefunc-request: bin\r\nContent-Type: application/octet-stream\r\nContent-Length: ${envelope.length + size}\r\n\r\n`,
  )
  socket.write(envelope)
  socket.write(part())
  await answered
  return socket
}
