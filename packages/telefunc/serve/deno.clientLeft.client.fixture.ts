// The client of serve/deno.clientLeft.spec.ts's rows, which its Deno server (deno.clientLeft.server.fixture.mjs) runs in
// its own process: a row's binary call with a File, its bytes built by the page's own encoder, over a raw socket, which
// alone holds a body open at an exact part and leaves at an exact moment.

import { connect, type Socket } from 'node:net'
import { once } from 'node:events'
import { SERIALIZER_PREFIX_BLOB, WIRE_VERSION } from '../wire-protocol/constants.js'
import { encodeRequestEnvelope } from '../wire-protocol/wire/envelope.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER } from '../wire-protocol/wire/requestKind.js'

export { PART, TELEFUNC_FILE, WINDOW_MS, sendRest, startCall }
export type { Row }

/** The rows' telefunctions (deno.clientLeft.telefunc.js), as Telefunc loads them. */
const TELEFUNC_FILE = '/deno.clientLeft.telefunc.js'
/** The channels' connect window (`config.channel.connectTtl`). */
const WINDOW_MS = 1000
/** The File's bytes go in parts of this size. */
const PART = 64 * 1024
/** How long a part may wait for the socket to drain before the upload counts as stalled. */
const STALL_MS = 5000

/** One call: `telefunction` (an export of `TELEFUNC_FILE`) with a File of `parts` × 64 KiB, read `readAfterMs` after the
 *  call (never where it is null), and what its client does after the File's first part: it sends the other parts, one
 *  each `pacedMs` (0: at once), or it leaves `leaveAfterMs` after the answer begins. */
type Row = {
  telefunction: string
  row: string
  parts: number
  readAfterMs: number | null
  rest: { pacedMs: number } | { leaveAfterMs: number }
}

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/** Sends the head, envelope and first file part, and reports when the answer begins. */
async function startCall(port: number, row: Row): Promise<{ socket: Socket; answered: Promise<void> }> {
  const size = row.parts * PART
  const file = `${SERIALIZER_PREFIX_BLOB}${JSON.stringify({ index: 0, size, type: '' })}`
  const metadata = JSON.stringify({
    file: TELEFUNC_FILE,
    name: row.telefunction,
    args: [file, row.row, row.readAfterMs],
  })
  const envelope = new Uint8Array(await encodeRequestEnvelope(metadata, []).arrayBuffer())
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
    `POST /_telefunc?wire=${WIRE_VERSION} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\n${REQUEST_KIND_HEADER}: ${REQUEST_KIND.BINARY}\r\nContent-Type: application/octet-stream\r\nContent-Length: ${envelope.length + size}\r\n\r\n`,
  )
  socket.write(envelope)
  socket.write(part())
  return { socket, answered }
}

/** Does `row`'s rest on `socket`, whose first part is sent: `'all sent'`, where it stalled, or `'left'`. */
async function sendRest(socket: Socket, row: Row): Promise<string> {
  if ('leaveAfterMs' in row.rest) {
    await pause(row.rest.leaveAfterMs)
    socket.destroy()
    return 'left'
  }
  for (let sent = 1; sent < row.parts; sent++) {
    if (row.rest.pacedMs > 0) await pause(row.rest.pacedMs)
    if (socket.write(part())) continue
    const drained = await Promise.race([once(socket, 'drain').then(() => true), pause(STALL_MS).then(() => false)])
    if (!drained) return `stalled after ${sent + 1} of ${row.parts} parts`
  }
  return 'all sent'
}

function part() {
  return new Uint8Array(PART).fill(120)
}
