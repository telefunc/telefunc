// The Deno server of serve/deno.clientLeft.spec.ts, which runs it from Telefunc's sources (`--sloppy-imports` takes
// their `.js` specifiers to the `.ts` files, as Vitest does) in either of Deno's signal modes. Each request names the
// entry it goes through (`x-entry`):
// - `adapter`: `telefunc/deno`, as docs/pages/Telefunc shows it;
// - `stream`: `serve({ request })`, answered with `httpResponse.getReadableWebStream()`, as docs/pages/serve shows it;
// - `string`: `serve({ request })`, answered with `httpResponse.body`, as examples/cloudflare-workers and examples/next.
// `/leave?entry=<entry>&row=<row>` makes a call whose client leaves after the answer, from this process
// (`callAndLeave`). It prints `LISTENING <port>` once it listens.

import { connect } from 'node:net'
import { once } from 'node:events'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Telefunc } from './deno.js'
import { serve } from '../node/server/telefunc.js'
import { config } from '../node/server/serverConfig.js'
import { WIRE_VERSION } from '../wire-protocol/constants.js'

config.root = dirname(fileURLToPath(import.meta.url))
config.telefuncFiles = [`${config.root}/deno.clientLeft.telefunc.js`]
config.disableNamingConvention = true
config.channel = { connectTtl: 1000 }

const PART = 64 * 1024
const telefunc = new Telefunc()
const server = Deno.serve(
  { port: 0, hostname: '127.0.0.1', onListen: ({ port }) => console.log(`LISTENING ${port}`) },
  async (request, info) => {
    const url = new URL(request.url)
    if (url.pathname === '/leave')
      return new Response(await callAndLeave(url.searchParams.get('entry'), url.searchParams.get('row')))
    const entry = request.headers.get('x-entry')
    if (entry === 'adapter') return (await telefunc.serve({ request, info })) ?? new Response(null, { status: 404 })
    const httpResponse = await serve({ request })
    const init = { status: httpResponse.statusCode, headers: httpResponse.headers }
    return new Response(entry === 'string' ? httpResponse.body : httpResponse.getReadableWebStream(), init)
  },
)

/** Makes `row`'s call to `keepsFile` through `entry`, with a File of 8 × 64 KiB read 120 ms after the call, and sends its
 *  first 64 KiB; leaves 160 ms after the answer begins. Its client is this process: where the client is another
 *  process, Deno at times fails the read on its own as the client leaves, which hides a read left pending. */
async function callAndLeave(entry, row) {
  const size = 8 * PART
  const token = `!TelefuncBlob:${JSON.stringify({ index: 0, size, type: '' })}`
  const metadata = new TextEncoder().encode(
    JSON.stringify({ file: '/deno.clientLeft.telefunc.js', name: 'keepsFile', args: [token, row, 120] }),
  )
  const start = new Uint8Array(4 + metadata.length)
  new DataView(start.buffer).setUint32(0, metadata.length)
  start.set(metadata, 4)
  const socket = connect(server.addr.port, '127.0.0.1')
  socket.on('error', () => {})
  await once(socket, 'connect')
  const answered = new Promise((resolve) =>
    socket.once('data', () => {
      socket.resume()
      resolve()
    }),
  )
  socket.write(
    `POST /_telefunc?wire=${WIRE_VERSION} HTTP/1.1\r\nHost: 127.0.0.1\r\nx-entry: ${entry}\r\nx-telefunc-request: bin\r\nContent-Type: application/octet-stream\r\nContent-Length: ${start.length + size}\r\n\r\n`,
  )
  socket.write(start)
  socket.write(new Uint8Array(PART).fill(120))
  await answered
  await new Promise((resolve) => setTimeout(resolve, 160))
  socket.destroy()
  return 'left'
}
