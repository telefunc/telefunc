// A Fetch `Request` built from Node's request as Hono's and srvx's Node servers build one: its body is
// `Readable.toWeb(req)`, and its signal aborts once Node's request is cut short or its connection closes.

import type { IncomingMessage } from 'node:http'
import { Readable } from 'node:stream'

export function frameworkRequest(req: IncomingMessage, url: string): Request {
  const abort = new AbortController()
  req.on('close', () => {
    if (req.readableAborted) abort.abort()
  })
  req.socket.once('close', () => abort.abort())
  const init: RequestInit & { duplex: 'half' } = {
    method: 'POST',
    headers: Object.entries(req.headers).flatMap(([name, value]) =>
      value === undefined ? [] : (Array.isArray(value) ? value : [value]).map((one): [string, string] => [name, one]),
    ),
    body: domStream(Readable.toWeb(req).getReader()),
    signal: abort.signal,
    duplex: 'half',
  }
  return new Request(url, init)
}

/** The bridge's reader, read through a stream of the DOM's own type, which `RequestInit` takes: it holds nothing
 *  itself (`highWaterMark: 0`), so what is queued is the bridge's, as in the framework. */
function domStream(bridge: {
  read(): Promise<{ done: boolean; value?: unknown }>
  cancel(reason?: unknown): Promise<void>
}) {
  return new ReadableStream<Uint8Array>(
    {
      async pull(controller) {
        const { done, value } = await bridge.read()
        if (done) controller.close()
        else if (value instanceof Uint8Array) controller.enqueue(value)
        else controller.error(new TypeError('the bridge gave a chunk that is not bytes'))
      },
      cancel: (reason) => bridge.cancel(reason),
    },
    { highWaterMark: 0 },
  )
}
