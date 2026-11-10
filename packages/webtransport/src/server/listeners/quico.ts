export { listen }

import quico, { type QuicoIncomingMessage, type QuicoServerResponse } from 'quico'
import { createSocket } from 'node:dgram'
import type { Writable } from 'node:stream'
import { MAX_CHANNELS_PER_CONNECTION } from 'telefunc/__internal'
import type { Listener, ListenOptions, SendStream, SessionHandlers } from '../listener.js'

/** Node and Bun, on QUICO: HTTP/3 only, so the app's own server keeps its TCP port. */
async function listen({ port, certificate, onSession }: ListenOptions): Promise<Listener> {
  // QUICO's `address()` answers with the TCP listener's port, and this server has none.
  const udpPort = port === 0 ? await freeUdpPort() : port
  const server = quico.createServer(
    {
      ...certificate,
      http1: false,
      http2: false,
      // The page opens a stream per channel it sends on.
      transportParams: { initialMaxStreamsUni: MAX_CHANNELS_PER_CONNECTION },
    },
    (req, res) => {
      if (req.headers[':protocol'] !== 'webtransport') {
        res.writeHead(404)
        res.end()
        return
      }
      res.writeHead(200)
      accept(req, res, onSession)
    },
  )
  server.unref()
  return new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(udpPort, () => {
      server.off('error', reject)
      resolve({
        port: udpPort,
        setCertificate: (next) => server.setSecureContext(next),
      })
    })
  })
}

function accept(req: QuicoIncomingMessage, res: QuicoServerResponse, onSession: ListenOptions['onSession']): void {
  let handlers: SessionHandlers | null = null
  const read = (stream: NodeJS.ReadableStream & object): void => {
    stream.on('data', (chunk: Buffer) => handlers?.onData(stream, chunk))
    stream.on('end', () => handlers?.onStreamEnd(stream))
    stream.on('error', () => {})
  }
  req.on('stream', (stream) => {
    // A session has one bidirectional stream, its control stream.
    if (handlers !== null) return void stream.destroy()
    handlers = onSession({
      control: sendStream(stream),
      openStream: ({ unsentOnly = false } = {}) =>
        sendStream(res.createUnidirectionalStream!({ waitUntilAvailable: true })!, unsentOnly),
      close: () => res.closeSession!(),
    })
    read(stream)
  })
  req.on('unidirectionalStream', (stream) => {
    if (handlers === null) return void stream.destroy()
    read(stream)
  })
  req.on('close', () => handlers?.onClose())
}

function sendStream(stream: Writable, unsentOnly = false): SendStream {
  stream.on('error', () => {})
  // QUICO calls a write back once its stream holds at most `writableHighWaterMark` unsent: at 0, once its bytes left.
  if (unsentOnly) Object.defineProperty(stream, 'writableHighWaterMark', { value: 0 })
  return {
    write: (bytes, onSent) => void stream.write(bytes, onSent),
    end: () => void stream.end(),
    get unsentBytes() {
      return stream.writableLength
    },
  }
}

/** A UDP port no socket holds now, as the OS picks one. */
async function freeUdpPort(): Promise<number> {
  const socket = createSocket('udp6')
  await new Promise<void>((resolve) => socket.bind(0, resolve))
  const { port } = socket.address()
  await new Promise<void>((resolve) => socket.close(resolve))
  return port
}
