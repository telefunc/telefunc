export { takeUpgrades }
export type { HttpServer }

import crossws from 'crossws/adapters/node'
import { upgradeServers } from '../node/server/runTelefunc/takesUpgrades.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { readRequestUrl } from '../utils/getUrlPathname.js'
import { hasProp } from '../utils/hasProp.js'
import type { IncomingMessage, Server } from 'node:http'
import type { Http2SecureServer } from 'node:http2'
import type { Duplex } from 'node:stream'
import type { Peer } from 'crossws'

declare module 'crossws' {
  interface PeerContext {
    socket?: Duplex
    /** The request the upgrade hook got: crossws's Node peer carries another, which it builds from `Host`. */
    request?: Request
  }
}

type HttpServer = Server | Http2SecureServer

// TODO: remove once Telefunc's own WebSocket adapter replaces crossws.
// Each upgrade's socket, by the request its upgrade hook gets.
const sockets = new WeakMap<Request, Duplex>()
// Made with the first upgrade listener: the hooks enable the WebSocket transport, which an app that serves only
// `serve({ request })` doesn't have.
let ws: ReturnType<typeof crossws> | undefined

/** What Telefunc reads of a request with `Upgrade`, once: its URL, where it can be read, and the config. */
type Handshake = { url: URL | null; config: ReturnType<typeof getServerConfig> }

function readHandshake(req: IncomingMessage): Handshake {
  return { url: readRequestUrl(req.url ?? ''), config: getServerConfig() }
}

/** Where the server reads `shouldUpgradeCallback` (Node 22.21, 24.9 and later), a request with `Upgrade` that isn't
 *  Telefunc's is upgraded only where another `upgrade` listener is there to take it and the server's own callback (the
 *  app's, or Node's default) accepts it; any other goes to the app's `request` listeners, as on the same server without
 *  Telefunc's WebSocket. What the callback reads of a request is kept in `handshakes`. Returns what puts the server's
 *  own callback back. */
function upgradeOthersOnlyForTheirListeners(
  server: HttpServer,
  handshakes: WeakMap<IncomingMessage, Handshake>,
): () => void {
  if (!hasProp(server, 'shouldUpgradeCallback', 'function')) return () => {}
  const own = server.shouldUpgradeCallback
  Reflect.set(server, 'shouldUpgradeCallback', (req: IncomingMessage) => {
    const handshake = readHandshake(req)
    handshakes.set(req, handshake)
    if (handshake.url?.pathname === handshake.config.telefuncUrl) return true
    return server.listenerCount('upgrade') > 1 && Boolean(own.call(server, req))
  })
  return () => Reflect.set(server, 'shouldUpgradeCallback', own)
}

/** Telefunc's `upgrade` listener on `httpServer`, once: its pages are offered WebSocket while the listener is there. A
 *  page's server is read off its request's socket, as `serve({ req, res })` and Vite dev's middleware do, or off the
 *  store an app's own `installWebSocket()` adds (serve/node.ts). */
function takeUpgrades(httpServer: HttpServer): void {
  if (upgradeServers.has(httpServer)) return
  upgradeServers.add(httpServer)
  const adapter = (ws ??= crossws(
    getTelefuncChannelOptions({
      upgrade: (request) => ({ context: { socket: sockets.get(request), request } }),
      send: sendInTaskWrite,
      wireRequest: (peer) => peer.context.request,
    }),
  ))

  // Read by the server's `shouldUpgradeCallback` where it has one; on a Node without it, by `onUpgrade`.
  const handshakes = new WeakMap<IncomingMessage, Handshake>()
  const onUpgrade = (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const { url, config } = handshakes.get(req) ?? readHandshake(req)
    if (url?.pathname !== config.telefuncUrl) {
      // Here only on a Node that doesn't read `shouldUpgradeCallback` (before 22.21 and 24.9), which hands every
      // request with `Upgrade` to the `upgrade` listeners: one that no other listener is there to take is answered as
      // a page the server doesn't have.
      if (httpServer.listenerCount('upgrade') === 1) {
        socket.once('finish', socket.destroy)
        socket.end('HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n')
      }
      return
    }
    if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
      socket.once('finish', socket.destroy)
      socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n')
      return
    }
    // The upgrade hook reads the handshake's headers (upgradeOrigin.ts).
    const headers = new Headers()
    for (const [name, value] of Object.entries(req.headers))
      for (const one of [value ?? []].flat()) headers.append(name, one)
    const request = new Request(url, { headers })
    sockets.set(request, socket)
    adapter.handleUpgrade(req, socket, head, request)
  }
  httpServer.on('upgrade', onUpgrade)
  const putBackCallback = upgradeOthersOnlyForTheirListeners(httpServer, handshakes)
  // Its pages are offered WebSocket while Telefunc's listener is there.
  const onRemoved = (event: string | symbol, removed: unknown) => {
    if (event !== 'upgrade' || removed !== onUpgrade) return
    httpServer.off('removeListener', onRemoved)
    putBackCallback()
    upgradeServers.delete(httpServer)
  }
  httpServer.on('removeListener', onRemoved)
}

/** Sends `frame` in one write with the task's other frames, up to the socket's mark: written alone, each frame costs a
 *  syscall while the socket has room. Past the mark it goes at once, or the page would wait for the whole task. */
function sendInTaskWrite(peer: Peer, frame: Uint8Array<ArrayBuffer>): void {
  const socket = peer.context.socket!
  if (!socket.writableCorked) {
    socket.cork()
    process.nextTick(() => socket.uncork())
  }
  peer.send(frame)
  if (socket.writableLength >= socket.writableHighWaterMark) socket.uncork()
}
