export { takeUpgrades, takeUpgradesOfNewServer }
export type { HttpServer }

import crossws from 'crossws/adapters/node'
import { upgradeServers } from '../node/server/runTelefunc/takesUpgrades.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { readRequestUrl } from '../utils/getUrlPathname.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
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

// 0.3.1 checks this registry, under this key and in this shape, before it adds its own `upgrade` listener: a server in
// it has one copy's listener already, and two would each take the same socket.
const { registeredServers } = getGlobalObject('serve/node.ts', { registeredServers: new WeakSet<object>() })

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

const { seenServers, handshakes, ownCallbacks } = getGlobalObject('serve/nodeUpgrades.ts', {
  /** The servers `serve({ req, res })` has taken, or found taken, once. */
  seenServers: new WeakSet<HttpServer>(),
  /** What Telefunc's `shouldUpgradeCallback` read of a request, for its `upgrade` listener. */
  handshakes: new WeakMap<IncomingMessage, Handshake>(),
  /** The server's own callback, by the callback of Telefunc's that stands in front of it. */
  ownCallbacks: new WeakMap<object, (req: IncomingMessage) => unknown>(),
})

/** Where the server reads `shouldUpgradeCallback` (Node 22.21, 24.9 and later), a request with `Upgrade` that isn't
 *  Telefunc's is upgraded only where another `upgrade` listener is there to take it and the server's own callback (the
 *  app's, or Node's default) accepts it; any other goes to the app's `request` listeners, as on the same server without
 *  Telefunc's WebSocket. Returns Telefunc's callback, put in front of the server's current one. */
function upgradeOthersOnlyForTheirListeners(server: HttpServer): unknown {
  if (!hasProp(server, 'shouldUpgradeCallback', 'function')) return undefined
  const own = server.shouldUpgradeCallback
  const callback = (req: IncomingMessage) => {
    const handshake = readHandshake(req)
    handshakes.set(req, handshake)
    if (handshake.url?.pathname === handshake.config.telefuncUrl) return true
    return server.listenerCount('upgrade') > 1 && Boolean(own.call(server, req))
  }
  server.shouldUpgradeCallback = callback
  ownCallbacks.set(callback, own)
  return callback
}

/** Puts the server's own callback back where Telefunc's is still the server's: one the app set since stays. */
function putBackOwnCallback(server: HttpServer, callback: unknown): void {
  if (!hasProp(server, 'shouldUpgradeCallback', 'function') || server.shouldUpgradeCallback !== callback) return
  const own = ownCallbacks.get(server.shouldUpgradeCallback)
  if (own !== undefined) server.shouldUpgradeCallback = own
}

/** `serve({ req, res })`'s take, of a server it hasn't seen before: one whose listener the app removed stays as the app
 *  left it, until the app's own `installWebSocket()`. */
function takeUpgradesOfNewServer(httpServer: HttpServer): void {
  if (seenServers.has(httpServer)) return
  seenServers.add(httpServer)
  takeUpgrades(httpServer)
}

/** Telefunc's `upgrade` listener on `httpServer`, once: its pages are offered WebSocket while the listener is there. A
 *  page's server is read off its request's socket, as `serve({ req, res })` and Vite dev's middleware do, or, for
 *  `serve({ request })`, off the header an app's own `installWebSocket()` adds (serve/requestServer.ts). */
function takeUpgrades(httpServer: HttpServer): void {
  const taken = upgradeServers.get(httpServer)
  if (taken !== undefined) {
    // The app put its own `shouldUpgradeCallback` in place of Telefunc's: Telefunc's goes in front of it again.
    const replaced =
      hasProp(httpServer, 'shouldUpgradeCallback', 'function') && httpServer.shouldUpgradeCallback !== taken.callback
    if (taken.callback !== undefined && replaced) taken.callback = upgradeOthersOnlyForTheirListeners(httpServer)
    return
  }
  if (registeredServers.has(httpServer)) return
  registeredServers.add(httpServer)
  const adapter = (ws ??= crossws(
    getTelefuncChannelOptions({
      upgrade: (request) => ({ context: { socket: sockets.get(request), request } }),
      send: sendInTaskWrite,
      wireRequest: (peer) => peer.context.request,
    }),
  ))

  const onUpgrade = (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const { url, config } = handshakes.get(req) ?? readHandshake(req)
    if (url?.pathname !== config.telefuncUrl) {
      // Here on a Node that doesn't read `shouldUpgradeCallback` (before 22.21 and 24.9), which hands every request
      // with `Upgrade` to the `upgrade` listeners, or where the app's own callback is in place of Telefunc's: one that
      // no other listener is there to take is answered as a page the server doesn't have.
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
  const callback = upgradeOthersOnlyForTheirListeners(httpServer)
  upgradeServers.set(httpServer, { server: httpServer, listener: onUpgrade, callback })
  // Frees the server for the next take, of this copy or of another.
  const onRemoved = (event: string | symbol, removed: unknown) => {
    if (event !== 'upgrade' || removed !== onUpgrade) return
    httpServer.off('removeListener', onRemoved)
    putBackOwnCallback(httpServer, upgradeServers.get(httpServer)?.callback)
    upgradeServers.delete(httpServer)
    registeredServers.delete(httpServer)
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
