export { installWebSocketOn, takeUpgradesOfNewServer }
export type { HttpServer }

import crossws from 'crossws/adapters/node'
import { type TakenServer, telefuncsUpgradesOn, upgradeServers } from '../node/server/runTelefunc/takesUpgrades.js'
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
import { assert } from '../utils/assert.js'
import { markRequestsOfServer } from './requestServer.js'

declare module 'crossws' {
  interface PeerContext {
    socket?: Duplex
    /** The request the upgrade hook got: crossws's Node peer carries another, which it builds from `Host`. */
    request?: Request
  }
}

type HttpServer = Server | Http2SecureServer

// 0.3.1 reads this registry, key and shape before it adds its `upgrade` listener: two would take one socket.
const { registeredServers } = getGlobalObject('serve/node.ts', { registeredServers: new WeakSet<object>() })

// Each upgrade's socket, by the request its upgrade hook gets.
const sockets = new WeakMap<Request, Duplex>()
// Made with the first upgrade listener, so an app that takes no upgrades makes no adapter.
let ws: ReturnType<typeof crossws> | undefined

/** Made outside `takeUpgrades()`, whose scope holds a server: the process keeps the adapter and its hooks. */
function webSocketAdapter(): ReturnType<typeof crossws> {
  ws ??= crossws(
    getTelefuncChannelOptions({
      upgrade: (request) => ({ context: { socket: sockets.get(request), request } }),
      send: sendInTaskWrite,
      wireRequest: (peer) => peer.context.request,
    }),
  )
  return ws
}

/** What Telefunc reads of a request with `Upgrade`, once: its URL, where it can be read, and the config. */
type Handshake = { url: URL | null; config: ReturnType<typeof getServerConfig> }

function readHandshake(req: IncomingMessage): Handshake {
  return { url: readRequestUrl(req.url ?? ''), config: getServerConfig() }
}

const { handshakes } = getGlobalObject('serve/nodeUpgrades.ts', {
  /** What Telefunc's `shouldUpgradeCallback` read of a request, for its `upgrade` listener. */
  handshakes: new WeakMap<IncomingMessage, Handshake>(),
})

/** Telefunc's `shouldUpgradeCallback` (Node 22.21, 24.9 and later) upgrades its handshake and leaves every other
 *  request to the server's own, which sees a `listenerCount` without Telefunc's listener. */
function upgradeOthersOnlyForTheirListeners(server: HttpServer, taken: TakenServer): void {
  if (!hasProp(server, 'shouldUpgradeCallback', 'function')) return
  const own = server.shouldUpgradeCallback
  const callback = (req: IncomingMessage) => {
    const handshake = readHandshake(req)
    handshakes.set(req, handshake)
    if (handshake.url?.pathname === handshake.config.telefuncUrl) return true
    const before = Object.getOwnPropertyDescriptor(server, 'listenerCount')
    const count = server.listenerCount
    const withoutTelefunc = (...args: [event: string | symbol, listener?: (...args: never[]) => void]) => {
      const [event, listener] = args
      if (event !== 'upgrade' || (listener !== undefined && listener !== taken.listener))
        return count.call(server, ...args)
      if (listener === taken.listener) return 0
      return count.call(server, 'upgrade') - count.call(server, 'upgrade', taken.listener)
    }
    Object.defineProperty(server, 'listenerCount', { value: withoutTelefunc, configurable: true, writable: true })
    try {
      return Boolean(own.call(server, req))
    } finally {
      if (before === undefined) Reflect.deleteProperty(server, 'listenerCount')
      else Object.defineProperty(server, 'listenerCount', before)
    }
  }
  server.shouldUpgradeCallback = callback
  taken.callback = callback
  taken.own = own
}

/** Puts the server's own callback back where Telefunc's is still the server's: one the app set since stays. */
function putBackOwnCallback(server: HttpServer, taken: TakenServer): void {
  if (taken.own === undefined || !hasProp(server, 'shouldUpgradeCallback', 'function')) return
  if (server.shouldUpgradeCallback === taken.callback) server.shouldUpgradeCallback = taken.own
}

/** `serve({ req, res })`'s take, of a server no copy of Telefunc took before: one whose listener the app removed, or
 *  whose callback it replaced, stays as the app left it, until the app's own `installWebSocket()`. */
function takeUpgradesOfNewServer(httpServer: HttpServer): void {
  if (upgradeServers.has(httpServer) || registeredServers.has(httpServer)) return
  takeUpgrades(httpServer)
}

/** The app's `installWebSocket()`: Telefunc takes the server's upgrades, and marks its requests for
 *  `serve({ request })`, which can't read the server off a `Request`'s socket as the other forms do. */
function installWebSocketOn(httpServer: HttpServer): void {
  takeUpgrades(httpServer)
  markRequestsOfServer(httpServer)
}

/** Telefunc's `upgrade` listener on `httpServer`, once: its pages are offered WebSocket while the listener is there. */
function takeUpgrades(httpServer: HttpServer): void {
  const before = upgradeServers.get(httpServer)
  const on = telefuncsUpgradesOn(httpServer)
  if (before !== undefined && on !== 'nothing') {
    // The app put its own `shouldUpgradeCallback` in place of Telefunc's: Telefunc's goes in front of it again.
    if (on === 'listener') upgradeOthersOnlyForTheirListeners(httpServer, before)
    return
  }
  // 0.3.1's listener is there.
  if (registeredServers.has(httpServer) && !before?.holdsRegistry) return
  // What a removal Telefunc didn't see left: its callback, still in front of the server's own.
  if (before !== undefined) putBackOwnCallback(httpServer, before)
  const adapter = webSocketAdapter()

  const onUpgrade = (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const { url, config } = handshakes.get(req) ?? readHandshake(req)
    if (url?.pathname !== config.telefuncUrl) {
      // Upgraded by a Node before 22.21 and 24.9, or by the server's own callback: with no other listener, a 404.
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
  const taken: TakenServer = {
    server: httpServer,
    listener: onUpgrade,
    callback: undefined,
    own: undefined,
    holdsRegistry: true,
  }
  registeredServers.add(httpServer)
  httpServer.on('upgrade', onUpgrade)
  upgradeOthersOnlyForTheirListeners(httpServer, taken)
  upgradeServers.set(httpServer, taken)
  // The removal gives the server its own callback back and frees 0.3.1's registry; the next install covers one unseen.
  const onRemoved = (event: string | symbol, removed: unknown) => {
    if (event !== 'upgrade' || removed !== onUpgrade) return
    httpServer.off('removeListener', onRemoved)
    putBackOwnCallback(httpServer, taken)
    taken.holdsRegistry = false
    // An install from a `removeListener` listener that ran before this one took the server again: the registry is its.
    if (upgradeServers.get(httpServer) === taken) registeredServers.delete(httpServer)
  }
  httpServer.on('removeListener', onRemoved)
}

/** Sends `frame` in one write with the task's other frames, up to the socket's mark: written alone, each frame costs a
 *  syscall while the socket has room. Past the mark it goes at once, or the page would wait for the whole task. */
function sendInTaskWrite(peer: Peer, frame: Uint8Array<ArrayBuffer>): void {
  const { socket } = peer.context
  assert(socket)
  if (!socket.writableCorked) {
    socket.cork()
    process.nextTick(() => socket.uncork())
  }
  peer.send(frame)
  if (socket.writableLength >= socket.writableHighWaterMark) socket.uncork()
}
