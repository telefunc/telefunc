export { takeUpgrades, takeUpgradesOfNewServer }
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

/** Where the server reads `shouldUpgradeCallback` (Node 22.21, 24.9 and later), Telefunc's callback goes in front of the
 *  server's current one: it upgrades Telefunc's handshake, and the server's own callback (the app's, or Node's default)
 *  decides every other request with `Upgrade`, as on the same server without Telefunc's listener. So Node's default
 *  upgrades a request only where another `upgrade` listener is there to take it, and sends any other to the app's
 *  `request` listeners. Node offers no way to ask what a callback decides without one of the listeners, so while the
 *  server's own callback runs, on the server as Node calls it, the server's `listenerCount` leaves Telefunc's listener
 *  out; its `listeners('upgrade')` and `rawListeners('upgrade')` still show it. */
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

/** Telefunc's `upgrade` listener on `httpServer`, once: its pages are offered WebSocket while the listener is there. A
 *  page's server is read off its request's socket, as `serve({ req, res })` and Vite dev's middleware do, or, for
 *  `serve({ request })`, off the header an app's own `installWebSocket()` adds (serve/requestServer.ts). */
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
      // Here where Node upgraded a request that isn't Telefunc's: on a Node that doesn't read `shouldUpgradeCallback`
      // (before 22.21 and 24.9), which hands every request with `Upgrade` to the `upgrade` listeners, or where the
      // server's own callback upgraded it. One that no other listener is there to take is answered as a page the
      // server doesn't have.
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
  // What only the removal itself can do: give the server its own callback back at once, and free 0.3.1's registry for
  // 0.3.1's next install. A removal it doesn't see, the next `installWebSocket()` makes up for.
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
  const socket = peer.context.socket!
  if (!socket.writableCorked) {
    socket.cork()
    process.nextTick(() => socket.uncork())
  }
  peer.send(frame)
  if (socket.writableLength >= socket.writableHighWaterMark) socket.uncork()
}
