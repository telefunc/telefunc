export { installWebSocketOn, takeUpgradesOfNewServer }
export type { HttpServer }

import crossws from 'crossws/adapters/node'
import { type TakenServer, telefuncsUpgradesOn, upgradeServers } from '../node/server/runTelefunc/takesUpgrades.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import {
  isServerProductionEntryPending,
  preloadServerProductionEntry,
} from '../node/server/runTelefunc/loadTelefuncFilesUsingVite.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { readRequestUrl } from '../utils/getUrlPathname.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import type { IncomingMessage, Server } from 'node:http'
import type { Http2SecureServer } from 'node:http2'
import type { Duplex } from 'node:stream'
import type { Peer } from 'crossws'
import { assert } from '../utils/assert.js'
import { markRequestsOfServer } from './requestServer.js'
import { putBackOwnCallback, upgradeOthersOnlyForTheirListeners } from '../workarounds/nodeShouldUpgradeCallback.js'
import { carryUpgradeRequest } from '../workarounds/crosswsNodeRequest.js'

declare module 'crossws' {
  interface PeerContext {
    socket?: Duplex
  }
}

type HttpServer = Server | Http2SecureServer

/** What Telefunc reads of a request with `Upgrade`, once: its URL, where it can be read, and the config. */
type Handshake = { url: URL | null; config: ReturnType<typeof getServerConfig> }

function readHandshake(req: IncomingMessage): Handshake {
  return { url: readRequestUrl(req.url ?? ''), config: getServerConfig() }
}

const globalObject = getGlobalObject<{
  /** What Telefunc's `shouldUpgradeCallback` read of a request, for its `upgrade` listener. */
  handshakes: WeakMap<IncomingMessage, Handshake>
  /** Each upgrade's socket, by the request its upgrade hook gets. */
  sockets: WeakMap<Request, Duplex>
  /** Made with the first upgrade listener, so an app that takes no upgrades makes no adapter. */
  ws: ReturnType<typeof crossws> | null
}>('serve/nodeUpgrades.ts', {
  handshakes: new WeakMap(),
  sockets: new WeakMap(),
  ws: null,
})

/** Made outside `takeUpgrades()`, whose scope holds a server: the process keeps the adapter and its hooks. */
function webSocketAdapter(): ReturnType<typeof crossws> {
  globalObject.ws ??= crossws(
    getTelefuncChannelOptions({
      ...carryUpgradeRequest((request) => ({ socket: globalObject.sockets.get(request) })),
      send: sendInTaskWrite,
    }),
  )
  return globalObject.ws
}

/** Whether Telefunc upgrades `req`: a WebSocket handshake at its `telefuncUrl`, read once for the `upgrade` listener.
 *  Until the build's entry loaded, its `telefuncUrl` isn't known: Telefunc takes every WebSocket handshake, and the
 *  `upgrade` listener judges it after the load. */
function upgradesItself(req: IncomingMessage): boolean {
  if (req.headers.upgrade?.toLowerCase() !== 'websocket') return false
  if (isServerProductionEntryPending()) return true
  const handshake = readHandshake(req)
  globalObject.handshakes.set(req, handshake)
  return handshake.url?.pathname === handshake.config.telefuncUrl
}

/** `serve({ req, res })`'s take, of a server Telefunc didn't take before: one whose listener the app removed, or
 *  whose callback it replaced, stays as the app left it, until the app's own `installWebSocket()`. */
function takeUpgradesOfNewServer(httpServer: HttpServer): void {
  if (upgradeServers.has(httpServer)) return
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
    if (on === 'listener') {
      const inFront = upgradeOthersOnlyForTheirListeners(httpServer, before.listener, upgradesItself)
      if (inFront !== null) before.wrapped = inFront
    }
    return
  }
  // What a removal Telefunc didn't see left: its callback, still in front of the server's own.
  if (before !== undefined) putBackOwnCallback(httpServer, before.wrapped)
  const adapter = webSocketAdapter()

  const onUpgrade = async (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    await preloadServerProductionEntry()
    const { url, config } = globalObject.handshakes.get(req) ?? readHandshake(req)
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
    globalObject.sockets.set(request, socket)
    adapter.handleUpgrade(req, socket, head, request)
  }
  const taken: TakenServer = { server: httpServer, listener: onUpgrade, wrapped: null }
  // The callback judges a handshake by the build's `telefuncUrl`: its load starts now, before the server's first request.
  void preloadServerProductionEntry()
  httpServer.on('upgrade', onUpgrade)
  taken.wrapped = upgradeOthersOnlyForTheirListeners(httpServer, onUpgrade, upgradesItself)
  upgradeServers.set(httpServer, taken)
  // The removal gives the server its own callback back; the next install covers one unseen.
  const onRemoved = (event: string | symbol, removed: unknown) => {
    if (event !== 'upgrade' || removed !== onUpgrade) return
    httpServer.off('removeListener', onRemoved)
    putBackOwnCallback(httpServer, taken.wrapped)
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
