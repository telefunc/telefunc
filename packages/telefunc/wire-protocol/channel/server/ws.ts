export { getTelefuncChannelOptions }

import { defineHooks, type Hooks, type Message, type Peer } from 'crossws'
import { handleError } from '../../../node/server/runTelefunc/handleError.js'
import { enableWebSocket } from '../../../node/server/serverConfig.js'
import { readRequestUrl } from '../../../utils/getUrlPathname.js'
import { CHANNEL_TRANSPORT, WS_CLOSE_PAGE_LEFT, WS_CLOSE_REFUSED, WS_CLOSE_WIRE_VERSION } from '../../constants.js'
import { wireEnd } from '../../refusal.js'
import { namesWireVersion } from '../../wire/requestKind.js'
import { getChannelMux } from './ChannelMux.js'
import type { ServerTransport } from './ChannelMux.js'
import { upgradeRefusal } from './upgradeOrigin.js'
import { guardCrosswsHook, getCrosswsNamespace } from '../../../workarounds/crosswsHooks.js'

/** crossws's options for Telefunc's channels. `bufferedAmount` reads the socket's, which crossws reads as 0 where the
 *  socket has none: workerd's, whose adapter passes `() => undefined`. */
function getTelefuncChannelOptions({
  upgrade,
  send = (peer, frame) => peer.send(frame),
  terminate = (peer) => peer.terminate(),
  bufferedAmount = (peer) => peer.bufferedAmount,
  copiesOnSend,
}: {
  upgrade?: Hooks['upgrade']
  send?: (peer: Peer, frame: Uint8Array<ArrayBuffer>) => void
  terminate?: (peer: Peer) => void
  bufferedAmount?: (peer: Peer) => number | undefined
  copiesOnSend?: boolean
} = {}) {
  const mux = getChannelMux()
  enableWebSocket()
  const transport: ServerTransport<Peer> = {
    /** Persistent bidirectional WebSocket: no out-of-band POSTs to route, so no need for a
     *  connId-based reverse lookup. */
    getConnId: () => null,
    sendNow: send,
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    copiesOnSend,
    bufferedAmount,
    // A loss is terminated: the mux detaches the wire as it decides the loss, while a Durable Object's or Deno's
    // terminate() only starts a close handshake, which a vanished client never answers.
    // An end the server decided closes with its refusal as the reason, after what the socket already holds and ahead of
    // what the wire's sender holds back, which goes with the wire.
    close: (peer, { refusal }) => {
      if (refusal === null) terminate(peer)
      else peer.close(WS_CLOSE_REFUSED, wireEnd(refusal))
    },
  }
  // A hook's throw `ends` its peer where it isn't ending already: the mux's peer goes as a loss, through the mux's one
  // close, so that it can't cut an end already going. The upgrade hook has no guard: neither `readableHost()` nor
  // `upgradeRefusal()` throws, and the adapters' own `upgrade` builds the peer's context.
  const hooks = defineHooks<Omit<Hooks, 'ping' | 'pong'>>({
    upgrade: (request) => {
      const host = readableHost(request)
      if (host === null) return new Response(null, { status: 400 })
      const refusal = upgradeRefusal(request, host)
      if (refusal) return refusal
      return upgrade?.(request)
    },
    // A throw in `open` comes before the mux has the peer, where nothing else would close it.
    open: guardCrosswsHook(
      (peer) => {
        const request = peer.request
        // A page on another version reloads: none of its frames could be read. Told after the 101, as a close code: a page
        // learns nothing from a refused handshake.
        if (!namesWireVersion(request)) return peer.close(WS_CLOSE_WIRE_VERSION, 'reload the page')
        mux.onWireOpen(peer, transport, request)
      },
      terminate,
      handleError,
    ),
    message: guardCrosswsHook(
      (peer, message: Message) => mux.onWireMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>),
      (peer) => mux.endFault(peer, { scope: 'loss' }),
      handleError,
    ),
    // Where the server decided the close, the mux forgot the peer as it decided it, and finds nothing here.
    close: guardCrosswsHook(
      (peer, details: Parameters<Hooks['close']>[1]) =>
        mux.onWireClosed(peer, details?.code === WS_CLOSE_PAGE_LEFT ? 'pageLeft' : 'transient'),
      null,
      handleError,
    ),
    error: guardCrosswsHook((peer) => mux.onWireClosed(peer, 'transient'), null, handleError),
    drain: guardCrosswsHook((peer) => mux.onWireDrain(peer), null, handleError),
  })
  return {
    hooks,
    getNamespace: getCrosswsNamespace,
    // crossws pings no wire: Telefunc's PING, every `config.channel.pingInterval`, tells a wire is alive on all of them.
    idleTimeout: 0,
  }
}

/** The `Host` of a handshake Telefunc can read: its URL is one, and its `Host` names an authority (a host, and a port
 *  where it has one) and nothing else, as a WebSocket handshake's must (RFC 6455, 4.2.1). Null otherwise, which is
 *  answered 400 (4.2.2). Never throws. */
function readableHost(request: Request): string | null {
  const host = request.headers.get('host')
  const readable =
    readRequestUrl(request.url) !== null && host !== null && !/[/\\?#@]/.test(host) && URL.canParse(`http://${host}`)
  return readable ? host : null
}
