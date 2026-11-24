export { getTelefuncChannelOptions }

import { defineHooks, type Hooks, type Message, type Peer } from 'crossws'
import { enableChannelTransports } from '../../../node/server/serverConfig.js'
import { handleError } from '../../../node/server/runTelefunc/handleError.js'
import { assert } from '../../../utils/assert.js'
import { readRequestUrl } from '../../../utils/getUrlPathname.js'
import { CHANNEL_TRANSPORT, WS_CLOSE_PAGE_LEFT, WS_CLOSE_REFUSED, WS_CLOSE_WIRE_VERSION } from '../../constants.js'
import { wireEnd } from '../../refusal.js'
import { namesWireVersion } from '../../wire/requestKind.js'
import { getChannelMux } from './ChannelMux.js'
import type { ServerTransport } from './ChannelMux.js'
import { upgradeRefusal } from './upgradeOrigin.js'

/** crossws's options for Telefunc's channels. `bufferedAmount` reads the socket's: `undefined` on workerd, whose
 *  WebSocket has none. `wireRequest` is the request the upgrade hook got, which crossws's peer carries as `peer.request`
 *  on every runtime but Node. */
function getTelefuncChannelOptions({
  upgrade,
  send = (peer, frame) => peer.send(frame),
  terminate = (peer) => peer.terminate(),
  bufferedAmount = (peer) => peer.websocket.bufferedAmount,
  wireRequest = (peer) => peer.request,
  copiesOnSend,
}: {
  upgrade?: Hooks['upgrade']
  send?: (peer: Peer, frame: Uint8Array<ArrayBuffer>) => void
  terminate?: (peer: Peer) => void
  bufferedAmount?: (peer: Peer) => number | undefined
  wireRequest?: (peer: Peer) => Request | undefined
  copiesOnSend?: boolean
} = {}) {
  enableChannelTransports(['ws'])
  const mux = getChannelMux()
  const transport: ServerTransport<Peer> = {
    /** Persistent bidirectional WebSocket: no out-of-band POSTs to route, so no need for a
     *  connId-based reverse lookup. */
    getConnId: () => null,
    sendNow: send,
    type: CHANNEL_TRANSPORT.WS,
    wholeMessages: true,
    copiesOnSend,
    bufferedAmount,
    // A loss closes at once: a Durable Object peer's terminate() is a close handshake a vanished client never answers.
    // An end the server decided closes with its refusal as the reason, after what the socket already holds and ahead of
    // what the wire's sender holds back, which goes with the wire.
    close: (peer, { refusal }) => {
      if (refusal === null) terminate(peer)
      else peer.close(WS_CLOSE_REFUSED, wireEnd(refusal))
    },
  }
  /** Whether each handshake the upgrade hook let through names this server's wire version, by its request. */
  const sameVersion = new WeakMap<Request, boolean>()

  /** crossws calls a hook with no catch: on Node a hook's throw would end the process. A throw is printed as a call's
   *  error is, and `ends` the hook's peer where it isn't ending already: the mux's peer goes as a loss, through the
   *  mux's one close, so that it can't cut an end already going. The upgrade hook has no guard: neither
   *  `readableHost()` nor `upgradeRefusal()` throws, and the adapters' own `upgrade` builds the peer's context. */
  function guard<Args extends unknown[]>(
    hook: (peer: Peer, ...args: Args) => unknown,
    ends: ((peer: Peer) => void) | null,
  ) {
    const onError = (peer: Peer, err: unknown) => {
      handleError(err)
      ends?.(peer)
    }
    return (peer: Peer, ...args: Args): void => {
      try {
        const result = hook(peer, ...args)
        if (result instanceof Promise) result.catch((err: unknown) => onError(peer, err))
      } catch (err) {
        onError(peer, err)
      }
    }
  }

  const hooks = defineHooks({
    // The one place that reads a handshake: `open` acts on what it decided.
    upgrade: (request) => {
      const host = readableHost(request)
      if (host === null) return new Response(null, { status: 400 })
      const refusal = upgradeRefusal(request, host)
      if (refusal) return refusal
      sameVersion.set(request, namesWireVersion(request))
      return upgrade?.(request)
    },
    // A throw in `open` comes before the mux has the peer, where nothing else would close it.
    open: guard((peer) => {
      const request = wireRequest(peer)
      const named = request && sameVersion.get(request)
      assert(request && named !== undefined)
      // A page on another version reloads: none of its frames could be read. Told after the 101, as a close code: a page
      // learns nothing from a refused handshake.
      if (!named) return peer.close(WS_CLOSE_WIRE_VERSION, 'reload the page')
      mux.onWireOpen(peer, transport, request)
    }, terminate),
    message: guard(
      (peer, message: Message) => mux.onWireMessage(peer, message.uint8Array() as Uint8Array<ArrayBuffer>),
      (peer) => mux.endFault(peer, { scope: 'loss' }),
    ),
    // Where the server decided the close, the mux forgot the peer as it decided it, and finds nothing here.
    close: guard(
      (peer, details: Parameters<Hooks['close']>[1]) =>
        mux.onWireClosed(peer, details?.code === WS_CLOSE_PAGE_LEFT ? 'pageLeft' : 'transient'),
      null,
    ),
    error: guard((peer) => mux.onWireClosed(peer, 'transient'), null),
  })
  return {
    hooks,
    // One namespace for every wire. crossws would take it from `new URL(request.url)`, which throws before the upgrade
    // hook runs on the path alone that Bun gives a request whose `Host` it can't read.
    getNamespace: () => 'telefunc',
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
