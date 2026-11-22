// A spec's page, in a realm of its own: the package as built, its own `fetch`, `WebSocket`, `pagehide` and session, so
// that nothing of it outlives the worker that `page.fixture.ts` ends as a closed tab ends its page.

import { parentPort, workerData } from 'node:worker_threads'
import { ClientChannel } from '../dist/wire-protocol/channel/client/ClientChannel.js'
import { decode, isOneFrame, messageFrames } from '../dist/wire-protocol/wire/frame.js'
import { TAG } from '../dist/wire-protocol/wire/tags.js'
import { base64urlToUint8Array } from '../dist/wire-protocol/wire/base64url.js'

/** `marker`: the page's own `x-telefunc-server` header on each of its requests; `idleTimeout`: its connection's, null for
 *  its server's. */
const { telefuncUrl, channelId, marker, idleTimeout } = workerData
const post = (message) => parentPort.postMessage(message)

// The page's `pagehide`, where its transports listen for it: a WebSocket's on the global object, an SSE wire's on a
// `window` that has the page's `location` and no `document`, so that nothing takes the realm for a browser's.
const pageEvents = new EventTarget()
const pageHideListenerSet = new Set()
const pageHideListeners = {
  addEventListener(type, listener) {
    if (type === 'pagehide') pageHideListenerSet.add(listener)
    pageEvents.addEventListener(type, listener)
  },
  removeEventListener(type, listener) {
    if (type === 'pagehide') pageHideListenerSet.delete(listener)
    pageEvents.removeEventListener(type, listener)
  },
}
/** Each request the page started and each WebSocket it opened, with when (`Date.now()`). */
const attempts = []
Object.assign(globalThis, pageHideListeners, {
  window: { ...pageHideListeners, location: { href: 'http://127.0.0.1/' } },
})

// The page's requests: its marker on each; the transports each RECONCILED its SSE streams carry; what its `keepalive`
// goodbyes got; and the tab's unload, which ends each request sent without `keepalive`, its stream too.
const nativeFetch = globalThis.fetch
const unloading = new AbortController()
let goodbyesUnanswered = 0
globalThis.fetch = async (input, init = {}) => {
  attempts.push({ at: Date.now(), what: `${init.method ?? 'GET'}${init.keepalive ? ' keepalive' : ''}` })
  const headers = new Headers(init.headers)
  if (marker !== undefined) headers.append('x-telefunc-server', marker)
  if (init.keepalive) {
    goodbyesUnanswered++
    try {
      const response = await nativeFetch(input, { ...init, headers })
      post({ kind: 'goodbye', what: `answered ${response.status}` })
      return response
    } catch (err) {
      const code = err?.cause?.code
      post({ kind: 'goodbye', what: typeof code === 'string' ? `refused: ${code}` : `failed: ${String(err)}` })
      throw err
    } finally {
      goodbyesUnanswered--
    }
  }
  const signal = init.signal ? AbortSignal.any([init.signal, unloading.signal]) : unloading.signal
  const response = await nativeFetch(input, { ...init, headers, signal })
  if (response.headers.get('content-type') !== 'text/event-stream' || !response.body) return response
  const [toPage, toOffers] = response.body.tee()
  void toOffers.pipeTo(new WritableStream({ write: offerRecorder() })).catch(() => {})
  return new Response(toPage, response)
}

function offerRecorder() {
  const decoder = new TextDecoder()
  let events = ''
  return (chunk) => {
    events += decoder.decode(chunk, { stream: true })
    const complete = events.split('\n\n')
    events = complete.pop() ?? ''
    for (const event of complete) {
      if (!event.startsWith('data: ')) continue
      for (const frame of framesOf(base64urlToUint8Array(event.slice('data: '.length))))
        if (frame.tag === TAG.RECONCILED) post({ kind: 'offer', transports: frame.payload.transports })
    }
  }
}

function framesOf(raw) {
  return (isOneFrame(raw) ? [raw] : [...messageFrames(raw)]).map((bytes) => decode(bytes))
}

// The page's WebSockets: what opened each, how it ended (Node 22's fire `error` and no `close` where the connection is
// refused), and the text of each acknowledgement it brought.
const NativeWebSocket = globalThis.WebSocket
const sockets = []
globalThis.WebSocket = class extends NativeWebSocket {
  constructor(url, protocols) {
    super(url, protocols)
    attempts.push({ at: Date.now(), what: 'WebSocket' })
    const stack = new Error().stack ?? ''
    const opener = /\bWsTransport\.probe\b/.test(stack)
      ? 'WsTransport.probe()'
      : /\bWsTransport\.start\b/.test(stack)
        ? 'WsTransport.start()'
        : 'elsewhere'
    const socket = { ws: this, opener, failed: false, acknowledgements: [] }
    sockets.push(socket)
    this.addEventListener('error', () => {
      socket.failed = true
    })
    this.addEventListener('message', (event) => {
      if (!(event.data instanceof ArrayBuffer)) return
      for (const frame of framesOf(new Uint8Array(event.data)))
        if (frame.tag === TAG.ACK_RES) socket.acknowledgements.push(frame.text)
    })
  }
}
const ended = ({ ws, failed }) => ws.readyState === NativeWebSocket.CLOSED || failed
const describe = ({ ws, opener, failed }) =>
  `${ws.url} from ${opener}, ${failed ? 'failed' : ['connecting', 'open', 'closing', 'closed'][ws.readyState]}`

const page = new ClientChannel({
  channelId,
  connection: { telefuncUrl, transports: ['sse', 'ws'], ...(idleTimeout === null ? {} : { idleTimeout }) },
})

const commands = {
  send: (data) => page.send(data, { ack: true }),
  /** The page closes its channel: the close's outcome, null where it closed gracefully. */
  close: async () => {
    const outcome = await page.close()
    return outcome === null ? null : `${outcome.name}: ${outcome.message}`
  },
  attempts: () => attempts,
  /** What the realm still holds open, by kind (`process.getActiveResourcesInfo()`), and its `pagehide` listeners. */
  resources: () => ({ open: process.getActiveResourcesInfo(), pageHideListeners: pageHideListenerSet.size }),
  /** Whether the page moved to a WebSocket of its own within `ms`: one is open, and the acknowledgement of a send its
   *  server answers comes over it. False once each of them ended without that, or once `ms` passed, also while a send
   *  waits for its answer. */
  async moved(ms) {
    const deadline = Date.now() + ms
    for (;;) {
      const open = sockets.filter(({ ws }) => ws.readyState === NativeWebSocket.OPEN)
      if (open.length > 0) {
        const sent = `moved:${crypto.randomUUID()}`
        const acknowledged =
          (await answerWithin(deadline - Date.now(), page.send(sent, { ack: true }))) === `ack:${sent}`
        if (acknowledged && open.some(({ acknowledgements }) => acknowledgements.some((text) => text.includes(sent))))
          return true
      }
      if ((sockets.length > 0 && sockets.every(ended)) || Date.now() >= deadline) return false
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
  },
  sockets: () => sockets.map(describe),
  /** The page leaves, as a closed tab's page does: each of its transports reads a `pagehide` that keeps nothing. */
  leave: () => void pageEvents.dispatchEvent(Object.assign(new Event('pagehide'), { persisted: false })),
  unload: () => unloading.abort(),
  /** Whether, within `ms`, each of the page's WebSockets ended and each of its goodbyes got its answer. */
  async left(ms) {
    const deadline = Date.now() + ms
    while (!sockets.every(ended) || goodbyesUnanswered > 0) {
      if (Date.now() >= deadline) return false
      await new Promise((resolve) => setTimeout(resolve, 20))
    }
    return true
  },
}

/** What `answer` settles to within `ms`, or undefined. */
async function answerWithin(ms, answer) {
  let timer
  const late = new Promise((resolve) => {
    timer = setTimeout(() => resolve(undefined), Math.max(ms, 0))
  })
  try {
    return await Promise.race([answer, late])
  } finally {
    clearTimeout(timer)
  }
}

parentPort.on('message', async ({ id, command, arg }) => {
  try {
    post({ kind: 'reply', id, value: await commands[command](arg) })
  } catch (err) {
    post({ kind: 'reply', id, error: err instanceof Error ? `${err.name}: ${err.message}` : String(err) })
  }
})
post({ kind: 'ready' })
