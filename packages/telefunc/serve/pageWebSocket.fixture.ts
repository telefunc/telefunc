export { NativeWebSocket, RecordedWebSocket, closedWithin, failedWebSockets, movedToWebSocket, webSockets }

import type { ClientChannel } from '../wire-protocol/channel/client/ClientChannel.js'
import { decode, isOneFrame, messageFrames } from '../wire-protocol/wire/frame.js'
import { TAG } from '../wire-protocol/wire/tags.js'

// A spec's page in Node: the WebSockets it opens, and the acknowledgements each of them brings.

const NativeWebSocket = globalThis.WebSocket
/** The page's WebSockets, by the host each went to, and those that failed (Node 22's fire `error` and no `close`). */
const webSockets = new Map<string, WebSocket[]>()
const failedWebSockets = new WeakSet<WebSocket>()
/** The text of each acknowledgement a WebSocket brought, as its server sent it. */
const acknowledgements = new WeakMap<WebSocket, string[]>()

/** The page's `WebSocket`, which a spec puts in place of Node's for its pages. */
class RecordedWebSocket extends NativeWebSocket {
  constructor(url: string | URL, protocols?: string | string[]) {
    super(url, protocols)
    const host = new URL(url).host
    webSockets.set(host, [...(webSockets.get(host) ?? []), this])
    this.addEventListener('error', () => failedWebSockets.add(this))
    this.addEventListener('message', (event) => {
      if (!(event.data instanceof ArrayBuffer)) return
      const raw = new Uint8Array(event.data)
      for (const bytes of isOneFrame(raw) ? [raw] : messageFrames(raw)) {
        const frame = decode(bytes)
        if (frame.tag === TAG.ACK_RES) acknowledgements.set(this, [...(acknowledgements.get(this) ?? []), frame.text])
      }
    })
  }
}

/** Whether the page moved to one of `sockets()`, its own WebSockets, within `ms`: one is open, and the acknowledgement
 *  of a send its server answers comes over it. False once each of them ended without that, or once `ms` passed, also
 *  while a send waits for its answer. The page's server answers each send with `ack:` and the send. */
async function movedToWebSocket(
  page: ClientChannel<string, string>,
  sockets: () => WebSocket[],
  ms: number,
): Promise<boolean> {
  const deadline = Date.now() + ms
  for (;;) {
    const open = sockets().filter((ws) => ws.readyState === NativeWebSocket.OPEN)
    if (open.length > 0) {
      const sent = `moved:${crypto.randomUUID()}`
      const acknowledged = (await answerWithin(deadline - Date.now(), page.send(sent, { ack: true }))) === `ack:${sent}`
      if (acknowledged && open.some((ws) => acknowledgements.get(ws)?.some((text) => text.includes(sent)))) return true
    }
    const ended =
      sockets().length > 0 &&
      sockets().every((ws) => ws.readyState === NativeWebSocket.CLOSED || failedWebSockets.has(ws))
    if (ended || Date.now() >= deadline) return false
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

/** Whether each of `sockets` closed within `ms`. A page's socket closed by the page is closed once its server read the
 *  page's close: the server knows the page left. */
async function closedWithin(sockets: WebSocket[], ms: number): Promise<boolean> {
  const deadline = Date.now() + ms
  for (;;) {
    if (sockets.every((ws) => ws.readyState === NativeWebSocket.CLOSED || failedWebSockets.has(ws))) return true
    if (Date.now() >= deadline) return false
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

/** What `answer` settles to within `ms`, or undefined. */
async function answerWithin<T>(ms: number, answer: Promise<T>): Promise<T | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const late = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => resolve(undefined), Math.max(ms, 0))
  })
  try {
    return await Promise.race([answer, late])
  } finally {
    clearTimeout(timer)
  }
}
