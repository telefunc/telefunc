export { startBlackHoleProxy, PacedDirection }
export type { BlackHoleProxy }

import * as net from 'node:net'

type BlackHoleProxy = {
  /** The origin the page loads through the proxy. */
  url: string
  /** The page's WebSocket connections stop carrying anything either way and stay open, as a link that died without a word; returns how many. */
  blackHoleWebSockets(): number
  /** The page's next WebSocket gets the server's upgrade answer and nothing after it, while what the page sends still reaches the server, but its close doesn't. */
  muteNextWebSocket(): void
  /** Null until the server closes the muted WebSocket, then how many WebSockets the page opened after it meanwhile. */
  openedBeforeMutedClosed(): number | null
  /** From now on each direction carries `bytesPerSecond` (download, upload) through its own queue of `queueBytes`, as a slow link does, each byte `delayMs` late; a direction whose queue is full stops reading its senders. */
  pace(link: {
    downBytesPerSecond: number
    upBytesPerSecond: number
    queueBytes: number
    burstMs?: number
    delayMs?: number
  }): void
  close(): Promise<void>
}

type Link = {
  page: net.Socket
  server: net.Socket
  webSocket: boolean
  dead: boolean
  /** The page's bytes after its upgrade request, not yet a whole frame; how many WebSockets the page had opened by it. */
  muted: { uplink: Buffer; opened: number } | null
}

/** A TCP proxy in the test process, in front of the server at `target`. */
async function startBlackHoleProxy(target: string): Promise<BlackHoleProxy> {
  const { hostname, port: targetPort } = new URL(target)
  const links = new Set<Link>()
  let muteNext = false
  let webSockets = 0
  let openedBeforeMutedClosed: number | null = null
  let down: PacedDirection | null = null
  let up: PacedDirection | null = null
  const proxy = net.createServer((page) => {
    const server = net.connect(Number(targetPort), hostname)
    const link: Link = { page, server, webSocket: false, dead: false, muted: null }
    links.add(link)
    page.on('data', (chunk: Buffer) => {
      if (link.dead) return
      if (link.muted) return forwardMutedUplink(link, chunk)
      const head = chunk.toString('latin1', 0, 2048)
      if (head.startsWith('GET /_telefunc') && /^upgrade: websocket/im.test(head)) {
        link.webSocket = true
        webSockets++
        if (muteNext) {
          muteNext = false
          link.muted = { uplink: Buffer.alloc(0), opened: webSockets }
        }
      }
      if (up) up.enqueue(page, server, chunk)
      else server.write(chunk)
    })
    let answered = false
    server.on('data', (chunk: Buffer) => {
      if (link.dead || (link.muted && answered)) return
      // The server sends nothing after its upgrade answer before the page's first frame.
      answered = true
      if (down) down.enqueue(server, page, chunk)
      else page.write(chunk)
    })
    // A dead or muted link tells neither end that the other went.
    page.on('close', () => {
      if (!link.dead && !link.muted) end()
    })
    server.on('close', () => {
      if (link.muted) openedBeforeMutedClosed = webSockets - link.muted.opened
      if (!link.dead && !link.muted) end()
    })
    const end = () => {
      links.delete(link)
      page.destroy()
      server.destroy()
    }
    page.on('error', () => {})
    server.on('error', () => {})
  })
  await new Promise<void>((resolve) => proxy.listen(0, 'localhost', resolve))
  const { port } = proxy.address() as net.AddressInfo
  return {
    url: `http://localhost:${port}`,
    blackHoleWebSockets() {
      let dead = 0
      for (const link of links) {
        if (!link.webSocket) continue
        link.dead = true
        dead++
      }
      return dead
    },
    muteNextWebSocket() {
      muteNext = true
    },
    openedBeforeMutedClosed: () => openedBeforeMutedClosed,
    pace({ downBytesPerSecond, upBytesPerSecond, queueBytes, burstMs = 5, delayMs = 0 }) {
      down = new PacedDirection(downBytesPerSecond, queueBytes, burstMs, delayMs)
      up = new PacedDirection(upBytesPerSecond, queueBytes, burstMs, delayMs)
    },
    async close() {
      down?.stop()
      up?.stop()
      for (const link of links) {
        link.page.destroy()
        link.server.destroy()
      }
      links.clear()
      await new Promise<void>((resolve) => proxy.close(() => resolve()))
    },
  }
}

/** Forwards each whole WebSocket frame the page sends, up to its close frame. */
function forwardMutedUplink(link: Link, chunk: Buffer) {
  let bytes = Buffer.concat([link.muted!.uplink, chunk])
  for (;;) {
    const length = frameLength(bytes)
    if (length === null || bytes.length < length) break
    if ((bytes[0]! & 0x0f) === 0x8) {
      link.dead = true
      return
    }
    link.server.write(bytes.subarray(0, length))
    bytes = bytes.subarray(length)
  }
  link.muted!.uplink = bytes
}

/** RFC 6455 §5.2: a page's frame is masked, so its 4-byte mask follows the length. */
function frameLength(bytes: Buffer): number | null {
  if (bytes.length < 2) return null
  const short = bytes[1]! & 0x7f
  if (short < 126) return 2 + 4 + short
  if (short === 126) return bytes.length < 4 ? null : 4 + 4 + bytes.readUInt16BE(2)
  return bytes.length < 10 ? null : 10 + 4 + Number(bytes.readBigUInt64BE(2))
}

/** The most of the link's time one tick of a busy link sends, however late it runs: a link of the named rate doesn't
 *  hand over more than a second of it at once. */
const MAX_TICK_MS = 1000

/** One direction of a slow link: what its senders write leaves in order at `bytesPerSecond`, no sooner than `delayMs`,
 *  however late its ticks run. */
class PacedDirection {
  private queue: { from: net.Socket; to: net.Socket; chunk: Buffer; at: number }[] = []
  private head = 0
  private queued = 0
  private paused = new Set<net.Socket>()
  private allowance = 0
  private last = performance.now()
  /** Whether bytes that were due still waited at the last tick: the link was busy the whole time since. */
  private backlogged = false
  private timer: ReturnType<typeof setInterval>

  constructor(
    private bytesPerSecond: number,
    private queueBytes: number,
    private burstMs: number,
    private delayMs: number,
  ) {
    this.timer = setInterval(() => this.drain(), burstMs)
  }

  enqueue(from: net.Socket, to: net.Socket, chunk: Buffer) {
    this.queue.push({ from, to, chunk, at: performance.now() + this.delayMs })
    this.queued += chunk.length
    if (this.queued > this.queueBytes && !this.paused.has(from)) {
      from.pause()
      this.paused.add(from)
    }
  }

  private drain() {
    const now = performance.now()
    // A busy link carried bytes the whole time since the last tick, however late this one runs. One idle for a while
    // doesn't send a burst once it has data again.
    const mostMs = this.backlogged ? MAX_TICK_MS : 2 * this.burstMs
    this.allowance = Math.min(
      this.allowance + ((now - this.last) * this.bytesPerSecond) / 1000,
      (this.bytesPerSecond * mostMs) / 1000,
    )
    this.last = now
    while (this.allowance >= 1 && this.head < this.queue.length && this.queue[this.head]!.at <= now) {
      const entry = this.queue[this.head]!
      const bytes = Math.min(entry.chunk.length, Math.floor(this.allowance))
      if (!entry.to.destroyed) entry.to.write(entry.chunk.subarray(0, bytes))
      this.allowance -= bytes
      this.queued -= bytes
      if (bytes === entry.chunk.length) this.head++
      else entry.chunk = entry.chunk.subarray(bytes)
    }
    this.backlogged = this.head < this.queue.length && this.queue[this.head]!.at <= now
    if (this.head > 1024 && this.head * 2 > this.queue.length) {
      this.queue = this.queue.slice(this.head)
      this.head = 0
    }
    if (this.queued <= this.queueBytes / 2) {
      for (const from of this.paused) from.resume()
      this.paused.clear()
    }
  }

  stop() {
    clearInterval(this.timer)
  }
}
