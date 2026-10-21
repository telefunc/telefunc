export { startBlackHoleProxy }
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
async function startBlackHoleProxy(target: string, port: number): Promise<BlackHoleProxy> {
  const { hostname, port: targetPort } = new URL(target)
  const links = new Set<Link>()
  let muteNext = false
  let webSockets = 0
  let openedBeforeMutedClosed: number | null = null
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
      server.write(chunk)
    })
    let answered = false
    server.on('data', (chunk: Buffer) => {
      if (link.dead || (link.muted && answered)) return
      // The server sends nothing after its upgrade answer before the page's first frame.
      answered = true
      page.write(chunk)
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
  await new Promise<void>((resolve) => proxy.listen(port, 'localhost', resolve))
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
    async close() {
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
