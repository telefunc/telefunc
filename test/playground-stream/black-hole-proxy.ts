export { startBlackHoleProxy }
export type { BlackHoleProxy }

import * as net from 'node:net'

type BlackHoleProxy = {
  /** The origin the page loads through the proxy. */
  url: string
  /** The page's WebSocket connections stop carrying anything either way and stay open, as a link that died without a word; returns how many. */
  blackHoleWebSockets(): number
  close(): Promise<void>
}

type Link = { page: net.Socket; server: net.Socket; webSocket: boolean; dead: boolean }

/** A TCP proxy in the test process, in front of the server at `target`. */
async function startBlackHoleProxy(target: string, port: number): Promise<BlackHoleProxy> {
  const { hostname, port: targetPort } = new URL(target)
  const links = new Set<Link>()
  const proxy = net.createServer((page) => {
    const server = net.connect(Number(targetPort), hostname)
    const link: Link = { page, server, webSocket: false, dead: false }
    links.add(link)
    page.on('data', (chunk: Buffer) => {
      const head = chunk.toString('latin1', 0, 2048)
      if (head.startsWith('GET /_telefunc') && /^upgrade: websocket/im.test(head)) link.webSocket = true
      if (!link.dead) server.write(chunk)
    })
    server.on('data', (chunk: Buffer) => {
      if (!link.dead) page.write(chunk)
    })
    // A dead link tells neither end that the other went.
    const end = () => {
      if (link.dead) return
      links.delete(link)
      page.destroy()
      server.destroy()
    }
    page.on('close', end)
    server.on('close', end)
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
