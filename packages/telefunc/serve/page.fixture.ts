export { rowOfPages, startPage }
export type { Page, Row }

import type { IncomingMessage, Server, ServerResponse } from 'node:http'
import type { Server as TcpServer, Socket } from 'node:net'
import { Worker } from 'node:worker_threads'
import { inject } from 'vitest'
import { hasProp } from '../utils/hasProp.js'
import { getChannelMux } from '../wire-protocol/channel/server/ChannelMux.js'
import { ServerChannel } from '../wire-protocol/channel/server/ServerChannel.js'

// A spec's page, in a realm of its own (`page.worker.mjs`): its `fetch`, `WebSocket`, `pagehide` and session token are
// its own, and ending it ends all it started, as closing a tab ends its page. The page runs the package as built, which
// vitest's `globalSetup` builds before the specs (`page.build.fixture.ts`); the spec's servers run its source.

/** What a page does, from the test's realm. */
type Page = {
  /** The answer to an acknowledged send. */
  send(data: string): Promise<string>
  /** Whether the page moved to a WebSocket of its own within `ms`: one is open, and the acknowledgement of a send its
   *  server answers comes over it. */
  moved(ms: number): Promise<boolean>
  /** Each WebSocket the page opened: its URL, what opened it, and its state. */
  sockets(): Promise<string[]>
  /** The page leaves, as a closed tab's page does: each of its transports reads a `pagehide` that keeps nothing. */
  leave(): Promise<void>
  /** The tab's unload after its `pagehide`: each request the page sent without `keepalive` ends, its stream too. */
  unload(): Promise<void>
  /** Whether, within `ms`, each of the page's WebSockets ended and each of its goodbyes got its answer. */
  left(ms: number): Promise<boolean>
  /** The page closes its channel: the close's outcome, null where it closed gracefully. */
  close(): Promise<string | null>
  /** Each request the page started and each WebSocket it opened, with when (`Date.now()`). */
  attempts(): Promise<{ at: number; what: string }[]>
  /** What the page's realm still holds open, by kind, and its `pagehide` listeners. */
  resources(): Promise<{ open: string[]; pageHideListeners: number }>
  /** The transports each RECONCILED its SSE streams carried. */
  readonly offers: string[][]
  /** What each of its `keepalive` goodbyes got. */
  readonly goodbyes: string[]
  /** Ends the page's realm: what of it didn't end, an error the page didn't catch or a worker still running 1 s on. */
  end(): Promise<string[]>
}

const worker = new URL('./page.worker.mjs', import.meta.url)

/** Starts a page with a channel to `channelId` through `telefuncUrl`; `marker` is its own `x-telefunc-server` header,
 *  `idleTimeout` its connection's, null for its server's; `random` what its `Math.random()` returns, null for its own. */
async function startPage({
  telefuncUrl,
  channelId,
  marker,
  idleTimeout = 0,
  random = null,
}: {
  telefuncUrl: string
  channelId: string
  marker?: string
  idleTimeout?: number | null
  random?: number | null
}): Promise<Page> {
  const build = inject('pageBuild')
  if (build === undefined)
    throw new Error("The pages need the package as built, which vitest.config.ts's `globalSetup` builds")
  if ('failed' in build) throw new Error(build.failed)
  const realm = new Worker(worker, {
    workerData: { telefuncUrl, channelId, marker, idleTimeout, random, dist: build.dist },
  })
  const offers: string[][] = []
  const goodbyes: string[] = []
  const uncaught: string[] = []
  const replies = new Map<number, { resolve: (value: unknown) => void; reject: (err: Error) => void }>()
  let ended: string | null = null
  const fail = (why: string) => {
    ended ??= why
    for (const { reject } of replies.values()) reject(new Error(`the page's realm ended: ${why}`))
    replies.clear()
  }
  realm.on('error', (err) => {
    uncaught.push(err.stack ?? String(err))
    fail(`uncaught ${err.message}`)
  })
  realm.on('exit', (code) => fail(`exit ${code}`))
  const ready = new Promise<void>((resolve) => {
    realm.on('message', (message: WorkerMessage) => {
      if (message.kind === 'ready') resolve()
      else if (message.kind === 'offer') offers.push(message.transports)
      else if (message.kind === 'goodbye') goodbyes.push(message.what)
      else {
        const reply = replies.get(message.id)
        replies.delete(message.id)
        if (message.error === undefined) reply?.resolve(message.value)
        else reply?.reject(new Error(message.error))
      }
    })
  })
  let ids = 0
  const ask = (command: string, arg?: unknown): Promise<unknown> => {
    if (ended !== null) return Promise.reject(new Error(`the page's realm ended: ${ended}`))
    const id = ids++
    return new Promise((resolve, reject) => {
      replies.set(id, { resolve, reject })
      realm.postMessage({ id, command, arg })
    })
  }
  await Promise.race([ready, new Promise((_, reject) => realm.once('exit', () => reject(new Error(String(ended)))))])
  return {
    send: async (data) => String(await ask('send', data)),
    moved: async (ms) => (await ask('moved', ms)) === true,
    sockets: async () => strings(await ask('sockets')),
    leave: async () => void (await ask('leave')),
    unload: async () => void (await ask('unload')),
    left: async (ms) => (await ask('left', ms)) === true,
    close: async () => {
      const outcome = await ask('close')
      return outcome === null ? null : String(outcome)
    },
    attempts: async () => {
      const value = await ask('attempts')
      return (Array.isArray(value) ? value : []).flatMap((each: unknown) =>
        hasProp(each, 'at', 'number') && hasProp(each, 'what', 'string') ? [{ at: each.at, what: each.what }] : [],
      )
    },
    resources: async () => {
      const value = await ask('resources')
      return {
        open: hasProp(value, 'open') ? strings(value.open) : [],
        pageHideListeners: hasProp(value, 'pageHideListeners', 'number') ? value.pageHideListeners : -1,
      }
    },
    offers,
    goodbyes,
    async end() {
      const exited = new Promise<boolean>((resolve) => {
        if (ended?.startsWith('exit')) return resolve(true)
        realm.once('exit', () => resolve(true))
        setTimeout(() => resolve(false), 1_000).unref()
      })
      await realm.terminate()
      return [
        ...uncaught.map((stack) => `the page threw ${stack}`),
        ...((await exited) ? [] : ["the page's worker still runs 1 s after it was ended"]),
      ]
    },
  }
}

type WorkerMessage =
  | { kind: 'ready' }
  | { kind: 'offer'; transports: string[] }
  | { kind: 'goodbye'; what: string }
  | { kind: 'reply'; id: number; value?: unknown; error?: string }

function strings(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String) : []
}

/** A spec's row: its pages and servers, which its end ends, pages first, and what they left behind. */
type Row = {
  /** What the row's end runs, the last pushed first: a server closes only after the pages opened since it ended. */
  readonly closers: (() => Promise<void>)[]
  /** What the row's pages left behind: a socket still open, a channel its server didn't end once the page left, the
   *  page's own errors, or a connection or request its server still had once the pages ended. */
  readonly left: string[]
  /** A page with a channel through `host`, whose server answers each send with `ack:` and the send. `marker`: its own
   *  `x-telefunc-server` header; `idleTimeout`: its connection's, null for its server's; `random`: what its
   *  `Math.random()` returns, which its reconnect delays take that part of, null for its own; `serverTold`: whether its
   *  server can be told the page left, which a server that takes no new connection can't; `unload`: whether the tab's
   *  unload follows the page's `pagehide`; `onServerEnd`: called with the outcome its server channel ended with. */
  page(
    host: string,
    options?: {
      marker?: string
      idleTimeout?: number | null
      random?: number | null
      serverTold?: boolean
      unload?: boolean
      onServerEnd?: (outcome: unknown) => void
    },
  ): Promise<Page>
  /** Each WebSocket the row's pages not ended yet opened to `host`: its URL, what opened it, and its state. */
  socketsTo(host: string): Promise<string[]>
  /** The closer of `server`, which follows its connections and requests from now on. */
  closerOf(server: Server | TcpServer): () => Promise<void>
  /** Closes `server` and destroys `sockets`, its connections, once none is left and `requests()` is 0, or after 5 s. */
  closeOnceIdle(server: Server | TcpServer, sockets: Set<Socket>, requests?: () => number): Promise<void>
  /** Runs the row's closers. */
  end(): Promise<void>
}

function rowOfPages(): Row {
  const closers: (() => Promise<void>)[] = []
  const left: string[] = []
  const pages = new Set<{ host: string; page: Page }>()
  /** Each server `closerOf()` follows: the addresses it took connections on, and its connections and requests open. */
  const served = new Set<{ hosts: Set<string>; open: () => number }>()
  const closeOnceIdle = async (server: Server | TcpServer, sockets: Set<Socket>, requests = () => 0) => {
    const address = server.address()
    const host = typeof address === 'object' && address !== null ? `127.0.0.1:${address.port}` : 'a closed server'
    const deadline = Date.now() + 5_000
    while ((sockets.size > 0 || requests() > 0) && Date.now() < deadline)
      await new Promise((resolve) => setTimeout(resolve, 20))
    if (sockets.size > 0 || requests() > 0)
      left.push(`${host} still had ${sockets.size} connections and ${requests()} requests once its pages ended`)
    const closed = new Promise<void>((resolve) => server.close(() => resolve()))
    for (const socket of sockets) socket.destroy()
    await closed
  }
  return {
    closers,
    left,
    async page(host, { marker, idleTimeout, random, serverTold = true, unload = false, onServerEnd } = {}) {
      const serverChannel = new ServerChannel<string, string>()
      serverChannel.listen((data) => `ack:${data}`)
      getChannelMux().registerChannel(serverChannel)
      if (onServerEnd) void serverChannel.closed.then(onServerEnd, onServerEnd)
      const telefuncUrl = `http://${host}/_telefunc`
      const page = await startPage({ telefuncUrl, channelId: serverChannel.id, marker, idleTimeout, random })
      const entry = { host, page }
      pages.add(entry)
      let serverEnded = false
      const ended = () => {
        serverEnded = true
      }
      void serverChannel.closed.then(ended, ended)
      // The page leaves before its server, as a closed tab does: within 5 s its sockets are closed and its server ended
      // its channel. Then its realm ends. A server that can't be told still holds the channel for the page's reconnect
      // once it has none of the page's connections and requests left, after which nothing of the page can reach it.
      closers.push(async () => {
        await page.leave()
        if (unload) await page.unload()
        const gone = await page.left(5_000)
        const deadline = Date.now() + 5_000
        while (serverTold && !serverEnded && Date.now() < deadline)
          await new Promise((resolve) => setTimeout(resolve, 20))
        if (!gone)
          left.push(...(await page.sockets()).filter((ws) => !/, (closed|failed)$/.test(ws)).map((ws) => `open ${ws}`))
        if (serverTold && !serverEnded) left.push(`channel ${serverChannel.id} on ${host} not ended by its server`)
        pages.delete(entry)
        left.push(...(await page.end()))
        if (serverTold) return
        const reached = [...served].filter(({ hosts }) => hosts.has(host))
        while (reached.some(({ open }) => open() > 0) && Date.now() < deadline)
          await new Promise((resolve) => setTimeout(resolve, 20))
        await new Promise((resolve) => setImmediate(resolve))
        if (serverEnded) left.push(`channel ${serverChannel.id} on ${host} ended by a server not told`)
      })
      return page
    },
    async socketsTo(host) {
      const all = await Promise.all([...pages].map(({ page }) => page.sockets()))
      return all.flat().filter((ws) => new URL(ws.slice(0, ws.indexOf(' '))).host === host)
    },
    closerOf(server) {
      const sockets = new Set<Socket>()
      let requests = 0
      const hosts = new Set<string>()
      const follows = { hosts, open: () => sockets.size + requests }
      served.add(follows)
      server.on('connection', (socket: Socket) => {
        hosts.add(`127.0.0.1:${socket.localPort}`)
        sockets.add(socket)
        socket.once('close', () => sockets.delete(socket))
      })
      server.on('request', (_req: IncomingMessage, res: ServerResponse) => {
        requests++
        res.once('close', () => requests--)
      })
      return async () => {
        await closeOnceIdle(server, sockets, () => requests)
        served.delete(follows)
      }
    },
    closeOnceIdle,
    async end() {
      for (const close of closers.splice(0).reverse()) await close()
    },
  }
}
