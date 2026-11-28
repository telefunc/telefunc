// A TCP forwarder between the backend and a store, for the store specs: a row points it at a store, holds either
// direction or lets a set number of bytes through, and reads back what went through each connection and how it ended.
export { forwarder, pointNowhere, sent, type Forwarder, type Passage, type Flow, type Direction }

import { connect, createServer, type Socket } from 'node:net'

/** Requests go to the store; replies and pushes go to the client. */
type Direction = 'toStore' | 'toClient'

/** What a forwarder keeps of one direction of a connection. */
type Flow = {
  /** The bytes it forwarded. */
  bytes: number
  /** When it forwarded the last of them, by `performance.now()`. */
  lastAt: number | undefined
  /** The latest chunks it forwarded, at most `CHUNKS_KEPT`, each with the first `HEAD_BYTES` of it as latin1: enough
   *  to read a command's name and its first arguments. */
  chunks: { at: number; bytes: number; head: string }[]
}

/** One connection a client made through the forwarder. */
type Passage = {
  /** When the forwarder took it, by `performance.now()`. */
  opened: number
  toStore: Flow
  toClient: Flow
  /** How it ended, while open undefined: 'reset' when the client reset it (an RST), 'end' when the client closed it,
   *  'store' when the store's side ended first, and 'dropped' when the forwarder dropped it (`to()`, `close()`). */
  ended: { at: number; how: 'reset' | 'end' | 'store' | 'dropped' } | undefined
}

type Forwarder = {
  port: number
  /** How many connections it took. */
  connections: () => number
  /** The latest connections it took, at most `PASSAGES_KEPT`, the oldest first. */
  passages: () => readonly Passage[]
  /** Points it at the server at `host` and `port` (a store's 6379 unless given), or nowhere (`''`), where it ends each
   *  connection it takes; the connections to the last server drop. */
  to: (host: string, port?: number) => void
  /** Forwards nothing more that way, in every connection, its next ones too: what comes stays in the sockets, whose
   *  buffers fill and then hold the sender back. */
  hold: (direction: Direction) => void
  /** Forwards everything that way again. */
  flow: (direction: Direction) => void
  /** Forwards `bytes` more that way, across its connections, then holds it. */
  pass: (direction: Direction, bytes: number) => void
  /** Forgets the connections it took and forwards everything both ways again: a row's start. */
  clear: () => void
  /** Resolves once `seen()` holds, which it asks again each time a byte moves or a connection opens or ends. */
  until: (seen: () => boolean) => Promise<void>
  close: () => void
}

/** How many chunks a flow keeps, and how many bytes of each: the record stays bounded on a row that moves gigabytes. */
const CHUNKS_KEPT = 1024
const HEAD_BYTES = 256
/** How many connections a forwarder keeps, so a client that reconnects in a loop leaves a bounded record. */
const PASSAGES_KEPT = 64

/** The forwarders made and not closed: `pointNowhere()` points each nowhere. */
const live = new Set<Forwarder>()

/** Points every forwarder nowhere, as a row does once it removes its stores: Docker gives a removed container's address
 *  to the next container it starts. */
function pointNowhere(): void {
  for (const proxy of live) proxy.to('')
}

/** Whether the head of a chunk that went `direction` in one of the forwarder's connections holds `text`, in any case:
 *  ioredis writes a command's name as the caller spelled it. */
function sent(proxy: Forwarder, direction: Direction, text: string): boolean {
  const lower = text.toLowerCase()
  return proxy
    .passages()
    .some(({ [direction]: flow }) => flow.chunks.some(({ head }) => head.toLowerCase().includes(lower)))
}

const newFlow = (): Flow => ({ bytes: 0, lastAt: undefined, chunks: [] })

function record(flow: Flow, chunk: Buffer): void {
  const at = performance.now()
  flow.bytes += chunk.length
  flow.lastAt = at
  flow.chunks.push({ at, bytes: chunk.length, head: chunk.toString('latin1', 0, HEAD_BYTES) })
  if (flow.chunks.length > CHUNKS_KEPT) flow.chunks.shift()
}

/** A new forwarder on a port of 127.0.0.1, pointed nowhere and forwarding both ways. */
async function forwarder(): Promise<Forwarder> {
  let upstream = ''
  let upstreamPort = 6379
  let connections = 0
  const passages: Passage[] = []
  /** The bytes each direction may still forward: `Infinity` while it flows, 0 while it is held. */
  const allowance: Record<Direction, number> = { toStore: Infinity, toClient: Infinity }
  /** Each open connection's way to drop it and to forward what its sockets hold. */
  const open = new Map<Passage, { drop: () => void; pump: () => void }>()
  const waiters = new Set<{ seen: () => boolean; resolve: () => void }>()
  const ask = () => {
    for (const waiter of waiters) {
      if (!waiter.seen()) continue
      waiters.delete(waiter)
      waiter.resolve()
    }
  }

  const server = createServer((client) => {
    connections++
    const passage: Passage = { opened: performance.now(), toStore: newFlow(), toClient: newFlow(), ended: undefined }
    passages.push(passage)
    if (passages.length > PASSAGES_KEPT) passages.shift()
    const end = (how: NonNullable<Passage['ended']>['how']) => {
      passage.ended ??= { at: performance.now(), how }
    }
    if (upstream === '') {
      end('dropped')
      client.destroy()
      ask()
      return
    }
    const store = connect(upstreamPort, upstream)
    const pumps = [forward(client, store, 'toStore'), forward(store, client, 'toClient')]
    const drop = () => {
      end('dropped')
      client.destroy()
      store.destroy()
    }
    open.set(passage, { drop, pump: () => pumps.forEach((pump) => pump()) })
    client.on('error', (error) => end('code' in error && error.code === 'ECONNRESET' ? 'reset' : 'end'))
    client.on('end', () => end('end'))
    store.on('error', () => end('store'))
    store.on('end', () => end('store'))
    for (const socket of [client, store]) {
      socket.on('close', () => {
        open.delete(passage)
        client.destroy()
        store.destroy()
        ask()
      })
    }
    ask()

    /** Forwards what `source` reads to `target` while `direction` has an allowance, waiting for `target` to drain. */
    function forward(source: Socket, target: Socket, direction: Direction): () => void {
      const flow = passage[direction]
      let draining = false
      const pump = () => {
        while (!draining && allowance[direction] > 0 && source.readableLength > 0) {
          const chunk: unknown = source.read(Math.min(allowance[direction], source.readableLength))
          if (!Buffer.isBuffer(chunk)) break
          allowance[direction] -= chunk.length
          record(flow, chunk)
          if (!target.write(chunk)) {
            draining = true
            target.once('drain', () => {
              draining = false
              pump()
            })
          }
        }
        // A read of an empty buffer is what lets the stream emit 'end' once the peer has closed its side.
        if (source.readableLength === 0) source.read()
        ask()
      }
      source.on('readable', pump)
      source.on('end', () => target.end())
      source.on('error', () => {})
      return pump
    }
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('the forwarder has no port')
  const pumpAll = () => {
    for (const { pump } of open.values()) pump()
  }
  const made: Forwarder = {
    port: address.port,
    connections: () => connections,
    passages: () => passages,
    to: (host, port = 6379) => {
      upstream = host
      upstreamPort = port
      for (const { drop } of [...open.values()]) drop()
    },
    hold: (direction) => {
      allowance[direction] = 0
    },
    flow: (direction) => {
      allowance[direction] = Infinity
      pumpAll()
    },
    pass: (direction, bytes) => {
      allowance[direction] = bytes
      pumpAll()
    },
    clear: () => {
      passages.length = 0
      connections = 0
      made.flow('toStore')
      made.flow('toClient')
    },
    until: (seen) =>
      new Promise((resolve) => {
        if (seen()) resolve()
        else waiters.add({ seen, resolve })
      }),
    close: () => {
      live.delete(made)
      for (const { drop } of [...open.values()]) drop()
      server.close()
    },
  }
  live.add(made)
  return made
}
