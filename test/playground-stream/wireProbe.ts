export { installWireProbe, readWire, forgetWireData }
export type { Wire, WireReport }

// What a page's channel wires carried, seen from the page: which wire is live, and on WebTransport which frames each
// stream carried. A page falls back a rung silently where a wire fails, so a run checks the wire it ran on from this.

type Wire = 'webtransport' | 'ws' | 'sse'

/** What the page saw of the frames on one WebTransport stream, by tag. */
type StreamSeen = { dir: 'in' | 'out'; tags: Record<number, number> }

type WireState = {
  webtransport: { ready: boolean; closed: boolean; streams: StreamSeen[] }[]
  ws: { open: boolean; dataIn: number }[]
  sse: { open: boolean; bytesIn: number }[]
}

type WireReport = {
  /** The wires still open, the one the page is on. */
  live: Wire[]
  webtransport: { sessions: number; ready: number; dataIn: number; dataOut: number }
  ws: { sockets: number; dataIn: number }
  sse: { streams: number; bytesIn: number }
  /** Of the WebTransport streams each way: those that carried a broadcast's DOMAIN frames, its binary publishes, and
   *  both, which a track on a domain of its own never does. */
  streams: Record<'in' | 'out', { domain: number; binaryPublish: number; both: number }>
}

/** An init script (`page.addInitScript(installWireProbe)`): it wraps the page's WebTransport, `/_telefunc` WebSockets
 *  and SSE responses, observing the readers and writers the client uses, so nothing it sends or reads changes. */
function installWireProbe(): void {
  const g = globalThis as any
  if (g.__wire) return
  const DATA_TAG_MIN = 0x10
  const state: WireState = { webtransport: [], ws: [], sse: [] }
  g.__wire = state

  /** Each value the stream's readers read goes to `onValue`, its end to `onEnd`. */
  const onRead = (stream: ReadableStream<any>, onValue: (value: any) => void, onEnd = () => {}) => {
    const getReader = stream.getReader.bind(stream) as (options?: unknown) => ReadableStreamDefaultReader<any>
    stream.getReader = ((options?: unknown) => {
      const reader = getReader(options)
      const read = reader.read.bind(reader)
      reader.read = () =>
        read().then(
          (result) => {
            if (result.done) onEnd()
            else onValue(result.value)
            return result
          },
          (err: unknown) => {
            onEnd()
            throw err
          },
        )
      return reader
    }) as typeof stream.getReader
  }
  /** Each chunk the stream's writers write goes to `onChunk`. */
  const onWrite = (stream: WritableStream<Uint8Array>, onChunk: (chunk: Uint8Array) => void) => {
    const getWriter = stream.getWriter.bind(stream)
    stream.getWriter = () => {
      const writer = getWriter()
      const write = writer.write.bind(writer)
      writer.write = (chunk?: Uint8Array) => {
        if (chunk) onChunk(chunk)
        return write(chunk)
      }
      return writer
    }
  }
  /** Tallies the tags of a stream's frames, each a u32 big-endian length and then the frame, without copying them. */
  const framesOf = (seen: StreamSeen) => {
    let lengthBytes = 0
    let length = 0
    let skip = 0
    return (chunk: Uint8Array) => {
      let at = 0
      while (at < chunk.byteLength) {
        if (skip > 0) {
          const n = Math.min(skip, chunk.byteLength - at)
          at += n
          skip -= n
          continue
        }
        if (lengthBytes < 4) {
          length = length * 256 + chunk[at++]!
          lengthBytes++
          continue
        }
        const tag = chunk[at++]!
        seen.tags[tag] = (seen.tags[tag] ?? 0) + 1
        skip = length - 1
        lengthBytes = 0
        length = 0
      }
    }
  }

  const NativeWebTransport = g.WebTransport
  if (NativeWebTransport) {
    g.WebTransport = class extends NativeWebTransport {
      constructor(url: string, options?: unknown) {
        super(url, options)
        const session: WireState['webtransport'][number] = { ready: false, closed: false, streams: [] }
        state.webtransport.push(session)
        this.ready.then(
          () => (session.ready = true),
          () => {},
        )
        this.closed.then(
          () => (session.closed = true),
          () => (session.closed = true),
        )
        const stream = (dir: StreamSeen['dir']) => {
          const seen: StreamSeen = { dir, tags: {} }
          session.streams.push(seen)
          return framesOf(seen)
        }
        onRead(this.incomingUnidirectionalStreams, (incoming) => onRead(incoming, stream('in')))
        const createBidirectionalStream = this.createBidirectionalStream.bind(this)
        this.createBidirectionalStream = async (...args: unknown[]) => {
          const bidirectional = await createBidirectionalStream(...args)
          onRead(bidirectional.readable, stream('in'))
          onWrite(bidirectional.writable, stream('out'))
          return bidirectional
        }
        const createUnidirectionalStream = this.createUnidirectionalStream.bind(this)
        this.createUnidirectionalStream = async (...args: unknown[]) => {
          const unidirectional = await createUnidirectionalStream(...args)
          onWrite(unidirectional, stream('out'))
          return unidirectional
        }
      }
    }
  }

  const NativeWebSocket = g.WebSocket
  g.WebSocket = class extends NativeWebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      super(url, protocols)
      if (!String(url).includes('/_telefunc')) return
      const socket = { open: true, dataIn: 0 }
      state.ws.push(socket)
      this.addEventListener('close', () => (socket.open = false))
      this.addEventListener('message', ({ data }: MessageEvent) => {
        if (data instanceof ArrayBuffer && new Uint8Array(data)[0]! >= DATA_TAG_MIN) socket.dataIn++
      })
    }
  }

  const nativeFetch = g.fetch.bind(g)
  g.fetch = async (...args: unknown[]) => {
    const response: Response = await nativeFetch(...args)
    if (response.body && response.headers.get('content-type')?.startsWith('text/event-stream')) {
      const sse = { open: true, bytesIn: 0 }
      state.sse.push(sse)
      onRead(
        response.body,
        (chunk: Uint8Array) => (sse.bytesIn += chunk.byteLength),
        () => (sse.open = false),
      )
    }
    return response
  }
}

/** The page's report, from what `installWireProbe` saw (run in the page); null on a page it never ran in. */
function readWire(): WireReport | null {
  const DATA_TAG_MIN = 0x10
  const DOMAIN = 0x1a
  const BINARY_PUBLISHES = [0x17, 0x18]
  const state = (globalThis as any).__wire as WireState | undefined
  if (state === undefined) return null
  const streams = state.webtransport.flatMap((session) => session.streams)
  const dataOf = (dir: StreamSeen['dir']) =>
    streams
      .filter((seen) => seen.dir === dir)
      .reduce(
        (sum, seen) =>
          sum + Object.entries(seen.tags).reduce((n, [tag, count]) => n + (Number(tag) >= DATA_TAG_MIN ? count : 0), 0),
        0,
      )
  const tally = (dir: StreamSeen['dir']) => {
    const ofDir = streams.filter((seen) => seen.dir === dir)
    const domain = (seen: StreamSeen) => (seen.tags[DOMAIN] ?? 0) > 0
    const binary = (seen: StreamSeen) => BINARY_PUBLISHES.some((tag) => (seen.tags[tag] ?? 0) > 0)
    return {
      domain: ofDir.filter(domain).length,
      binaryPublish: ofDir.filter(binary).length,
      both: ofDir.filter((seen) => domain(seen) && binary(seen)).length,
    }
  }
  return {
    live: [
      ...(state.webtransport.some((session) => session.ready && !session.closed) ? (['webtransport'] as const) : []),
      ...(state.ws.some((socket) => socket.open) ? (['ws'] as const) : []),
      ...(state.sse.some((sse) => sse.open) ? (['sse'] as const) : []),
    ],
    webtransport: {
      sessions: state.webtransport.length,
      ready: state.webtransport.filter((session) => session.ready).length,
      dataIn: dataOf('in'),
      dataOut: dataOf('out'),
    },
    ws: { sockets: state.ws.length, dataIn: state.ws.reduce((sum, socket) => sum + socket.dataIn, 0) },
    sse: { streams: state.sse.length, bytesIn: state.sse.reduce((sum, sse) => sum + sse.bytesIn, 0) },
    streams: { in: tally('in'), out: tally('out') },
  }
}

/** Zeroes what the page's wires carried so far, keeping which are open (run in the page): a page a test reuses then
 *  reports only that test's data. */
function forgetWireData(): void {
  const state = (globalThis as any).__wire as WireState | undefined
  if (state === undefined) return
  for (const session of state.webtransport) for (const seen of session.streams) seen.tags = {}
  for (const socket of state.ws) socket.dataIn = 0
  for (const sse of state.sse) sse.bytesIn = 0
}
