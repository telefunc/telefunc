// In-memory two-connection driver for the REAL `ChannelMux` + REAL `ServerChannel`s.
//
// Nothing in the repo ever constructed a `ChannelMux` before this, so the SSE→WS upgrade's
// server half had zero coverage. The seam needs no production hook: `ServerTransport` is a
// 5-method structural type and connections are identity-keyed map entries, so `{}` and `{}`
// are two fully distinct connections. `getConnId()` returning a string vs `null` is already
// the SSE-vs-WS discriminator the mux keys on.
//
// Two deliberate shape choices:
//   - **Callbacks, not streams.** A stream-driven harness makes reader wakeups non-timer-driven,
//     which is why `connection.reconnect-ordering.spec.ts` has to use wall-clock sleeps. Frames
//     go in through `onConnectionRawMessage` directly, so fake timers stay usable.
//   - **Per-wire `sent[]`, never merged.** Wire-of-delivery is itself an oracle (which wire got
//     FIN vs RECONCILED), and merging the logs would destroy it.
//
// `resolvedOptions` is resolved lazily per instance and `dispose()` deliberately does NOT reset
// it, so config cannot vary across `dispose()` within one mux. Each harness therefore constructs
// its own `ChannelMux` rather than sharing the module-global one.

export { createMuxHarness, settle, textFrame, reconcileFrame, prepareFrame, pingFrame }
export { lengthPrefixed, globalRegisterChannel, disposeGlobalChannels, openSseDownstream, openGlobalProbeWire }
export type { MuxHarness, HarnessWire, HarnessChannel }

import { expect } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'

import {
  ChannelMux,
  getChannelMux,
  type BacklogSnapshot,
  type MuxResourceLimits,
  type ServerTransport,
} from './server/mux.js'
import { ServerChannel } from './server/channel.js'
import { handleSseChannelRequest } from './server/sse.js'
import { encodeSseRequestMetadata } from './sse-request.js'
import { encodeU32 } from './frame.js'
import { decode, encode, type DecodedFrame, type PreparePayload, type ReconcilePayload } from './shared-ws.js'

/** Let every already-scheduled microtask AND macrotask turn run. `setTimeout(0)` rather than
 *  `await null`, because the recv chain hops several promise links per frame. */
const settle = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0))

type HarnessWire = {
  /** The identity-keyed connection object. Distinct object ⇒ distinct connection. */
  readonly conn: object
  /** Frames the mux sent on THIS wire, decoded, in call order. */
  readonly sent: DecodedFrame[]
  /** Session id the mux assigned to this connection, or undefined before its first reconcile.
   *  Read straight off the transport — the same source `handleFrame` reads at `mux.ts:245`. */
  sessionId(): string | undefined
  /** Whether the mux asked the transport to kill this wire. */
  terminated(): boolean
  /** This wire's recv-chain backlog, read straight off the fields the overflow check compares.
   *  Null once the connection entry is gone. */
  backlog(): BacklogSnapshot | null
  /** Hand a frame to the mux exactly as `ws.ts:33` / `sse.ts:145` do. The returned promise
   *  settles when this frame's recv-chain turn completes — `await` it for ordinary frames,
   *  keep it un-awaited to model a frame parked behind an earlier turn. */
  deliver(frame: Uint8Array<ArrayBuffer>): Promise<void>
  /** Model the wire dying (`sse.ts:262` / `ws.ts:39`). */
  close(isPermanent?: boolean): void
}

type HarnessChannel = {
  readonly id: string
  readonly channel: ServerChannel<number, never>
  /** Application-level listener payload log — the ONLY non-circular delivery oracle.
   *  `_lastClientSeq` and the RECONCILED `open[].lastSeq` are the same bookkeeping the
   *  replay decision uses, so asserting on them proves nothing. */
  readonly received: number[]
}

type MuxHarness = {
  readonly mux: ChannelMux
  readonly sse: HarnessWire
  readonly ws: HarnessWire
  /** An additional wire, for the invariants that need a second probe (one stage per old session)
   *  or a replacement SSE connection. `connId: null` makes it a WS. */
  makeWire(connId: string | null): HarnessWire
  /** Build a channel WITHOUT registering it — a reconcile naming it with `initial: true`
   *  then parks in `waitForChannelRegistration` until `register()` releases it. */
  createChannel(id: string): HarnessChannel
  /** Build and register in one step. */
  registerChannel(id: string): HarnessChannel
  /** Release every reconcile parked on this channel id (fires waiters synchronously). */
  register(channel: HarnessChannel): void
  dispose(): void
}

/** `limits` are injected into the REAL `ChannelMux`, so a mechanism test at a small limit
 *  drives the same enforcement path production runs — not a pure accountant standing beside it. */
function createMuxHarness(limits: Partial<MuxResourceLimits> = {}): MuxHarness {
  const mux = new ChannelMux(limits)
  const channels: HarnessChannel[] = []

  const makeWire = (connId: string | null): HarnessWire => {
    const conn = {}
    const sent: DecodedFrame[] = []
    let sessionId: string | undefined
    let terminated = false

    const transport: ServerTransport<object> = {
      getSessionId: () => sessionId,
      setSessionId: (_conn, id) => {
        sessionId = id
      },
      getConnId: () => connId,
      sendNow: (_conn, frame) => {
        sent.push(decode(frame))
      },
      terminateConnection: () => {
        terminated = true
      },
    }

    mux.onConnectionOpen(conn, transport)

    return {
      conn,
      sent,
      sessionId: () => sessionId,
      terminated: () => terminated,
      backlog: () => mux._getBacklogSnapshot(conn),
      deliver: (frame) => mux.onConnectionRawMessage(conn, frame),
      close: (isPermanent = false) => mux.onConnectionClosed(conn, isPermanent),
    }
  }

  const createChannel = (id: string): HarnessChannel => {
    const channel = new ServerChannel<number, never>({ id })
    const received: number[] = []
    channel.listen((n) => {
      received.push(n)
    })
    const entry: HarnessChannel = { id, channel, received }
    channels.push(entry)
    return entry
  }

  return {
    mux,
    sse: makeWire('sse-conn-1'),
    ws: makeWire(null),
    makeWire,
    createChannel,
    registerChannel: (id) => {
      const entry = createChannel(id)
      mux.registerChannel(entry.channel)
      return entry
    },
    register: (entry) => mux.registerChannel(entry.channel),
    dispose: () => {
      mux.dispose()
      // Releases each channel's connect-TTL / reconnect timers and its replay buffer.
      for (const { channel } of channels) channel._onPeerClose()
      channels.length = 0
    },
  }
}

/** A client→server TEXT frame carrying `value` on channel index `ix` at replay seq `seq`. */
function textFrame(ix: number, seq: number, value: number): Uint8Array<ArrayBuffer> {
  return encode.text(ix, stringify(value), seq)
}

function reconcileFrame(payload: ReconcilePayload): Uint8Array<ArrayBuffer> {
  return encode.reconcile(payload)
}

function prepareFrame(payload: PreparePayload): Uint8Array<ArrayBuffer> {
  return encode.prepare(payload)
}

function pingFrame(): Uint8Array<ArrayBuffer> {
  return encode.ping()
}

// ── The REAL `sse.ts` seam ───────────────────────────────────────────────────────────────────
//
// Some guards live in `handleBatchPost` / `runStreamResponse`, ABOVE the mux — the harness above
// delivers straight to `onConnectionRawMessage` and never reaches them. These drive
// `handleSseChannelRequest` with hand-built Requests against the MODULE-GLOBAL mux that `sse.ts`
// resolves, which is also what makes them exercise the shipped default limits.

function lengthPrefixed(bytes: Uint8Array<ArrayBuffer>): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(4 + bytes.length) as Uint8Array<ArrayBuffer>
  out.set(encodeU32(bytes.length), 0)
  out.set(bytes, 4)
  return out
}

const globalChannels: ServerChannel<number, never>[] = []

function globalRegisterChannel(id: string): ServerChannel<number, never> {
  const channel = new ServerChannel<number, never>({ id })
  getChannelMux().registerChannel(channel)
  globalChannels.push(channel)
  return channel
}

/** `afterEach` teardown for `globalRegisterChannel` — releases connect-TTL / reconnect timers. */
function disposeGlobalChannels(): void {
  for (const channel of globalChannels) channel._onPeerClose()
  globalChannels.length = 0
}

/** Open the SSE downstream so the connection exists and its `ready` gate is resolved. The
 *  stream-response POST carries the first reconcile, which registers `channelId` at ix 0.
 *  Returns the session it minted. */
async function openSseDownstream(connId: string, channelId: string): Promise<string> {
  const reconcile = encode.reconcile({ open: [{ id: channelId, ix: 0, lastSeq: 0, initial: true }] })
  const body = new Blob([encodeSseRequestMetadata({ connId, streamResponse: true }), lengthPrefixed(reconcile)])
  const response = await handleSseChannelRequest(new Request('http://test.local/_telefunc', { method: 'POST', body }))
  expect(response?.statusCode).toBe(200)
  // `runStreamResponse` consumes the body asynchronously; the session appearing IS its completion.
  // Polled rather than settled: real `ReadableStream` wakeups are not timer-driven and take an
  // unbounded number of turns under load — a single `settle()` here was measured to pass alone and
  // fail intermittently across a full suite run.
  const live = () => getChannelMux().getConnectionByConnId(connId) as { sessionId: string | null } | undefined
  for (let i = 0; i < 1_000; i++) {
    if (typeof live()?.sessionId === 'string') return live()!.sessionId as string
    await settle()
  }
  throw new Error(`Timed out waiting for downstream ${connId} to reconcile`)
}

/** A probe (WebSocket-shaped) connection on the same global mux `sse.ts` uses. `getConnId: null` is
 *  already the SSE-vs-WS discriminator the mux keys on, so no new production seam is needed. */
function openGlobalProbeWire() {
  const conn = {}
  const sent: DecodedFrame[] = []
  let sessionId: string | undefined
  let terminated = false
  const transport: ServerTransport<object> = {
    getSessionId: () => sessionId,
    setSessionId: (_c, id) => {
      sessionId = id
    },
    getConnId: () => null,
    sendNow: (_c, frame) => {
      sent.push(decode(frame))
    },
    terminateConnection: () => {
      terminated = true
    },
  }
  getChannelMux().onConnectionOpen(conn, transport)
  return {
    conn,
    sent,
    sessionId: () => sessionId,
    terminated: () => terminated,
    deliver: (frame: Uint8Array<ArrayBuffer>) => getChannelMux().onConnectionRawMessage(conn, frame),
  }
}
