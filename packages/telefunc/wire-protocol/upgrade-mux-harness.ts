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
export type { MuxHarness, HarnessWire, HarnessChannel }

import { stringify } from '@brillout/json-serializer/stringify'

import { ChannelMux, type BacklogSnapshot, type MuxResourceLimits, type ServerTransport } from './server/mux.js'
import { ServerChannel } from './server/channel.js'
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
