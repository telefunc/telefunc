export { Bench }

import React, { useEffect, useRef, useState } from 'react'
import { withContext } from 'telefunc/client'
import {
  onBenchInfo,
  onBenchChannelEcho,
  onBenchChannelServerPush,
  onBenchChannelBinaryEcho,
  onBenchChannelBinaryServerPush,
  onBenchBroadcast,
  onBenchBroadcastServerPush,
  onBenchBroadcastBinary,
  onBenchBroadcastBinaryServerPush,
} from './Bench.telefunc'

// ── Configuration ──────────────────────────────────────────────────────────

const COUNT = 1000
const SIZES = [
  { label: '64B', bytes: 64 },
  { label: '1KB', bytes: 1024 },
  { label: '4KB', bytes: 4 * 1024 },
  { label: '16KB', bytes: 16 * 1024 },
] as const
const CONCURRENCY_LEVELS = [1] as const
const MAX_CONCURRENCY = Math.max(...CONCURRENCY_LEVELS)
const CELL_TIMEOUT_MS = 15_000

// ── Helpers ────────────────────────────────────────────────────────────────

/** Wrap a telefunc so its returned channels/broadcasts use a per-`runId` `ClientConnection`.
 *  Without this, parallel runs all multiplex over the page's single default WS pipe and we
 *  measure client-side wire saturation instead of server throughput. */
const isolated = <F extends (...args: any[]) => any>(fn: F, runId: number): F =>
  withContext(fn, { channel: { connectionKey: `bench-run-${runId}` } })

const percentile = (sorted: number[], p: number): number =>
  sorted.length === 0 ? 0 : sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p))]!

function summarize(rtts: number[]): { p50: number; p99: number } {
  const sorted = [...rtts].sort((a, b) => a - b)
  return { p50: percentile(sorted, 0.5), p99: percentile(sorted, 0.99) }
}

/** Race a Promise against a timeout. On timeout, throws an Error with `name = 'TimeoutError'`. */
async function withTimeout<T>(p: Promise<T>, ms: number, label: string): Promise<T> {
  let handle: ReturnType<typeof setTimeout>
  const timer = new Promise<never>((_, reject) => {
    handle = setTimeout(() => {
      const err = new Error(`${label} timed out after ${ms}ms`)
      err.name = 'TimeoutError'
      reject(err)
    }, ms)
  })
  try {
    return await Promise.race([p, timer])
  } finally {
    clearTimeout(handle!)
  }
}

// ── Result row ─────────────────────────────────────────────────────────────

/** One row per (scenario × size × concurrency) cell, aggregated across all parallel runs. */
type Result = {
  scenario: string
  size: string
  concurrency: number
  /** Aggregate throughput: total messages across all runs / cell wall time. */
  msgPerSec: number
  /** Aggregate bandwidth from the same definition. */
  mbPerSec: number
  /** Pooled across every run's per-message RTTs. Undefined for scenarios without per-msg ack. */
  p50?: number
  p99?: number
  notes?: string
}

// ── Scenario shape ─────────────────────────────────────────────────────────
//
// setup → measure → cleanup. setup opens + primes resources (untimed). measure runs the
// timed loop and returns an outcome (never pushes a row directly — that's the runner's
// job). cleanup releases. Each scenario type below is built via a factory that bakes in
// the text-vs-binary differences so we don't repeat the structural code.

type ScenarioOutcome = {
  instance: string
  totalMs: number
  rtts?: number[]
  notes?: string
}

type Scenario<R> = {
  name: string
  setup: (bytes: number, runId: number) => Promise<R>
  measure: (res: R, args: { count: number; bytes: number }) => Promise<ScenarioOutcome>
  cleanup: (res: R) => Promise<void>
}

// ── Channel-echo factory ───────────────────────────────────────────────────
//
// Sequential `count` round-trips on a pre-opened channel. Returns p50/p95/p99 RTT.

type EchoChannel<P> = {
  channel: { close: () => Promise<unknown> }
  instance: string
  payload: P
}

function defineChannelEcho<P, C extends { close: () => Promise<unknown> }>(args: {
  name: string
  open: (runId: number) => Promise<{ channel: C; instance: string }>
  buildPayload: (bytes: number) => P
  send: (channel: C, payload: P) => Promise<unknown>
}): Scenario<EchoChannel<P> & { channel: C }> {
  return {
    name: args.name,
    async setup(bytes, runId) {
      const { channel, instance } = await args.open(runId)
      return { channel, instance, payload: args.buildPayload(bytes) }
    },
    async measure({ channel, instance, payload }, { count }) {
      const rtts: number[] = []
      const t0 = performance.now()
      for (let i = 0; i < count; i++) {
        const sentAt = performance.now()
        await args.send(channel, payload)
        rtts.push(performance.now() - sentAt)
      }
      return { instance, totalMs: performance.now() - t0, rtts }
    },
    async cleanup({ channel }) {
      await channel.close()
    },
  }
}

// ── Channel-server-push factory ────────────────────────────────────────────
//
// Server fires `count` frames after the page sends a `'go'` trigger. Listener is
// registered in measure (sync, no race — the trigger's reply travels the same channel).

type ServerPushChannel<C> = { channel: C; instance: string }

function defineChannelServerPush<
  C extends {
    send: (msg: 'go') => Promise<unknown>
    close: () => Promise<unknown>
  },
>(args: {
  name: string
  open: (runId: number, count: number, bytes: number) => Promise<{ channel: C; instance: string }>
  listen: (channel: C, cb: () => void) => void
}): Scenario<ServerPushChannel<C>> {
  return {
    name: args.name,
    async setup(bytes, runId) {
      const { channel, instance } = await args.open(runId, COUNT, bytes)
      return { channel, instance }
    },
    async measure({ channel, instance }, { count }) {
      let firstArrival: number | null = null
      const done = new Promise<void>((resolve) => {
        let received = 0
        args.listen(channel, () => {
          if (firstArrival === null) firstArrival = performance.now()
          if (++received === count) resolve()
        })
      })
      const t0 = performance.now()
      void channel.send('go')
      await done
      return {
        instance,
        totalMs: performance.now() - t0,
        notes: firstArrival !== null ? `first arrival ${(firstArrival - t0).toFixed(1)}ms` : undefined,
      }
    },
    async cleanup({ channel }) {
      await channel.close()
    },
  }
}

// ── Broadcast-server-push factory ──────────────────────────────────────────
//
// The race we're fighting: `room.subscribe()` sends `sendBroadcastSubscribe` on the WS
// fire-and-forget; the trigger telefunc travels a separate HTTP connection. If HTTP
// arrives at the server before the subscribe frame, the server publishes before the page
// is registered as a peer — early messages are lost and `allReceived` deadlocks.
//
// Fix: in `setup`, after subscribing the page publishes one probe message itself and
// awaits its arrival on its own subscriber. Since publish + subscribe both travel the
// same WS in FIFO order, the probe's round-trip proves the server has registered our
// peer subscription before `measure` runs the real trigger.
//
// Bindings supply closures rather than a typed room — keeps the factory free of the
// text-vs-binary type parameter and avoids generic-inference ambiguity.

type BroadcastBindings = {
  waitOpen: () => Promise<void>
  subscribe: (cb: () => void) => () => void
  publishProbe: () => Promise<unknown>
}

type BroadcastSetup = {
  key: string
  runId: number
  dispatch: { fn: () => void }
  unsubscribe: () => void
}

function defineBroadcastServerPush(args: {
  name: string
  keyPrefix: string
  openRoom: (runId: number, key: string) => Promise<BroadcastBindings>
  trigger: (
    runId: number,
    key: string,
    count: number,
    bytes: number,
  ) => Promise<{ instance: string; elapsedMs: number }>
}): Scenario<BroadcastSetup> {
  return {
    name: args.name,
    async setup(_bytes, runId) {
      const key = `${args.keyPrefix}:${Math.random().toString(36).slice(2, 10)}`
      const room = await args.openRoom(runId, key)
      await room.waitOpen()

      const dispatch: { fn: () => void } = { fn: () => {} }
      let probeSeen = false
      let resolveProbe!: () => void
      const probed = new Promise<void>((resolve) => {
        resolveProbe = resolve
      })
      const unsubscribe = room.subscribe(() => {
        if (!probeSeen) {
          probeSeen = true
          resolveProbe()
          return
        }
        dispatch.fn()
      })
      await room.publishProbe()
      await probed

      return { key, runId, dispatch, unsubscribe }
    },
    async measure({ key, runId, dispatch }, { count, bytes }) {
      const allReceived = new Promise<void>((resolve) => {
        let received = 0
        dispatch.fn = () => {
          if (++received === count) resolve()
        }
      })
      const t0 = performance.now()
      const result = await args.trigger(runId, key, count, bytes)
      await allReceived
      return {
        instance: result.instance,
        totalMs: performance.now() - t0,
        notes: `server publish loop took ${result.elapsedMs}ms`,
      }
    },
    async cleanup({ unsubscribe }) {
      unsubscribe()
    },
  }
}

// ── Broadcast client publish+subscribe (one-off, page is both sides) ───────

type BroadcastClientSetup = {
  room: Awaited<ReturnType<typeof onBenchBroadcast>>['room']
  instance: string
  getStats: Awaited<ReturnType<typeof onBenchBroadcast>>['getStats']
  payload: string
  dispatch: { fn: (msg: { seq: number }) => void }
  unsubscribe: () => void
}

const broadcastClient: Scenario<BroadcastClientSetup> = {
  name: 'broadcast client publish+subscribe',
  async setup(bytes, runId) {
    const key = `bench:client:${Math.random().toString(36).slice(2, 10)}`
    const { room, instance, getStats } = await isolated(onBenchBroadcast, runId)(key)
    await new Promise<void>((resolve) => room.onOpen(resolve))
    const dispatch: { fn: (msg: { seq: number }) => void } = { fn: () => {} }
    const unsubscribe = room.subscribe((msg) => dispatch.fn(msg))
    return {
      room,
      instance,
      getStats,
      payload: 'x'.repeat(bytes),
      dispatch,
      unsubscribe,
    }
  },
  async measure({ room, instance, getStats, payload, dispatch }, { count }) {
    const sentAt = new Map<number, number>()
    const rtts: number[] = []
    const allReceived = new Promise<void>((resolve) => {
      let received = 0
      dispatch.fn = (msg) => {
        const t = sentAt.get(msg.seq)
        if (t !== undefined) rtts.push(performance.now() - t)
        if (++received === count) resolve()
      }
    })
    const t0 = performance.now()
    for (let i = 0; i < count; i++) {
      sentAt.set(i, performance.now())
      void room.publish({ seq: i, payload, sentAt: Date.now() })
    }
    await allReceived
    const stats = await getStats()
    return {
      instance,
      totalMs: performance.now() - t0,
      rtts,
      notes: `server-side received ${stats.count} on ${stats.instance}`,
    }
  },
  async cleanup({ unsubscribe }) {
    unsubscribe()
  },
}

// ── Concrete scenarios (factory bindings) ──────────────────────────────────

const channelTextEcho = defineChannelEcho({
  name: 'channel echo (text, ack)',
  open: async (runId) => {
    const { channel, instance } = await isolated(onBenchChannelEcho, runId)()
    return { channel, instance }
  },
  buildPayload: (bytes) => 'x'.repeat(bytes),
  send: (channel, payload) => channel.send({ seq: 0, payload }, { ack: true }),
})

const channelBinaryEcho = defineChannelEcho({
  name: 'channel echo (binary, ack)',
  open: async (runId) => {
    const { channel, instance } = await isolated(onBenchChannelBinaryEcho, runId)()
    return { channel, instance }
  },
  buildPayload: (bytes) => {
    const buf = new Uint8Array(bytes)
    for (let i = 0; i < bytes; i++) buf[i] = i & 0xff
    return buf
  },
  send: (channel, payload) => channel.sendBinary(payload, { ack: true }),
})

const channelTextServerPush = defineChannelServerPush({
  name: 'channel server push (text)',
  open: async (runId, count, bytes) => {
    const { channel, instance } = await isolated(onBenchChannelServerPush, runId)(count, bytes)
    return { channel, instance }
  },
  listen: (channel, cb) => {
    channel.listen(() => cb())
  },
})

const channelBinaryServerPush = defineChannelServerPush({
  name: 'channel server push (binary)',
  open: async (runId, count, bytes) => {
    const { channel, instance } = await isolated(onBenchChannelBinaryServerPush, runId)(count, bytes)
    return { channel, instance }
  },
  listen: (channel, cb) => {
    channel.listenBinary(() => cb())
  },
})

function broadcastTextServerPush(mode: 'sequential' | 'parallel') {
  return defineBroadcastServerPush({
    name: `broadcast server push, ${mode} (text)`,
    keyPrefix: `bench:srv-${mode}`,
    openRoom: async (runId, key) => {
      const { room } = await isolated(onBenchBroadcast, runId)(key)
      return {
        waitOpen: () => new Promise<void>((resolve) => room.onOpen(resolve)),
        subscribe: (cb) => room.subscribe(() => cb()),
        publishProbe: () => room.publish({ seq: -1, payload: '', sentAt: 0 }),
      }
    },
    trigger: (runId, key, count, bytes) => isolated(onBenchBroadcastServerPush, runId)(key, count, bytes, mode),
  })
}

function broadcastBinaryServerPush(mode: 'sequential' | 'parallel') {
  const probeBuf = new Uint8Array(1)
  return defineBroadcastServerPush({
    name: `broadcast server push, ${mode} (binary)`,
    keyPrefix: `bench:bin-${mode}`,
    openRoom: async (runId, key) => {
      const { room } = await isolated(onBenchBroadcastBinary, runId)(key)
      return {
        waitOpen: () => new Promise<void>((resolve) => room.onOpen(resolve)),
        subscribe: (cb) => room.subscribeBinary(() => cb()),
        publishProbe: () => room.publishBinary(probeBuf),
      }
    },
    trigger: (runId, key, count, bytes) => isolated(onBenchBroadcastBinaryServerPush, runId)(key, count, bytes, mode),
  })
}

const SCENARIOS: Array<Scenario<any>> = [
  channelTextEcho,
  channelBinaryEcho,
  channelTextServerPush,
  channelBinaryServerPush,
  broadcastClient,
  broadcastTextServerPush('sequential'),
  broadcastTextServerPush('parallel'),
  broadcastBinaryServerPush('sequential'),
  broadcastBinaryServerPush('parallel'),
]

// ── Warmup ─────────────────────────────────────────────────────────────────

/** Open `MAX_CONCURRENCY` isolated `ClientConnection`s in parallel and exercise every
 *  hot path on each (text echo, binary echo, broadcast pub/sub). Two jobs in one: WS
 *  handshakes finish here instead of inside a measured cell, and V8 has JIT'd every
 *  path before the matrix starts. One text channel per connection is kept open as a
 *  keepalive — without it the connection's TTL would fire between cells and we'd pay
 *  handshake cost again.
 *
 *  Then idle for `TRANSPORT_UPGRADE_SETTLE_MS` before returning. Channels start on SSE
 *  by default and asynchronously upgrade to WS in the background; without an idle
 *  window, the matrix's first cells run on the half-upgraded transport and skew the
 *  numbers. The settle gives every connection time to land on its final transport.
 *
 *  Returned closer tears the keepalive channels down at the end of the run. */
async function warmupConnections(): Promise<() => Promise<void>> {
  const binBuf = new Uint8Array(64)
  const keepalive = await Promise.all(
    Array.from({ length: MAX_CONCURRENCY }, async (_, runId) => {
      const { channel: textCh } = await isolated(onBenchChannelEcho, runId)()
      for (let i = 0; i < 10; i++) await textCh.send({ seq: i, payload: 'w' }, { ack: true })

      const { channel: binCh } = await isolated(onBenchChannelBinaryEcho, runId)()
      for (let i = 0; i < 10; i++) await binCh.sendBinary(binBuf, { ack: true })
      await binCh.close()

      const key = `bench:warmup:${runId}:${Math.random().toString(36).slice(2, 10)}`
      const { room } = await isolated(onBenchBroadcast, runId)(key)
      await new Promise<void>((resolve) => room.onOpen(resolve))
      for (let i = 0; i < 5; i++) await room.publish({ seq: i, payload: 'w', sentAt: Date.now() })

      return textCh
    }),
  )
  return async () => {
    await Promise.all(keepalive.map((c) => c.close().catch(() => {})))
  }
}

// ── Component ──────────────────────────────────────────────────────────────

function Bench() {
  const [running, setRunning] = useState<string | null>(null)
  const [results, setResults] = useState<Result[]>([])
  const [info, setInfo] = useState<{ instanceId: string; hasRedis: boolean } | null>(null)

  // Results accumulate in a ref during the run, flushed to state every 500ms — keeps
  // `setState`-driven re-renders out of the bench's hot path so the table render cost
  // doesn't bias timings.
  const resultsRef = useRef<Result[]>([])

  useEffect(() => {
    onBenchInfo().then(setInfo)
  }, [])

  function reset() {
    resultsRef.current = []
    setResults([])
  }

  function startFlusher(): () => void {
    let lastLen = 0
    const id = setInterval(() => {
      if (resultsRef.current.length !== lastLen) {
        lastLen = resultsRef.current.length
        setResults([...resultsRef.current])
      }
    }, 500)
    return () => {
      clearInterval(id)
      setResults([...resultsRef.current])
    }
  }

  /** Run one (scenario × size × concurrency) cell and push a single aggregated row.
   *  Setup, measure, and cleanup are each guarded by their own timeout — any phase
   *  hanging produces a row with a `notes` annotation and the matrix moves on. */
  async function runCell<R>(scenario: Scenario<R>, conc: number, bytes: number, sizeLabel: string) {
    const setups = await Promise.allSettled(
      Array.from({ length: conc }, (_, i) =>
        withTimeout(scenario.setup(bytes, i), CELL_TIMEOUT_MS, `${scenario.name}.setup`),
      ),
    )
    const live: Array<{ res: R; runId: number }> = []
    const setupFailures: string[] = []
    for (let i = 0; i < setups.length; i++) {
      const s = setups[i]!
      if (s.status === 'fulfilled') {
        live.push({ res: s.value, runId: i })
      } else {
        setupFailures.push(s.reason instanceof Error ? s.reason.message : String(s.reason))
      }
    }

    const outcomes: ScenarioOutcome[] = []
    await Promise.all(
      live.map(async ({ res }) => {
        const t0 = performance.now()
        try {
          outcomes.push(
            await withTimeout(
              scenario.measure(res, { count: COUNT, bytes }),
              CELL_TIMEOUT_MS,
              `${scenario.name} @ ${sizeLabel}`,
            ),
          )
        } catch (err) {
          outcomes.push({
            instance: '?',
            totalMs: performance.now() - t0,
            notes: err instanceof Error ? err.message : String(err),
          })
        }
      }),
    )

    pushAggregateRow({
      scenario: scenario.name,
      sizeLabel,
      conc,
      bytes,
      outcomes,
      setupFailures,
    })

    await Promise.all(
      live.map(({ res }) =>
        withTimeout(scenario.cleanup(res), CELL_TIMEOUT_MS, `${scenario.name}.cleanup`).catch((err) =>
          console.warn(`cleanup failed for ${scenario.name}:`, err),
        ),
      ),
    )
  }

  /** Aggregate `outcomes` from all runs of one cell into a single Result row.
   *
   *   msg/s  = (runs × COUNT) / max(totalMs/1000)   — system throughput across the cell
   *   MB/s   = same, weighted by payload size
   *   p50/p99 = percentiles over the pooled per-message RTTs from every run combined
   *   notes  = setup failures, measure timeouts, multi-instance distribution, etc. */
  function pushAggregateRow(args: {
    scenario: string
    sizeLabel: string
    conc: number
    bytes: number
    outcomes: ScenarioOutcome[]
    setupFailures: string[]
  }) {
    const { scenario, sizeLabel, conc, bytes, outcomes, setupFailures } = args
    const noteParts: string[] = []
    if (setupFailures.length > 0) {
      noteParts.push(`${setupFailures.length}/${conc} setup failures: ${setupFailures[0]}`)
    }
    const measureNotes = [...new Set(outcomes.map((o) => o.notes).filter(Boolean) as string[])]
    if (measureNotes.length > 0) noteParts.push(measureNotes.join(' · '))
    const instances = [...new Set(outcomes.map((o) => o.instance).filter((x) => x !== '?'))]
    if (instances.length > 1) noteParts.push(`instances: ${instances.join(',')}`)

    if (outcomes.length === 0) {
      resultsRef.current.push({
        scenario,
        size: sizeLabel,
        concurrency: conc,
        msgPerSec: 0,
        mbPerSec: 0,
        notes: noteParts.join(' · ') || 'no runs completed',
      })
      return
    }

    const totalMessages = outcomes.length * COUNT
    const maxTotalMs = Math.max(...outcomes.map((o) => o.totalMs))
    const pooled = outcomes.flatMap((o) => o.rtts ?? [])
    const seconds = maxTotalMs / 1000
    const r: Result = {
      scenario,
      size: sizeLabel,
      concurrency: conc,
      msgPerSec: seconds > 0 ? Math.round(totalMessages / seconds) : 0,
      mbPerSec: seconds > 0 ? +((totalMessages * bytes) / 1024 / 1024 / seconds).toFixed(2) : 0,
      notes: noteParts.length > 0 ? noteParts.join(' · ') : undefined,
    }
    if (pooled.length > 0) {
      const { p50, p99 } = summarize(pooled)
      r.p50 = +p50.toFixed(2)
      r.p99 = +p99.toFixed(2)
    }
    resultsRef.current.push(r)
  }

  async function runMatrix() {
    reset()
    const stop = startFlusher()
    let close: (() => Promise<void>) | null = null
    try {
      setRunning(`warmup (${MAX_CONCURRENCY} connections × all code paths)`)
      close = await warmupConnections()
      for (const conc of CONCURRENCY_LEVELS) {
        for (const scenario of SCENARIOS) {
          for (const { label, bytes } of SIZES) {
            setRunning(`conc ${conc}: ${scenario.name} @ ${label}`)
            await runCell(scenario, conc, bytes, label)
          }
        }
      }
    } finally {
      if (close) await close()
      setRunning(null)
      stop()
    }
  }

  // ── JSX ──────────────────────────────────────────────────────────────────

  return (
    <div className="p-6 max-w-5xl">
      <h1 className="text-xl font-semibold mb-4">Channel + Broadcast Benchmark</h1>

      {info && (
        <div className="text-xs text-zinc-500 mb-4">
          Server: <code>INSTANCE_ID={info.instanceId}</code>
          {' · '}
          {info.hasRedis ? 'Redis substrate' : 'in-memory substrate'}
          {' · '}
          {COUNT} messages × sizes [{SIZES.map((s) => s.label).join(', ')}] · concurrency [
          {CONCURRENCY_LEVELS.join(', ')}] · {MAX_CONCURRENCY} isolated connections · per-cell timeout{' '}
          {CELL_TIMEOUT_MS / 1000}s
        </div>
      )}

      <div className="flex flex-wrap gap-2 mb-6">
        <button
          id="bench-run"
          onClick={runMatrix}
          disabled={running !== null}
          className="px-4 py-1.5 text-sm font-medium bg-zinc-900 text-white rounded hover:bg-zinc-800 disabled:opacity-50"
        >
          Run benchmark
        </button>
        <button
          onClick={reset}
          disabled={running !== null}
          className="px-3 py-1.5 text-sm text-zinc-500 hover:text-zinc-900 disabled:opacity-50"
        >
          Clear
        </button>
      </div>

      {running && <div className="text-xs text-zinc-500 mb-4">Running {running}…</div>}

      <table className="w-full text-xs">
        <thead className="text-left text-zinc-500">
          <tr>
            <th className="py-2 pr-4">Scenario</th>
            <th className="py-2 pr-4">Size</th>
            <th className="py-2 pr-4">Conc</th>
            <th className="py-2 pr-4 text-right">msg/s</th>
            <th className="py-2 pr-4 text-right">MB/s</th>
            <th className="py-2 pr-4 text-right">p50 ms</th>
            <th className="py-2 pr-4 text-right">p99 ms</th>
            <th className="py-2 pr-4">Notes</th>
          </tr>
        </thead>
        <tbody>
          {results.map((r, i) => (
            <tr key={i} className="border-t border-zinc-100">
              <td className="py-2 pr-4">{r.scenario}</td>
              <td className="py-2 pr-4">{r.size}</td>
              <td className="py-2 pr-4">{r.concurrency}</td>
              <td className="py-2 pr-4 text-right tabular-nums">{r.msgPerSec.toLocaleString()}</td>
              <td className="py-2 pr-4 text-right tabular-nums">{r.mbPerSec}</td>
              <td className="py-2 pr-4 text-right tabular-nums">{r.p50 ?? '—'}</td>
              <td className="py-2 pr-4 text-right tabular-nums">{r.p99 ?? '—'}</td>
              <td className="py-2 pr-4 text-zinc-500">{r.notes ?? ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
