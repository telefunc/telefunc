export { Bench }

import React, { useRef, useState } from 'react'
import { withContext, close } from 'telefunc/client'
import {
  onBenchBinaryEchoLatency,
  onBenchBinaryServerPush,
  onBenchBinaryThroughput,
  onBenchEchoLatency,
  onBenchInfo,
  onBenchServerPush,
  onBenchTextThroughput,
} from './Bench.telefunc'

// ── Configuration ──────────────────────────────────────────────────────────

const LATENCY_SAMPLES = 40
// Start at 16KB — sub-16KB SSE latency is dominated by HTTP-per-message overhead (~15ms),
// not transmission time, making small sizes uninformative.
const LATENCY_SIZES = [16384, 65536, 262144]

const THROUGHPUT_SIZES = [64, 4096, 32768, 131072, 524288]
const INSTANCE_COUNTS = [1, 3, 6] as const
const INSTANCE_LETTERS = ['a', 'b', 'c', 'd', 'e', 'f'] as const

// Pure-direction throughput timing. Each phase runs N short bursts with
// rest gaps in between — Chrome's WS internal queue drains during rest, so
// peak in-flight stays small and the UI stays responsive. Workers stop &
// restart per burst (credit window persists across bursts since channel is
// kept open, so subsequent bursts skip the credit ramp). Median of the
// burst rates is reported.
// Adaptive plateau detection. Workers run continuously; rate is sampled every
// SAMPLE_MS. Plateau = the rate stagnates within PLATEAU_TOLERANCE of its mean
// FOR A CONTINUOUS PLATEAU_HOLD_MS WINDOW. During BDP ramp the samples are
// rising (each doubling shifts the rate up), so the hold-window naturally
// rejects mid-ramp readings — the plateau can't form until the ramp completes
// and the system is at steady state. That's the entire principle; no extra
// "min measure" knob needed.
const SAMPLE_MS = 100
const PLATEAU_HOLD_MS = 500
const PLATEAU_SAMPLES = Math.max(2, Math.ceil(PLATEAU_HOLD_MS / SAMPLE_MS))
// 10% — tight enough that a single BDP-window doubling (≈17% rate-step) breaks
// the hold and re-arms the detector, so plateau can't be declared while the
// credit windows are still ramping.
const PLATEAU_TOLERANCE = 0.1
// Warmup: discard samples for the first `WARMUP_MS` so the BDP credit windows
// have time to ramp before the plateau detector starts looking. The cold first
// run after page reload is the worst case — JIT compilation + module wiring
// drags down the first RTT's BDP sample below the saturation threshold
// (`sample*3 ≥ window*2`), so the window stays stuck at the initial 64KB / 50
// msgs and the bench reports the credit-bound rate instead of the steady-state
// one. Sized to cover the observed 4–5s cold ramp with margin.
const WARMUP_MS = 3_000
const MAX_MEASURE_MS = 5_000
const INTER_PHASE_MS = 200
const DRAIN_DEADLINE_MS = 800

const PUSH_COUNT = 60
const PUSH_SIZES = [64, 4096, 32768, 131072, 524288]

// ── Scenarios ──────────────────────────────────────────────────────────────
// One entry = one card. Comment / reorder / mix freely. Each card also gets
// a "Run" button to execute that single scenario in isolation.

type Transport = 'sse' | 'ws'
type Routing = 'rr' | 'pinned'

type LatencyScenario = {
  id: string
  label: string
  transport: Transport
  binary: boolean
  kind: 'latency'
}
type RoutedScenario = {
  id: string
  label: string
  transport: Transport
  binary: boolean
  kind: 'throughput' | 'push'
  routing: Routing
}
type Scenario = LatencyScenario | RoutedScenario

const SCENARIOS: Scenario[] = [
  { id: 'latency-text-ws', label: 'Echo Latency (text)', transport: 'ws', binary: false, kind: 'latency' },
  { id: 'latency-text-sse', label: 'Echo Latency (text)', transport: 'sse', binary: false, kind: 'latency' },
  { id: 'latency-bin-ws', label: 'Echo Latency (binary)', transport: 'ws', binary: true, kind: 'latency' },
  { id: 'latency-bin-sse', label: 'Echo Latency (binary)', transport: 'sse', binary: true, kind: 'latency' },

  {
    id: 'tput-text-ws-rr',
    label: 'Throughput (text)',
    transport: 'ws',
    binary: false,
    kind: 'throughput',
    routing: 'rr',
  },
  {
    id: 'tput-text-ws-pin',
    label: 'Throughput (text)',
    transport: 'ws',
    binary: false,
    kind: 'throughput',
    routing: 'pinned',
  },
  {
    id: 'tput-text-sse-rr',
    label: 'Throughput (text)',
    transport: 'sse',
    binary: false,
    kind: 'throughput',
    routing: 'rr',
  },
  {
    id: 'tput-text-sse-pin',
    label: 'Throughput (text)',
    transport: 'sse',
    binary: false,
    kind: 'throughput',
    routing: 'pinned',
  },

  {
    id: 'tput-bin-ws-rr',
    label: 'Throughput (binary)',
    transport: 'ws',
    binary: true,
    kind: 'throughput',
    routing: 'rr',
  },
  {
    id: 'tput-bin-ws-pin',
    label: 'Throughput (binary)',
    transport: 'ws',
    binary: true,
    kind: 'throughput',
    routing: 'pinned',
  },
  {
    id: 'tput-bin-sse-rr',
    label: 'Throughput (binary)',
    transport: 'sse',
    binary: true,
    kind: 'throughput',
    routing: 'rr',
  },
  {
    id: 'tput-bin-sse-pin',
    label: 'Throughput (binary)',
    transport: 'sse',
    binary: true,
    kind: 'throughput',
    routing: 'pinned',
  },

  { id: 'push-text-ws-rr', label: 'Server Push (text)', transport: 'ws', binary: false, kind: 'push', routing: 'rr' },
  {
    id: 'push-text-ws-pin',
    label: 'Server Push (text)',
    transport: 'ws',
    binary: false,
    kind: 'push',
    routing: 'pinned',
  },
  { id: 'push-text-sse-rr', label: 'Server Push (text)', transport: 'sse', binary: false, kind: 'push', routing: 'rr' },
  {
    id: 'push-text-sse-pin',
    label: 'Server Push (text)',
    transport: 'sse',
    binary: false,
    kind: 'push',
    routing: 'pinned',
  },

  { id: 'push-bin-ws-rr', label: 'Server Push (binary)', transport: 'ws', binary: true, kind: 'push', routing: 'rr' },
  {
    id: 'push-bin-ws-pin',
    label: 'Server Push (binary)',
    transport: 'ws',
    binary: true,
    kind: 'push',
    routing: 'pinned',
  },
  { id: 'push-bin-sse-rr', label: 'Server Push (binary)', transport: 'sse', binary: true, kind: 'push', routing: 'rr' },
  {
    id: 'push-bin-sse-pin',
    label: 'Server Push (binary)',
    transport: 'sse',
    binary: true,
    kind: 'push',
    routing: 'pinned',
  },
]

// ── Run state ──────────────────────────────────────────────────────────────

type CellStatus = 'pending' | 'running' | 'done'

type LatencyCell = { bytes: number; status: CellStatus; p50?: number; p99?: number; instance?: string }

// Push: one-way (server → client only), one rate per cell.
type RateCell = { bytes: number; status: CellStatus; msgS?: number; mbps?: number; instances?: string[] }
type RateGroup = { instances: number; cells: RateCell[] }

// Throughput: pure-direction. TX = client→server (server-counted), RX = server→client (client-counted).
// Measured in two independent sequential phases — neither involves an echo round-trip.
type Direction = { msgS: number; mbps: number }
type ThroughputCell = { bytes: number; status: CellStatus; tx?: Direction; rx?: Direction; instances?: string[] }
type ThroughputGroup = { instances: number; cells: ThroughputCell[] }

type RunState = {
  scenario: Scenario
  status: 'pending' | 'running' | 'done' | 'error'
  error?: string
  latency?: LatencyCell[]
  throughput?: ThroughputGroup[]
  push?: RateGroup[]
}

function makeRunState(scenario: Scenario): RunState {
  if (scenario.kind === 'latency') {
    return {
      scenario,
      status: 'pending',
      latency: LATENCY_SIZES.map((bytes) => ({ bytes, status: 'pending' })),
    }
  }
  if (scenario.kind === 'throughput') {
    return {
      scenario,
      status: 'pending',
      throughput: INSTANCE_COUNTS.map((instances) => ({
        instances,
        cells: THROUGHPUT_SIZES.map((bytes) => ({ bytes, status: 'pending' as CellStatus })),
      })),
    }
  }
  return {
    scenario,
    status: 'pending',
    push: INSTANCE_COUNTS.map((instances) => ({
      instances,
      cells: PUSH_SIZES.map((bytes) => ({ bytes, status: 'pending' as CellStatus })),
    })),
  }
}

// ── Connection helpers ─────────────────────────────────────────────────────

function makeUrl(scenario: Scenario, instances: number, slot: number): string {
  if (scenario.kind === 'latency') return `/_telefunc?bench_instances=1`
  return scenario.routing === 'pinned'
    ? `/_telefunc?bench_instance=${INSTANCE_LETTERS[slot]}`
    : `/_telefunc?bench_instances=${instances}`
}

function openWith<F extends (...args: never[]) => unknown>(
  fn: F,
  scenario: Scenario,
  opts: { instances?: number; slot?: number; bytes?: number } = {},
): F {
  const { instances = 1, slot = 0, bytes = 0 } = opts
  return withContext(fn, {
    telefuncUrl: makeUrl(scenario, instances, slot),
    channel: {
      transports: [scenario.transport],
      connectionKey: `bench-${scenario.id}-${bytes}b-ch${slot}`,
      idleTimeout: 0,
    },
  }) as F
}

// ── Timing helper ──────────────────────────────────────────────────────────

function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    if (signal.aborted) {
      reject(new DOMException('Aborted', 'AbortError'))
      return
    }
    const onAbort = () => {
      clearTimeout(t)
      reject(new DOMException('Aborted', 'AbortError'))
    }
    const t = setTimeout(() => {
      signal.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    signal.addEventListener('abort', onAbort, { once: true })
  })
}

// ── Measurement functions ──────────────────────────────────────────────────

async function measureLatencyCell(
  scenario: LatencyScenario,
  bytes: number,
  signal: AbortSignal,
): Promise<{ p50: number; p99: number; instance: string }> {
  const result = scenario.binary
    ? await openWith(onBenchBinaryEchoLatency, scenario)()
    : await openWith(onBenchEchoLatency, scenario)()
  const rtts: number[] = []
  try {
    for (let i = 0; i < LATENCY_SAMPLES; i++) {
      if (signal.aborted) break
      const t0 = performance.now()
      if (scenario.binary) {
        await (result as Awaited<ReturnType<typeof onBenchBinaryEchoLatency>>).channel.sendBinary(
          new Uint8Array(bytes).fill(0xab),
          { ack: true },
        )
      } else {
        await (result as Awaited<ReturnType<typeof onBenchEchoLatency>>).channel.send(
          { seq: i, payload: 'x'.repeat(bytes) },
          { ack: true },
        )
      }
      rtts.push(performance.now() - t0)
    }
  } finally {
    await result.channel.close()
  }
  rtts.sort((a, b) => a - b)
  return {
    p50: rtts[Math.floor(rtts.length * 0.5)] ?? 0,
    p99: rtts[Math.floor(rtts.length * 0.99)] ?? 0,
    instance: result.instance,
  }
}

/** Adaptive plateau detector. Samples `getCount()` every `SAMPLE_MS`, computes
 *  rolling rates, and returns the steady-state rate as soon as the last
 *  `PLATEAU_SAMPLES` rates fit within `PLATEAU_TOLERANCE` of their mean.
 *
 *  Rationale: the BDP credit window ramps adaptively (up to 11 doublings, ≥50ms
 *  each = 550ms), and the ramp speed depends on host CPU / payload size / wire
 *  RTT. A fixed warmup either over-shoots (slow benches) or under-shoots
 *  (measures during ramp → noisy, non-monotonic results). Detecting the plateau
 *  directly is the correct abstraction.
 *
 *  Returns 0 if no signal at all (`getCount` never advanced).
 *  Times out at `MAX_MEASURE_MS` and returns the mean of the last samples. */
async function measureRate(getCount: () => number, signal: AbortSignal): Promise<number> {
  // Workers are already running before we're called; let them burn `WARMUP_MS`
  // so the BDP estimator has time to ramp the credit window. Sampling that
  // pre-ramp window pollutes plateau detection (cold rates look stable
  // because the window is stuck, not because steady state has been reached).
  await delay(WARMUP_MS, signal)
  const start = performance.now()
  const samples: number[] = []
  let prevCount = getCount()
  let prevAt = start

  while (true) {
    if (signal.aborted) throw new DOMException('Aborted', 'AbortError')
    await delay(SAMPLE_MS, signal)

    const count = getCount()
    const at = performance.now()
    const rate = (count - prevCount) / ((at - prevAt) / 1000)
    samples.push(rate)
    prevCount = count
    prevAt = at

    const elapsed = at - start
    if (samples.length >= PLATEAU_SAMPLES) {
      const recent = samples.slice(-PLATEAU_SAMPLES)
      const mean = recent.reduce((a, b) => a + b, 0) / recent.length
      if (mean > 0) {
        let min = Infinity
        let max = 0
        for (const r of recent) {
          if (r < min) min = r
          if (r > max) max = r
        }
        // Plateau iff the spread of recent rates is small relative to the mean.
        if ((max - min) / mean <= PLATEAU_TOLERANCE) return mean
      }
    }
    if (elapsed >= MAX_MEASURE_MS) {
      // Timed out without plateau. Scan all samples for the contiguous
      // `PLATEAU_SAMPLES`-wide window with the smallest relative spread —
      // i.e., the most plateau-like region the bench actually saw, regardless
      // of where it appeared. Better than blindly averaging the tail.
      return bestPlateauWindow(samples, PLATEAU_SAMPLES)
    }
  }
}

function bestPlateauWindow(samples: number[], k: number): number {
  if (samples.length === 0) return 0
  if (samples.length < k) return samples.reduce((a, b) => a + b, 0) / samples.length
  let bestMean = 0
  let bestSpread = Infinity
  for (let i = 0; i + k <= samples.length; i++) {
    let sum = 0
    let min = Infinity
    let max = 0
    for (let j = i; j < i + k; j++) {
      const r = samples[j]!
      sum += r
      if (r < min) min = r
      if (r > max) max = r
    }
    const mean = sum / k
    if (mean <= 0) continue
    const spread = (max - min) / mean
    if (spread < bestSpread) {
      bestSpread = spread
      bestMean = mean
    }
  }
  // Every window had `mean <= 0` (no signal at all) — fall back to the
  // global mean so the caller still gets *something* non-NaN.
  if (bestSpread === Infinity) return samples.reduce((a, b) => a + b, 0) / samples.length
  return bestMean
}

async function measureThroughputCell(
  scenario: RoutedScenario,
  instances: number,
  bytes: number,
  signal: AbortSignal,
): Promise<{ tx: Direction; rx: Direction; instances: string[] } | null> {
  const sum = (arr: number[]) => arr.reduce((a, b) => a + b, 0)
  const toDir = (msgS: number): Direction => ({
    msgS: Math.round(msgS),
    mbps: +((msgS * bytes) / (1024 * 1024)).toFixed(2),
  })

  const opens = await Promise.all(
    Array.from({ length: instances }, (_, i) =>
      openWith(scenario.binary ? onBenchBinaryThroughput : onBenchTextThroughput, scenario, {
        instances,
        slot: i,
        bytes,
      })(bytes),
    ),
  )
  const instanceList = opens.map((o) => o.instance)
  const binaryPayload = scenario.binary ? new Uint8Array(bytes).fill(0xab) : null
  const textPayload = scenario.binary ? null : 'x'.repeat(bytes)

  // Wait for `getServerReceived` to stop growing, or for the deadline.
  async function drain(): Promise<void> {
    let prev = -1
    const deadline = performance.now() + DRAIN_DEADLINE_MS
    while (performance.now() < deadline) {
      await delay(INTER_PHASE_MS, signal)
      const cur = sum(await Promise.all(opens.map((o) => o.getServerReceived())))
      if (cur === prev) break
      prev = cur
    }
  }

  try {
    // ── Phase TX: client floods, server counts (no echoes). Workers run
    // continuously while `measureRate` samples the local `sent` counter every
    // SAMPLE_MS and waits for the rate to plateau — i.e., for the BDP credit
    // window to finish ramping and the system to reach steady state.
    //
    // Local counter rather than `getServerReceived` RPC: under WS saturation,
    // closure-RPC queues for seconds. Credit-window backpressure enforces that
    // completed-send-rate == server-consume-rate in steady state, so the local
    // count is the right signal.
    let txActive = true
    let sent = 0
    const txWorkers = opens.map((r) =>
      (async () => {
        try {
          while (txActive) {
            if (binaryPayload) await r.channel.sendBinary(binaryPayload)
            else await r.channel.send({ seq: 0, payload: textPayload! })
            sent++
          }
        } catch {}
      })(),
    )
    const txMsgS = await measureRate(() => sent, signal)
    txActive = false
    await Promise.all(txWorkers)

    // Drain backlog before flipping to RX so `{ctrl:'start'}` isn't queued
    // behind in-flight TX data.
    await drain()

    // ── Phase RX: server pushes fire-and-forget, client counts. Same plateau-
    // detection on the receive-side counter.
    let clientReceived = 0
    const offs = scenario.binary
      ? opens.map((r) =>
          r.channel.listenBinary(() => {
            clientReceived++
          }),
        )
      : opens.map((r) =>
          r.channel.listen(() => {
            clientReceived++
          }),
        )
    await Promise.all(opens.map((r) => r.channel.send({ ctrl: 'start' })))
    const rxMsgS = await measureRate(() => clientReceived, signal)
    await Promise.all(opens.map((r) => r.channel.send({ ctrl: 'stop' }).catch(() => {})))
    for (const off of offs) off()

    return { tx: toDir(txMsgS), rx: toDir(rxMsgS), instances: instanceList }
  } catch {
    return null
  } finally {
    await Promise.all(opens.map(close))
  }
}

async function measurePushCell(
  scenario: RoutedScenario,
  instances: number,
  bytes: number,
  signal: AbortSignal,
): Promise<{ msgS: number; mbps: number; instances: string[] } | null> {
  const totalExpected = instances * PUSH_COUNT
  let received = 0
  let resolveDone!: () => void
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve
  })
  const onReceive = () => {
    if (++received === totalExpected) resolveDone()
  }
  const aborted = new Promise<boolean>((resolve) =>
    signal.addEventListener('abort', () => resolve(true), { once: true }),
  )

  if (scenario.binary) {
    const opens = await Promise.all(
      Array.from({ length: instances }, (_, slot) =>
        openWith(onBenchBinaryServerPush, scenario, { instances, slot, bytes })(PUSH_COUNT, bytes),
      ),
    )
    const instanceList = opens.map((o) => o.instance)
    for (const r of opens) r.channel.listenBinary(onReceive)
    const t0 = performance.now()
    for (const r of opens) void r.channel.send('go')
    const wasAborted = await Promise.race([done.then(() => false), aborted])
    const elapsed = performance.now() - t0
    for (const r of opens) await r.channel.close()
    if (wasAborted) return null
    const msgS = (totalExpected / elapsed) * 1000
    return { msgS: Math.round(msgS), mbps: +((msgS * bytes) / (1024 * 1024)).toFixed(2), instances: instanceList }
  } else {
    const opens = await Promise.all(
      Array.from({ length: instances }, (_, slot) =>
        openWith(onBenchServerPush, scenario, { instances, slot, bytes })(PUSH_COUNT, bytes),
      ),
    )
    const instanceList = opens.map((o) => o.instance)
    for (const r of opens) r.channel.listen(onReceive)
    const t0 = performance.now()
    for (const r of opens) void r.channel.send('go')
    const wasAborted = await Promise.race([done.then(() => false), aborted])
    const elapsed = performance.now() - t0
    for (const r of opens) await r.channel.close()
    if (wasAborted) return null
    const msgS = (totalExpected / elapsed) * 1000
    return { msgS: Math.round(msgS), mbps: +((msgS * bytes) / (1024 * 1024)).toFixed(2), instances: instanceList }
  }
}

// ── Per-scenario execution ─────────────────────────────────────────────────

function patchThroughputCell(r: RunState, instances: number, bytes: number, update: Partial<ThroughputCell>): RunState {
  return {
    ...r,
    throughput: r.throughput!.map((g) =>
      g.instances === instances
        ? { ...g, cells: g.cells.map((c) => (c.bytes === bytes ? { ...c, ...update } : c)) }
        : g,
    ),
  }
}

function patchPushCell(r: RunState, instances: number, bytes: number, update: Partial<RateCell>): RunState {
  return {
    ...r,
    push: r.push!.map((g) =>
      g.instances === instances
        ? { ...g, cells: g.cells.map((c) => (c.bytes === bytes ? { ...c, ...update } : c)) }
        : g,
    ),
  }
}

async function executeScenario(
  scenario: Scenario,
  patch: (updater: (r: RunState) => RunState) => void,
  signal: AbortSignal,
): Promise<void> {
  if (scenario.kind === 'latency') {
    for (const bytes of LATENCY_SIZES) {
      if (signal.aborted) break
      patch((r) => ({
        ...r,
        latency: r.latency!.map((c) => (c.bytes === bytes ? { ...c, status: 'running' } : c)),
      }))
      const result = await measureLatencyCell(scenario, bytes, signal).catch(() => null)
      patch((r) => ({
        ...r,
        latency: r.latency!.map((c) => (c.bytes === bytes ? { ...c, status: 'done', ...(result ?? {}) } : c)),
      }))
    }
    return
  }

  if (scenario.kind === 'throughput') {
    for (const instances of INSTANCE_COUNTS) {
      for (const bytes of THROUGHPUT_SIZES) {
        if (signal.aborted) break
        patch((r) => patchThroughputCell(r, instances, bytes, { status: 'running' }))
        const result = await measureThroughputCell(scenario, instances, bytes, signal).catch(() => null)
        patch((r) => patchThroughputCell(r, instances, bytes, { status: 'done', ...(result ?? {}) }))
      }
    }
    return
  }

  // push
  for (const instances of INSTANCE_COUNTS) {
    for (const bytes of PUSH_SIZES) {
      if (signal.aborted) break
      patch((r) => patchPushCell(r, instances, bytes, { status: 'running' }))
      const result = await measurePushCell(scenario, instances, bytes, signal).catch(() => null)
      patch((r) => patchPushCell(r, instances, bytes, { status: 'done', ...(result ?? {}) }))
    }
  }
}

// ── UI helpers ─────────────────────────────────────────────────────────────

function fmtBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}KB`
  return `${(bytes / (1024 * 1024)).toFixed(0)}MB`
}

function fmtMs(ms: number): string {
  return ms < 1 ? `${(ms * 1000).toFixed(0)}μs` : `${ms.toFixed(1)}ms`
}

function fmtNum(n: number): string {
  return n >= 1_000_000 ? `${(n / 1_000_000).toFixed(2)}M` : n >= 1_000 ? `${(n / 1_000).toFixed(1)}k` : String(n)
}

function instancesHit(groups: { cells: { instances?: string[] }[] }[] | undefined): string {
  if (!groups) return ''
  const seen = new Set<string>()
  for (const g of groups) for (const c of g.cells) if (c.instances) for (const i of c.instances) seen.add(i)
  return [...seen].sort().join('')
}

// Project a ThroughputGroup[] onto one direction so the existing RateScalingTable
// renderer can stay direction-agnostic.
function toDirectionView(groups: ThroughputGroup[], dir: 'tx' | 'rx'): RateGroup[] {
  return groups.map((g) => ({
    instances: g.instances,
    cells: g.cells.map((c) => ({
      bytes: c.bytes,
      status: c.status,
      msgS: c[dir]?.msgS,
      mbps: c[dir]?.mbps,
      instances: c.instances,
    })),
  }))
}

function Spinner() {
  return <span className="animate-pulse text-zinc-300">…</span>
}

function CopyJsonButton({ data }: { data: unknown }) {
  const [copied, setCopied] = useState(false)
  async function copy() {
    try {
      await navigator.clipboard.writeText(JSON.stringify(data, null, 2))
      setCopied(true)
      setTimeout(() => setCopied(false), 1200)
    } catch {}
  }
  return (
    <button
      onClick={copy}
      title="Copy this table as JSON"
      className="text-[10px] px-1.5 py-0.5 rounded border border-zinc-200 text-zinc-500 hover:bg-zinc-50"
    >
      {copied ? '✓ copied' : 'copy json'}
    </button>
  )
}

function TableHeader({ label, data }: { label: string; data: unknown }) {
  return (
    <div className="flex items-center justify-between text-xs text-zinc-400 mt-2 mb-0.5">
      <span className="font-medium">{label}</span>
      <CopyJsonButton data={data} />
    </div>
  )
}

// ── Cards ──────────────────────────────────────────────────────────────────

function StatusBadge({ status }: { status: RunState['status'] }) {
  const cls =
    status === 'running'
      ? 'bg-blue-100 text-blue-700'
      : status === 'done'
        ? 'bg-green-100 text-green-700'
        : status === 'error'
          ? 'bg-red-100 text-red-700'
          : 'bg-zinc-100 text-zinc-500'
  return <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${cls}`}>{status}</span>
}

function CellButton({
  children,
  onRun,
  busy,
  className = '',
}: {
  children: React.ReactNode
  onRun: () => void
  busy: boolean
  className?: string
}) {
  return (
    <td
      role="button"
      tabIndex={busy ? -1 : 0}
      onClick={() => {
        if (!busy) onRun()
      }}
      onKeyDown={(e) => {
        if (!busy && (e.key === 'Enter' || e.key === ' ')) {
          e.preventDefault()
          onRun()
        }
      }}
      title="Click to run this cell"
      className={`py-1 text-right font-mono ${busy ? 'cursor-not-allowed opacity-60' : 'cursor-pointer hover:bg-zinc-50'} ${className}`}
    >
      {children}
    </td>
  )
}

function LatencyTable({
  cells,
  busy,
  onCellRun,
}: {
  cells: LatencyCell[]
  busy: boolean
  onCellRun: (bytes: number) => void
}) {
  const done = cells.filter((c) => c.p50 != null)
  const minP50 = done.length ? Math.min(...done.map((c) => c.p50!)) : -1
  const minP99 = done.length ? Math.min(...done.map((c) => c.p99!)) : -1
  return (
    <table className="w-full text-xs mt-2 table-fixed">
      <thead>
        <tr className="text-zinc-400 text-left">
          <th className="pb-1 w-14">size</th>
          <th className="pb-1 text-right">p50</th>
          <th className="pb-1 text-right">p99</th>
        </tr>
      </thead>
      <tbody>
        {cells.map((c) => (
          <tr key={c.bytes} className="border-t border-zinc-100">
            <td className="py-1 text-zinc-500">{fmtBytes(c.bytes)}</td>
            <CellButton
              busy={busy}
              onRun={() => onCellRun(c.bytes)}
              className={c.p50 != null && c.p50 === minP50 ? 'font-semibold text-emerald-700' : ''}
            >
              {c.status === 'running' ? <Spinner /> : c.p50 != null ? fmtMs(c.p50) : '—'}
            </CellButton>
            <CellButton
              busy={busy}
              onRun={() => onCellRun(c.bytes)}
              className={c.p99 != null && c.p99 === minP99 ? 'font-semibold text-emerald-700' : ''}
            >
              {c.status === 'running' ? <Spinner /> : c.p99 != null ? fmtMs(c.p99) : '—'}
            </CellButton>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function RateScalingTable({
  data,
  sizes,
  busy,
  onCellRun,
}: {
  data: RateGroup[]
  sizes: number[]
  busy: boolean
  onCellRun: (instances: number, bytes: number) => void
}) {
  const base = data[0]
  const top = data[data.length - 1]

  return (
    <table className="w-full text-xs mt-2 table-fixed">
      <thead>
        <tr className="text-zinc-400 text-left">
          <th className="pb-1 w-14">size</th>
          {data.map((d) => (
            <th key={d.instances} className="pb-1 text-right">
              {d.instances}i
            </th>
          ))}
          <th className="pb-1 text-right w-12">scale</th>
        </tr>
      </thead>
      <tbody>
        {sizes.map((bytes) => {
          const cells = data.map((d) => d.cells.find((c) => c.bytes === bytes)!)
          const rowMsgSPeak = Math.max(0, ...cells.filter((c) => c?.msgS != null).map((c) => c.msgS!))
          const rowMbpsPeak = Math.max(0, ...cells.filter((c) => c?.mbps != null).map((c) => c.mbps!))
          const baseCell = base?.cells.find((c) => c.bytes === bytes)
          const topCell = top?.cells.find((c) => c.bytes === bytes)
          const scale =
            baseCell?.msgS != null && topCell?.msgS != null && data.length > 1
              ? (topCell.msgS / baseCell.msgS).toFixed(1)
              : undefined
          return (
            <tr key={bytes} className="border-t border-zinc-100">
              <td className="py-1 text-zinc-500">{fmtBytes(bytes)}</td>
              {cells.map((cell, i) => {
                const isBestMsgS = cell?.msgS != null && cell.msgS > 0 && cell.msgS === rowMsgSPeak
                const isBestMbps = cell?.mbps != null && cell.mbps > 0 && cell.mbps === rowMbpsPeak
                const instances = data[i]!.instances
                return (
                  <CellButton key={i} busy={busy} onRun={() => onCellRun(instances, bytes)}>
                    <div className={isBestMsgS ? 'font-semibold text-emerald-700' : ''}>
                      {cell?.status === 'running' ? <Spinner /> : cell?.msgS != null ? fmtNum(cell.msgS) : '—'}
                    </div>
                    <div className={isBestMbps ? 'font-semibold text-emerald-700' : 'text-zinc-500'}>
                      {cell?.mbps != null ? `${cell.mbps} MB/s` : ' '}
                    </div>
                  </CellButton>
                )
              })}
              <td className="py-1 text-right font-mono text-zinc-500">{scale != null ? `${scale}×` : '—'}</td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}

function RoutingBadge({ scenario }: { scenario: Scenario }) {
  if (scenario.kind === 'latency') return null
  const label = scenario.routing === 'pinned' ? 'pinned' : 'round-robin'
  return (
    <span className="text-[10px] uppercase tracking-wide px-1.5 py-0.5 rounded bg-zinc-100 text-zinc-600 font-mono">
      {label}
    </span>
  )
}

function RunCard({
  run,
  busy,
  onRunOne,
  onRunCell,
}: {
  run: RunState
  busy: boolean
  onRunOne: () => void
  onRunCell: (instances: number | undefined, bytes: number) => void
}) {
  const { scenario } = run
  const hit = instancesHit(run.throughput) || instancesHit(run.push)
  const latencyInstance = run.latency?.find((c) => c.instance)?.instance
  return (
    <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
      <div className="flex items-center justify-between mb-1 gap-2">
        <div className="flex items-center gap-2 min-w-0">
          <span className="font-medium text-sm text-zinc-800 truncate">{scenario.label}</span>
          <span className="text-xs uppercase tracking-wide text-zinc-400 font-mono">{scenario.transport}</span>
          <RoutingBadge scenario={scenario} />
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <StatusBadge status={run.status} />
          <button
            onClick={onRunOne}
            disabled={busy}
            title="Run this scenario only"
            className="text-xs px-2 py-0.5 rounded border border-zinc-200 text-zinc-700 hover:bg-zinc-50 disabled:opacity-40 disabled:cursor-not-allowed"
          >
            ▶ Run
          </button>
        </div>
      </div>
      {run.status === 'error' && <div className="text-xs text-red-500 mt-2 font-mono">{run.error}</div>}
      {run.latency && (
        <>
          <TableHeader label="Latency" data={{ scenario: scenario.id, cells: run.latency }} />
          <LatencyTable cells={run.latency} busy={busy} onCellRun={(bytes) => onRunCell(undefined, bytes)} />
        </>
      )}
      {run.throughput && (
        <>
          <TableHeader
            label="↑ TX (client → server)"
            data={{ scenario: scenario.id, direction: 'tx', groups: toDirectionView(run.throughput, 'tx') }}
          />
          <RateScalingTable
            data={toDirectionView(run.throughput, 'tx')}
            sizes={THROUGHPUT_SIZES}
            busy={busy}
            onCellRun={onRunCell}
          />
          <TableHeader
            label="↓ RX (server → client)"
            data={{ scenario: scenario.id, direction: 'rx', groups: toDirectionView(run.throughput, 'rx') }}
          />
          <RateScalingTable
            data={toDirectionView(run.throughput, 'rx')}
            sizes={THROUGHPUT_SIZES}
            busy={busy}
            onCellRun={onRunCell}
          />
        </>
      )}
      {run.push && (
        <>
          <TableHeader label="Push" data={{ scenario: scenario.id, groups: run.push }} />
          <RateScalingTable data={run.push} sizes={PUSH_SIZES} busy={busy} onCellRun={onRunCell} />
        </>
      )}
      {(hit || latencyInstance) && (
        <div className="text-[10px] text-zinc-400 mt-2 font-mono">hit: {hit || latencyInstance}</div>
      )}
    </div>
  )
}

// ── Component ──────────────────────────────────────────────────────────────

function Bench() {
  const [runs, setRuns] = useState<RunState[]>(() => SCENARIOS.map(makeRunState))
  const [running, setRunning] = useState(false)
  const [info, setInfo] = useState<Awaited<ReturnType<typeof onBenchInfo>> | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  async function runScenarios(toRun: Scenario[]) {
    if (running) return
    setRunning(true)

    // Reset state for the scenarios we're about to run (preserve others).
    const ids = new Set(toRun.map((s) => s.id))
    setRuns((prev) => prev.map((r) => (ids.has(r.scenario.id) ? makeRunState(r.scenario) : r)))

    const ac = new AbortController()
    abortRef.current = ac

    onBenchInfo()
      .then(setInfo)
      .catch(() => {})

    for (const scenario of toRun) {
      if (ac.signal.aborted) break
      setRuns((prev) => prev.map((r) => (r.scenario.id === scenario.id ? { ...r, status: 'running' } : r)))
      try {
        await executeScenario(
          scenario,
          (updater) => setRuns((prev) => prev.map((r) => (r.scenario.id === scenario.id ? updater(r) : r))),
          ac.signal,
        )
        setRuns((prev) => prev.map((r) => (r.scenario.id === scenario.id ? { ...r, status: 'done' } : r)))
      } catch (err) {
        if (ac.signal.aborted) {
          setRuns((prev) =>
            prev.map((r) => (r.scenario.id === scenario.id ? { ...r, status: 'error', error: 'aborted' } : r)),
          )
          break
        }
        setRuns((prev) =>
          prev.map((r) => (r.scenario.id === scenario.id ? { ...r, status: 'error', error: String(err) } : r)),
        )
      }
    }

    setRunning(false)
  }

  function start() {
    void runScenarios(SCENARIOS)
  }

  function runOne(scenarioId: string) {
    const s = SCENARIOS.find((s) => s.id === scenarioId)
    if (s) void runScenarios([s])
  }

  async function runCell(scenarioId: string, instances: number | undefined, bytes: number) {
    if (running) return
    const scenario = SCENARIOS.find((s) => s.id === scenarioId)
    if (!scenario) return

    setRunning(true)
    const ac = new AbortController()
    abortRef.current = ac

    const patch = (updater: (r: RunState) => RunState) =>
      setRuns((prev) => prev.map((r) => (r.scenario.id === scenarioId ? updater(r) : r)))

    try {
      if (scenario.kind === 'latency') {
        patch((r) => ({
          ...r,
          latency: r.latency!.map((c) => (c.bytes === bytes ? { ...c, status: 'running' } : c)),
        }))
        const result = await measureLatencyCell(scenario, bytes, ac.signal).catch(() => null)
        patch((r) => ({
          ...r,
          latency: r.latency!.map((c) => (c.bytes === bytes ? { ...c, status: 'done', ...(result ?? {}) } : c)),
        }))
      } else if (scenario.kind === 'throughput' && instances !== undefined) {
        patch((r) => patchThroughputCell(r, instances, bytes, { status: 'running' }))
        const result = await measureThroughputCell(scenario, instances, bytes, ac.signal).catch(() => null)
        patch((r) => patchThroughputCell(r, instances, bytes, { status: 'done', ...(result ?? {}) }))
      } else if (scenario.kind === 'push' && instances !== undefined) {
        patch((r) => patchPushCell(r, instances, bytes, { status: 'running' }))
        const result = await measurePushCell(scenario, instances, bytes, ac.signal).catch(() => null)
        patch((r) => patchPushCell(r, instances, bytes, { status: 'done', ...(result ?? {}) }))
      }
    } finally {
      setRunning(false)
    }
  }

  function stop() {
    abortRef.current?.abort()
    setRunning(false)
  }

  return (
    <div className="p-6 max-w-5xl mx-auto font-sans">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-semibold text-zinc-800">Telefunc Benchmark</h1>
          {info && (
            <p className="text-xs text-zinc-400 mt-1 font-mono">
              {info.instanceId} · node {info.nodeVersion} · {info.platform}/{info.arch}
              {info.hasRedis && ' · redis'}
            </p>
          )}
        </div>
        <div className="flex gap-2">
          {!running ? (
            <button
              onClick={start}
              className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
            >
              Run all
            </button>
          ) : (
            <button
              onClick={stop}
              className="px-4 py-2 bg-zinc-200 text-zinc-700 text-sm font-medium rounded-lg hover:bg-zinc-300 transition-colors"
            >
              Stop
            </button>
          )}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        {runs.map((run) => (
          <RunCard
            key={run.scenario.id}
            run={run}
            busy={running}
            onRunOne={() => runOne(run.scenario.id)}
            onRunCell={(instances, bytes) => void runCell(run.scenario.id, instances, bytes)}
          />
        ))}
      </div>

      <div className="mt-6 text-xs text-zinc-400 space-y-1">
        <p>
          <strong>Latency:</strong> {LATENCY_SAMPLES} sequential ack round-trips — p50/p99. Green = best.
        </p>
        <p>
          <strong>Throughput:</strong> {INSTANCE_COUNTS.join('/')} channels. <code>round-robin</code> uses{' '}
          <code>?bench_instances=N</code> (Caddy distributes). <code>pinned</code> uses{' '}
          <code>?bench_instance=&lt;letter&gt;</code> (fixed backend per slot). Pure one-direction measurement:{' '}
          <strong>TX</strong> = client floods, server counts received (no echoes); <strong>RX</strong> = server pushes
          fire-and-forget, client counts. Two sequential phases per cell. Each phase burns {WARMUP_MS / 1000}s of warmup
          (workers running, no samples collected) so the BDP credit window ramps before measurement starts. Then samples
          the rate every {SAMPLE_MS}ms and terminates when the rate has held within{' '}
          {(PLATEAU_TOLERANCE * 100).toFixed(0)}% of its mean for a continuous {PLATEAU_HOLD_MS}ms window — BDP ramp
          produces shifting samples that can't form a plateau, so the hold-window naturally waits past it. Measurement
          capped at {MAX_MEASURE_MS / 1000}s. No RTT in the loop. Each cell: msg/s + MB/s. Green = best per row. Scale =
          top / base.
        </p>
        <p>
          <strong>Server Push:</strong> {INSTANCE_COUNTS.join('/')} channels. Each cell: {PUSH_COUNT}-msg burst across N
          parallel channels — aggregate download rate. Green = best per row (msg/s and MB/s independently). Scale ={' '}
          {INSTANCE_COUNTS[INSTANCE_COUNTS.length - 1]}i / 1i.
        </p>
        <p>
          <strong>hit:</strong> backend instances actually reached (verify routing — pinned should show the expected
          letters).
        </p>
      </div>
    </div>
  )
}
