export { Bench }

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { withContext } from 'telefunc/client'
import {
  onBenchInfo,
  onBenchChannelEcho,
  onBenchChannelEchoNoAck,
  onBenchChannelBinaryEchoNoAck,
  onBenchChannelServerPush,
  onBenchChannelBinaryEcho,
  onBenchChannelBinaryServerPush,
  onBenchBroadcast,
  onBenchBroadcastServerPush,
  onBenchBroadcastBinary,
  onBenchBroadcastBinaryServerPush,
} from './Bench.telefunc'

// ── Configuration ──────────────────────────────────────────────────────────

// ── Matrix axes ──
// Each cell = (transport × instances × scenario × size × concurrency). Sized to finish in
// ~5–10 min on a dev machine, while still surfacing every scenario, both transports, three
// cluster sizes (1/3/6), and serial-vs-parallel behavior.
//
// Why these values:
// - COUNT=100: enough N for stable p50; p99 is noisier but still directional. Halving from
//   200 ~halves wall-time; the marginal information from 200→100 is small.
// - SIZES: 64B (latency floor), 4KB (typical RPC body), 64KB (large), 256KB (xlarge / where
//   serialization dominates). Dropped 16KB — interpolates between 4KB and 64KB without new info.
// - CONCURRENCY scales with instance count via `concurrencyFor(inst)`:
//     1inst → [1, 8]   conc=1 captures the transport's serial latency floor; conc=8 saturates
//     3inst → [8]      conc=1 doesn't exercise the cluster (1 channel = 1 shard busy at a
//                      time) and SSE@conc=1 is timer-clamped at 15.6ms regardless of inst
//                      count, so the data is degenerate. Skip it. Conc=8 = ~2.7 chan/shard.
//     6inst → [12]     same reasoning — conc=12 = 2 chan/shard, real saturation.
//   The asymmetry is intentional: latency floor and scaling are different questions, and
//   each inst count answers only one of them well.
// - INSTANCE_COUNTS [1, 3, 6]: shows whether the system scales out. Caddy reads
//   `?bench_instances=N` from the URL (works on both SSE fetches and WS upgrades — header
//   would be stripped by the browser on `new WebSocket()`).
const COUNT = 100
const SIZES = [
  { label: '64B', bytes: 64 },
  { label: '4KB', bytes: 4 * 1024 },
  { label: '64KB', bytes: 64 * 1024 },
  { label: '256KB', bytes: 256 * 1024 },
] as const
const CELL_TIMEOUT_MS = 10_000
const TRANSPORTS = [['sse'], ['ws']] as const
const INSTANCE_COUNTS = [1, 3, 6] as const

/** Concurrency levels per instance count. 1inst keeps both the serial floor and the
 *  saturation level; multi-instance configs drop conc=1 (it doesn't exercise the cluster
 *  and the SSE timer clamp pegs it identically across inst counts anyway). */
function concurrencyFor(instances: number): readonly number[] {
  if (instances === 1) return [1, 8]
  return [Math.max(8, instances * 2)]
}

/** Union of all concurrency values used across the matrix — for reporting and warmup sizing. */
const ALL_CONCURRENCY_LEVELS = [...new Set(INSTANCE_COUNTS.flatMap((i) => concurrencyFor(i)))].sort((a, b) => a - b)
const MAX_CONCURRENCY = Math.max(...ALL_CONCURRENCY_LEVELS)

/** Number of scenario factories — counted statically here to avoid a forward reference
 *  to `SCENARIOS` (declared further down, after the factory definitions). Update both
 *  together if scenarios are added/removed. */
const SCENARIOS_COUNT = 11

/** Total cells the matrix will produce. Used to drive progress and ETA during a run. */
const TOTAL_CELLS =
  TRANSPORTS.length *
  INSTANCE_COUNTS.reduce((sum, inst) => sum + concurrencyFor(inst).length * SCENARIOS_COUNT * SIZES.length, 0)

// ── Helpers ────────────────────────────────────────────────────────────────

/** Mutable while `runMatrix` iterates `TRANSPORTS` × `INSTANCE_COUNTS`; `isolated()` reads
 *  them to scope every channel/broadcast to (transport, instance count) and to bake both
 *  into the per-call header + connectionKey. */
let currentTransport: (typeof TRANSPORTS)[number] = TRANSPORTS[0]
let currentInstanceCount: (typeof INSTANCE_COUNTS)[number] = INSTANCE_COUNTS[0]

/** Wrap a telefunc so its returned channels/broadcasts use a per-`runId` `ClientConnection`
 *  on the current transport. Without this, parallel runs all multiplex over the page's
 *  single default WS pipe and we measure client-side wire saturation instead of server
 *  throughput. The `currentTransport` and `currentInstanceCount` are baked into the
 *  connectionKey so any matrix-axis change forces a fresh connection rather than reusing
 *  the previous cell's pipe. The per-call `telefuncUrl` override carries the
 *  `bench_instances=N` query so Caddy round-robins into the right upstream subset on both
 *  SSE fetches and WS upgrades (URL works on WS where headers don't). */
const isolated = <F extends (...args: any[]) => any>(fn: F, runId: number): F =>
  withContext(fn, {
    telefuncUrl: `/_telefunc?bench_instances=${currentInstanceCount}`,
    channel: {
      transports: [...currentTransport],
      connectionKey: `bench-${currentTransport.join('-')}-inst${currentInstanceCount}-run-${runId}`,
    },
  })

const percentile = (sorted: number[], p: number): number =>
  sorted.length === 0 ? 0 : sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p))]!

function summarize(rtts: number[]): { p50: number; p99: number } {
  const sorted = [...rtts].sort((a, b) => a - b)
  return { p50: percentile(sorted, 0.5), p99: percentile(sorted, 0.99) }
}

/** Render results as GitHub-flavored markdown — paste into a PR description or issue
 *  comment to share a snapshot. Layout: config strip → aggregate scores → per-family
 *  table → per-transport table → full per-cell table. Each section is interpretable on
 *  its own, so the reader doesn't need to scroll to make sense of any one block. */
function toMarkdown(results: Result[], info: { instanceId: string; hasRedis: boolean } | null): string {
  const cell = (v: string | number | undefined) => (v === undefined ? '—' : String(v))
  const lines: string[] = []
  if (info) {
    lines.push(
      `_\`INSTANCE_ID=${info.instanceId}\` · ${info.hasRedis ? 'redis substrate' : 'in-memory substrate'} · ${COUNT} msgs × [${SIZES.map((s) => s.label).join(', ')}] × conc [${ALL_CONCURRENCY_LEVELS.join(', ')}] × transports [${TRANSPORTS.map((t) => t.join('+')).join(', ')}] × instances [${INSTANCE_COUNTS.join(', ')}]_`,
    )
    lines.push('')
  }

  const scores = deriveScores(results)
  lines.push('### Aggregate scores _(geomean)_')
  lines.push('')
  lines.push('| msg/s | MB/s | p50 ms | p99 ms |')
  lines.push('| ---: | ---: | ---: | ---: |')
  lines.push(`| ${scores.throughput.toLocaleString()} | ${scores.bandwidth} | ${scores.latency} | ${scores.tail} |`)
  lines.push('')

  const family = deriveFamilyScores(results)
  if (family.length > 0) {
    lines.push('### By scenario family')
    lines.push('')
    lines.push('| Family | Cells | msg/s | MB/s | p50 ms | p99 ms |')
    lines.push('| --- | ---: | ---: | ---: | ---: | ---: |')
    for (const f of family) {
      lines.push(
        `| ${f.family} | ${f.cells} | ${f.throughput.toLocaleString()} | ${f.bandwidth} | ${cell(f.latency)} | ${cell(f.tail)} |`,
      )
    }
    lines.push('')
  }

  const tport = deriveTransportScores(results)
  if (tport.length > 0) {
    lines.push('### By transport')
    lines.push('')
    lines.push('| Transport | msg/s | MB/s | p50 ms | p99 ms |')
    lines.push('| --- | ---: | ---: | ---: | ---: |')
    for (const t of tport) {
      lines.push(
        `| ${t.transport} | ${t.throughput.toLocaleString()} | ${t.bandwidth} | ${cell(t.latency)} | ${cell(t.tail)} |`,
      )
    }
    lines.push('')
  }

  const scaling = deriveScalingScores(results)
  if (scaling.length > 0) {
    lines.push('### Scaling _(× = msg/s relative to smallest cluster size for that transport)_')
    lines.push('')
    lines.push('| Transport | Inst | msg/s | scale × | MB/s | p50 ms | p99 ms |')
    lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: |')
    for (const s of scaling) {
      lines.push(
        `| ${s.transport} | ${s.instances} | ${s.throughput.toLocaleString()} | ${s.scaleRatio.toFixed(2)}× | ${s.bandwidth} | ${cell(s.latency)} | ${cell(s.tail)} |`,
      )
    }
    lines.push('')
  }

  const warns = deriveWarnings(results)
  if (warns.length > 0) {
    const errs = warns.filter((w) => w.level === 'error').length
    const wrns = warns.filter((w) => w.level === 'warn').length
    const infos = warns.filter((w) => w.level === 'info').length
    lines.push(
      `### Warnings · ${errs} error${errs === 1 ? '' : 's'}, ${wrns} warning${wrns === 1 ? '' : 's'}${infos > 0 ? `, ${infos} info` : ''}`,
    )
    lines.push('')
    for (const w of warns.filter((w) => w.level !== 'info')) {
      lines.push(`- **${w.level === 'error' ? '⛔' : '⚠️'} ${w.reason}** — \`${fmtRowKey(w.row)}\``)
    }
    // Aggregate info entries by reason — typically dozens of cells share the same platform note.
    const infoBuckets = new Map<string, Result[]>()
    for (const w of warns) {
      if (w.level !== 'info') continue
      const b = infoBuckets.get(w.reason)
      if (b) b.push(w.row)
      else infoBuckets.set(w.reason, [w.row])
    }
    for (const [reason, rs] of infoBuckets) {
      lines.push(`- **ℹ️ ${reason}** — ${rs.length} cell${rs.length === 1 ? '' : 's'} · e.g. \`${fmtRowKey(rs[0]!)}\``)
    }
    lines.push('')
  }

  lines.push('<details><summary>All cells</summary>')
  lines.push('')
  lines.push('| Transport | Inst | Scenario | Size | Conc | msg/s | MB/s | p50 ms | p99 ms | Notes |')
  lines.push('| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |')
  for (const r of results) {
    lines.push(
      `| ${r.transport} | ${r.instances} | ${r.scenario} | ${r.size} | ${r.concurrency} | ${r.msgPerSec.toLocaleString()} | ${r.mbPerSec} | ${cell(r.p50)} | ${cell(r.p99)} | ${r.notes ?? ''} |`,
    )
  }
  lines.push('')
  lines.push('</details>')

  return lines.join('\n') + '\n'
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

/** One row per (transport × instances × scenario × size × concurrency) cell, aggregated across all parallel runs. */
type Result = {
  transport: string
  instances: number
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

// ── Scoring & analysis ─────────────────────────────────────────────────────
//
// Goal: a regression in *any* semantic axis (any scenario family, any size, any conc level)
// should move the score by a comparable amount, so the bench is a sensitive regression
// detector — not a production-traffic-weighted "average user experience" estimator.
//
// Scheme: **equal-per-family geomean**. Within each scenario family, take the unweighted
// geomean of every contributing cell. Then take the geomean of those per-family numbers.
// Each family contributes one number to the headline regardless of how many cells it has,
// so a 16-cell family doesn't drown out a 4-cell family. Within a family, cells share the
// same family voice, so adding more sizes/concurrencies/instance-counts doesn't tilt the
// score either — they just refine that family's number.
//
// Outlier policy:
//   - Failures (`v <= 0`, e.g. all-channels-timed-out cells) are dropped — they're not data.
//   - Info-level cells (SSE streamRequest timer clamp) are dropped from latency/tail
//     computations because they peg p50 at the platform flush quantum regardless of
//     telefunc behavior, burying real signal.
//   - Ack-bound scenarios are dropped from throughput/bandwidth because their msg/s is
//     RTT-bound (browser/OS/transport flush behavior), not a measure of telefunc throughput.
//   - Genuine extreme cells (e.g. broadcast 256KB p50=600ms) are kept — geomean is
//     logarithmic, one outlier moves the score by ~log(outlier)/N proportionally.

function geomean(values: number[]): number {
  const positive = values.filter((v) => Number.isFinite(v) && v > 0)
  if (positive.length === 0) return 0
  const log = positive.reduce((sum, v) => sum + Math.log(v), 0) / positive.length
  return Math.exp(log)
}

/** Pick the numeric metric from each row, applying outlier filters, returning per-family
 *  groups so the caller can take per-family geomeans before aggregating. */
function familyGroupedValues(
  rows: Result[],
  pick: (r: Result) => number | undefined,
  options: { excludeTimerClamp?: boolean; excludeAckBound?: boolean } = {},
): Map<string, number[]> {
  const out = new Map<string, number[]>()
  for (const r of rows) {
    if (options.excludeTimerClamp && isSseTimerClamp(r)) continue
    if (options.excludeAckBound && isAckBoundScenario(r)) continue
    const v = pick(r)
    if (typeof v !== 'number' || !Number.isFinite(v) || v <= 0) continue
    const fam = scenarioFamily(r.scenario)
    const arr = out.get(fam) ?? []
    arr.push(v)
    out.set(fam, arr)
  }
  return out
}

/** Equal-per-family score: per-family geomeans averaged geometrically. Each family
 *  contributes one value to the result; per-family voice is independent of cell count. */
function equalFamilyScore(
  rows: Result[],
  pick: (r: Result) => number | undefined,
  options: { excludeTimerClamp?: boolean; excludeAckBound?: boolean } = {},
): { value: number; cells: number; families: number } {
  const groups = familyGroupedValues(rows, pick, options)
  const familyMeans: number[] = []
  let totalCells = 0
  for (const arr of groups.values()) {
    if (arr.length === 0) continue
    familyMeans.push(geomean(arr))
    totalCells += arr.length
  }
  return { value: geomean(familyMeans), cells: totalCells, families: familyMeans.length }
}

type Scores = {
  throughput: number // msg/s, geomean across all rows that reported it
  bandwidth: number // MB/s, same
  latency: number // p50 ms, geomean across ack-bearing rows
  tail: number // p99 ms, same
  cellsThroughput: number
  cellsLatency: number
}

function deriveScores(rows: Result[]): Scores {
  // Throughput / bandwidth exclude ack-bound scenarios: their msg/s is RTT-bound, not a
  // measure of telefunc's ability to move messages. Latency / tail exclude timer-clamp
  // cells. Each metric is an equal-family geomean — every scenario family has equal voice.
  const msg = equalFamilyScore(rows, (r) => r.msgPerSec, { excludeAckBound: true })
  const mb = equalFamilyScore(rows, (r) => r.mbPerSec, { excludeAckBound: true })
  const p50 = equalFamilyScore(rows, (r) => r.p50, { excludeTimerClamp: true })
  const p99 = equalFamilyScore(rows, (r) => r.p99, { excludeTimerClamp: true })
  return {
    throughput: Math.round(msg.value),
    bandwidth: +mb.value.toFixed(2),
    latency: +p50.value.toFixed(2),
    tail: +p99.value.toFixed(2),
    cellsThroughput: msg.cells,
    cellsLatency: p50.cells,
  }
}

/** Map a scenario name to its semantic family so we can report sub-scores per family. */
function scenarioFamily(scenario: string): string {
  if (scenario.startsWith('channel echo') && scenario.includes('no-ack')) return 'channel echo (no-ack)'
  if (scenario.startsWith('channel echo')) return 'channel echo (ack)'
  if (scenario.startsWith('channel server push')) return 'channel server push'
  if (scenario.startsWith('broadcast server push')) return 'broadcast server push'
  if (scenario.startsWith('broadcast client')) return 'broadcast client pub/sub'
  return scenario
}

/** Ack-bound scenarios pessimize throughput: each send waits for an ack, so the per-message
 *  rate is bound by RTT (browser fetch flush quantum, OS scheduler, transport round-trip)
 *  rather than telefunc's actual ability to move messages. Their msg/s and MB/s belong on
 *  the latency cards (where p50/p99 captures the right thing) — not on throughput.
 *  Excluded from throughput/bandwidth scoring so the headline reflects telefunc's
 *  implementation cost, not the platform's RTT floor. */
function isAckBoundScenario(r: Result): boolean {
  return scenarioFamily(r.scenario) === 'channel echo (ack)'
}

type FamilyScore = {
  family: string
  cells: number
  throughput: number
  bandwidth: number
  latency?: number
  tail?: number
}

function deriveFamilyScores(rows: Result[]): FamilyScore[] {
  const groups = new Map<string, Result[]>()
  for (const r of rows) {
    const f = scenarioFamily(r.scenario)
    if (!groups.has(f)) groups.set(f, [])
    groups.get(f)!.push(r)
  }
  const order = [
    'channel echo (ack)',
    'channel echo (no-ack)',
    'channel server push',
    'broadcast server push',
    'broadcast client pub/sub',
  ]
  return order
    .filter((f) => groups.has(f))
    .map((f): FamilyScore => {
      const rs = groups.get(f)!
      // Inside a family there's just one family, so equalFamilyScore degenerates to a
      // plain unweighted geomean over the family's cells — exactly what we want here.
      const pickGeomean = (
        pick: (r: Result) => number | undefined,
        opts?: { excludeTimerClamp?: boolean },
      ): number | undefined => {
        const values: number[] = []
        for (const r of rs) {
          if (opts?.excludeTimerClamp && isSseTimerClamp(r)) continue
          const v = pick(r)
          if (typeof v === 'number' && Number.isFinite(v) && v > 0) values.push(v)
        }
        return values.length > 0 ? geomean(values) : undefined
      }
      const t = pickGeomean((r) => r.msgPerSec)
      const b = pickGeomean((r) => r.mbPerSec)
      const p50 = pickGeomean((r) => r.p50, { excludeTimerClamp: true })
      const p99 = pickGeomean((r) => r.p99, { excludeTimerClamp: true })
      return {
        family: f,
        cells: rs.length,
        throughput: Math.round(t ?? 0),
        bandwidth: +(b ?? 0).toFixed(2),
        latency: p50 != null ? +p50.toFixed(2) : undefined,
        tail: p99 != null ? +p99.toFixed(2) : undefined,
      }
    })
}

type TransportScore = { transport: string; throughput: number; bandwidth: number; latency?: number; tail?: number }

function deriveTransportScores(rows: Result[]): TransportScore[] {
  const groups = new Map<string, Result[]>()
  for (const r of rows) {
    if (!groups.has(r.transport)) groups.set(r.transport, [])
    groups.get(r.transport)!.push(r)
  }
  return [...groups.entries()].map(([transport, rs]): TransportScore => {
    const msg = equalFamilyScore(rs, (r) => r.msgPerSec, { excludeAckBound: true })
    const mb = equalFamilyScore(rs, (r) => r.mbPerSec, { excludeAckBound: true })
    const p50 = equalFamilyScore(rs, (r) => r.p50, { excludeTimerClamp: true })
    const p99 = equalFamilyScore(rs, (r) => r.p99, { excludeTimerClamp: true })
    return {
      transport,
      throughput: Math.round(msg.value),
      bandwidth: +mb.value.toFixed(2),
      latency: p50.cells > 0 ? +p50.value.toFixed(2) : undefined,
      tail: p99.cells > 0 ? +p99.value.toFixed(2) : undefined,
    }
  })
}

// Scaling story: groups by (transport, instances). Bench was designed so SSE bounces across
// every upstream in the subset (Caddy round-robins each fetch) while WS pins to one upstream
// after the upgrade. Reading the ratios:
//   - SSE ratio < 1 means cross-instance routing via the substrate is *costing* more than
//     it saves — you're hitting the substrate hop on most traffic.
//   - SSE ratio ~1 means scaling is flat — the substrate keeps up but doesn't help.
//   - SSE ratio > 1 means the system genuinely benefits from more shards.
//   - WS ratio is expected to be ~1 across instance counts — single connection sticks to one
//     shard regardless of cluster size. Useful as a control: if WS ratios drift, the routing
//     query plumbing or warmup is biasing results.

type ScalingScore = {
  transport: string
  instances: number
  throughput: number
  bandwidth: number
  latency?: number
  tail?: number
  /** msg/s relative to instances=min for the same transport. 1.00 means flat scaling. */
  scaleRatio: number
}

/** Saturation-conc per inst count = the largest concurrency level we run there. Scaling
 *  comparisons should use only saturation cells so the baseline (1inst) and the higher-inst
 *  configs are measured at equivalent per-shard load. Otherwise the 1inst geomean includes
 *  its timer-clamped conc=1 cells while 3inst/6inst don't, biasing the ratio. */
function isSaturationConc(r: Result): boolean {
  const concs = concurrencyFor(r.instances)
  return r.concurrency === Math.max(...concs)
}

function deriveScalingScores(rows: Result[]): ScalingScore[] {
  // Saturation-only: each inst count's geomean uses only its top-conc cells, so the ratio
  // compares "fully-loaded 1 shard" vs "fully-loaded N shards" — the actual scaling question.
  const saturatedRows = rows.filter(isSaturationConc)
  const groups = new Map<string, Result[]>()
  for (const r of saturatedRows) {
    const key = `${r.transport}|${r.instances}`
    if (!groups.has(key)) groups.set(key, [])
    groups.get(key)!.push(r)
  }
  // First pass: equal-family geomean per (transport, instances), matching overall scoring.
  const partials = [...groups.entries()].map(([key, rs]) => {
    const [transport, instStr] = key.split('|') as [string, string]
    const msg = equalFamilyScore(rs, (r) => r.msgPerSec, { excludeAckBound: true })
    const mb = equalFamilyScore(rs, (r) => r.mbPerSec, { excludeAckBound: true })
    const p50 = equalFamilyScore(rs, (r) => r.p50, { excludeTimerClamp: true })
    const p99 = equalFamilyScore(rs, (r) => r.p99, { excludeTimerClamp: true })
    return {
      transport,
      instances: Number(instStr),
      throughput: Math.round(msg.value),
      bandwidth: +mb.value.toFixed(2),
      latency: p50.cells > 0 ? +p50.value.toFixed(2) : undefined,
      tail: p99.cells > 0 ? +p99.value.toFixed(2) : undefined,
    }
  })
  // Second pass: baseline = lowest-instances entry per transport. Ratios are relative to it.
  const baselines = new Map<string, number>()
  for (const t of new Set(partials.map((p) => p.transport))) {
    const lowest = partials.filter((p) => p.transport === t).sort((a, b) => a.instances - b.instances)[0]
    if (lowest && lowest.throughput > 0) baselines.set(t, lowest.throughput)
  }
  return partials
    .map(
      (p): ScalingScore => ({
        ...p,
        scaleRatio:
          baselines.has(p.transport) && baselines.get(p.transport)! > 0
            ? +(p.throughput / baselines.get(p.transport)!).toFixed(2)
            : 1,
      }),
    )
    .sort((a, b) => a.transport.localeCompare(b.transport) || a.instances - b.instances)
}

// ── Warnings ──
// Every row is checked against a few simple rules. Collecting them upfront means the
// reader gets a triage list ("here are the cells that misbehaved") instead of having to
// scan a 500-row table for outliers. Three levels:
//   error — failure / unusable cell
//   warn  — anomaly worth investigating (tail spike etc.)
//   info  — known platform behavior we want surfaced but not actionable, e.g. the
//           Chrome streamRequest flush quantum. Annotated rather than alarmed.

type WarnLevel = 'error' | 'warn' | 'info'

/** Chrome's fetch upload-stream flushes single chunks gated on a timer (commonly observed
 *  at ~15.6ms but the exact value depends on the platform). Hits SSE serial RPC (one ack
 *  per send, no overlap) and pegs p50 at the flush quantum regardless of payload size.
 *  Disappears once transfer rate climbs (conc>1 fills the inter-flush gap, or large payloads
 *  push past the per-chunk threshold). Not a regression; not a bug; just a platform quirk.
 *  Flagged so reviewers don't read it as a slowdown. */
function isSseTimerClamp(r: Result): boolean {
  return r.transport === 'sse' && r.p50 != null && r.p50 >= 14.5 && r.p50 <= 16.7
}

function rowWarning(r: Result): { level: WarnLevel; reason: string } | null {
  const notes = r.notes ?? ''
  if (notes.includes('setup failure')) return { level: 'error', reason: 'setup failure' }
  if (notes.toLowerCase().includes('timed out') || notes.includes('TimeoutError'))
    return { level: 'error', reason: 'timed out' }
  if (r.msgPerSec === 0) return { level: 'error', reason: 'no throughput' }
  if (r.p50 !== undefined && r.p99 !== undefined && r.p99 > r.p50 * 10 && r.p99 > 5)
    return { level: 'warn', reason: `tail spike (p99=${r.p99}ms vs p50=${r.p50}ms, ${(r.p99 / r.p50).toFixed(0)}×)` }
  if (isSseTimerClamp(r)) return { level: 'info', reason: 'SSE streamRequest timer clamp (~15.6ms flush quantum)' }
  return null
}

type Warning = { level: WarnLevel; reason: string; row: Result }

function deriveWarnings(rows: Result[]): Warning[] {
  const out: Warning[] = []
  for (const r of rows) {
    const w = rowWarning(r)
    if (w) out.push({ ...w, row: r })
  }
  return out
}

// ── Insights ──
// "Best" cells across the full matrix — anchors that put the geomean numbers in context.

function bestBy(rows: Result[], pick: (r: Result) => number | undefined, dir: 'min' | 'max'): Result | null {
  let best: { v: number; r: Result } | null = null
  for (const r of rows) {
    const v = pick(r)
    if (v === undefined || !Number.isFinite(v) || v <= 0) continue
    if (!best || (dir === 'max' ? v > best.v : v < best.v)) best = { v, r }
  }
  return best?.r ?? null
}

function fmtRowKey(r: Result): string {
  return `${r.transport} · ${r.instances}inst · ${r.scenario} · ${r.size} · c${r.concurrency}`
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

// ── Channel-echo factory (with ack) ────────────────────────────────────────
//
// MEASURES:  Sequential request/response RTT — page sends, awaits ack, sends next.
// MODELS:    RPC-style workloads (form submits, mutations, anything blocking on the reply).
// READ:      p50 = transport's natural round-trip floor (TLS + serialize + server hop +
//            ack + parse). conc=1 isolates the floor; conc=8 reveals whether the server
//            can pipeline overlapping requests on a single channel.
// REGRESS:   p50 climbs (a hop was added — extra microtask, extra serialization).
//            p99/p50 ratio explodes (head-of-line blocking, or GC pause).
//            msg/s tanks at conc=8 with same p50 (server lost concurrency).

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

// ── Channel-echo-no-ack factory ────────────────────────────────────────────
//
// MEASURES:  Pure client→server upstream throughput. Page fires `count` sends in a tight
//            loop with no per-send ack, then awaits all echoes. RTT-blind by design.
// MODELS:    High-volume publish workloads — telemetry, log shipping, event streams.
// READ:      msg/s = sustained upstream rate. MB/s should rise with payload size; if it
//            flatlines around small sizes, the upstream is CPU-bound (serialization,
//            framing, or alias/index encoding). Compare conc=1 vs conc=8 to see whether
//            multiple channels add pipeline parallelism on the same connection.
// REGRESS:   msg/s drops (upstream slowed). Bandwidth ceiling lowers (per-byte overhead).
//            Note: p50/p99 are absent — there's no ack, so no per-message RTT.

type EchoNoAckChannel<P, C> = { channel: C; instance: string; payload: P }

function defineChannelEchoNoAck<P, C extends { close: () => Promise<unknown> }>(args: {
  name: string
  open: (runId: number) => Promise<{ channel: C; instance: string }>
  buildPayload: (bytes: number) => P
  send: (channel: C, payload: P) => void
  listen: (channel: C, cb: () => void) => void
}): Scenario<EchoNoAckChannel<P, C>> {
  return {
    name: args.name,
    async setup(bytes, runId) {
      const { channel, instance } = await args.open(runId)
      return { channel, instance, payload: args.buildPayload(bytes) }
    },
    async measure({ channel, instance, payload }, { count }) {
      const done = new Promise<void>((resolve) => {
        let received = 0
        args.listen(channel, () => {
          if (++received === count) resolve()
        })
      })
      const t0 = performance.now()
      for (let i = 0; i < count; i++) args.send(channel, payload)
      await done
      return { instance, totalMs: performance.now() - t0 }
    },
    async cleanup({ channel }) {
      await channel.close()
    },
  }
}

// ── Channel-server-push factory ────────────────────────────────────────────
//
// MEASURES:  Server→client download throughput. Page sends a `'go'` trigger; server fires
//            `count` frames as fast as it can. The listener is registered before the
//            trigger flies (sync, no race — trigger's reply travels the same channel).
// MODELS:    Server-streamed responses — live tickers, log tails, server-sent updates,
//            anything where the server emits a long burst of frames.
// READ:      msg/s = how fast the page drains the wire end-to-end (server emit + transit
//            + browser parse). The `first arrival` note is the time-to-first-byte: if it
//            spikes, the server is buffering instead of flushing. With concurrency, each
//            run gets its own channel — saturating multiple downstreams in parallel.
// REGRESS:   msg/s drops (server slow to emit, or backpressure). first arrival climbs
//            (server batched before sending — flush_interval problem on Caddy, or
//            whatwg streams compositor delaying the first chunk).

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
// MEASURES:  Pub/sub fanout from server to one client. Server publishes `count` frames to
//            a key the page is subscribed to. Two modes:
//              - sequential: server `await`s each `publish()` (substrate RTT × count)
//              - parallel:   server `Promise.all`s all publishes (substrate concurrency)
// MODELS:    Real broadcasts — chat fanout, live updates, presence, anywhere the server
//            pushes to many subscribers via the substrate (Redis pub/sub or in-memory).
// READ:      msg/s = effective broadcast rate this client sees. The gap between sequential
//            and parallel modes shows the substrate's concurrency win. With multi-instance
//            (3/6), the publisher and subscriber may sit on different nodes — Redis pub/sub
//            adds a hop. Watch for `instances:` in notes flagging cross-instance routing.
// REGRESS:   msg/s drops below the substrate's published throughput floor. The parallel
//            mode loses its advantage over sequential (substrate concurrency broken).
//
// Race we fight: `room.subscribe()` is fire-and-forget on the WS; the trigger telefunc goes
// over a separate HTTP request. If HTTP lands first, the server publishes before our peer
// is registered — early frames are lost and `allReceived` deadlocks. `setup` resolves this
// by publishing one probe round-trip ourselves on the same WS — when the probe lands, FIFO
// guarantees the subscribe was already processed.
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

// ── Broadcast client publish+subscribe (page is both sides) ────────────────
//
// MEASURES:  Page-to-page-via-server round-trip on a broadcast key the page is both
//            publishing to and subscribed to. Per-message RTT pooled across `count` sends.
// MODELS:    Ephemeral peer-to-peer-via-server — presence, cursor positions, collab cues.
//            The server itself doesn't process payloads; it just routes the broadcast back.
// READ:      p50 = substrate fanout RTT (publish → substrate → subscribe). msg/s reflects
//            how many independent publishes/sec the substrate can sustain. Note in `notes`:
//            `server-side received N` confirms the substrate didn't drop frames.
// REGRESS:   p99/p50 ratio expands (substrate jitter). server-side received < count
//            (substrate dropping). msg/s drops (substrate overhead per publish climbed).

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

const channelTextEchoNoAck = defineChannelEchoNoAck({
  name: 'channel echo (text, no-ack)',
  open: async (runId) => {
    const { channel, instance } = await isolated(onBenchChannelEchoNoAck, runId)()
    return { channel, instance }
  },
  buildPayload: (bytes) => 'x'.repeat(bytes),
  send: (channel, payload) => {
    void channel.send({ seq: 0, payload })
  },
  listen: (channel, cb) => {
    channel.listen(() => cb())
  },
})

const channelBinaryEchoNoAck = defineChannelEchoNoAck({
  name: 'channel echo (binary, no-ack)',
  open: async (runId) => {
    const { channel, instance } = await isolated(onBenchChannelBinaryEchoNoAck, runId)()
    return { channel, instance }
  },
  buildPayload: (bytes) => {
    const buf = new Uint8Array(bytes)
    for (let i = 0; i < bytes; i++) buf[i] = i & 0xff
    return buf
  },
  send: (channel, payload) => {
    void channel.sendBinary(payload)
  },
  listen: (channel, cb) => {
    channel.listenBinary(() => cb())
  },
})

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
  channelTextEchoNoAck,
  channelBinaryEchoNoAck,
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
 *  hot path on each (text echo, binary echo, broadcast pub/sub). Two jobs in one: the
 *  transport handshake finishes here instead of inside a measured cell, and V8 has JIT'd
 *  every path before the matrix starts. One text channel per connection is kept open as
 *  a keepalive — without it the connection's TTL would fire between cells and we'd pay
 *  handshake cost again.
 *
 *  No upgrade-settle is needed: `isolated()` pins each cell to a specific transport
 *  (`['sse']` or `['ws']`), so SSE→WS upgrade-in-the-background never runs.
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
  /** Wall-clock start of the active run, used to compute elapsed/ETA. Null when idle. */
  const [runStartTime, setRunStartTime] = useState<number | null>(null)
  /** Increments once per second while a run is in progress so the elapsed/ETA display
   *  stays current even during warmup phases when no cells are flushing. */
  const [tick, setTick] = useState(0)

  // Results accumulate in a ref during the run, flushed to state every 500ms — keeps
  // `setState`-driven re-renders out of the bench's hot path so the table render cost
  // doesn't bias timings.
  const resultsRef = useRef<Result[]>([])

  // ── ETA model ────────────────────────────────────────────────────────────
  // The matrix is fully deterministic: enumerate the entire schedule of (warmup, cell)
  // entries at run-start. As each entry completes, record its duration in a per-group
  // table keyed by `(scenario, size, conc)` — these tuples have similar costs across
  // instance counts and transports, so once we've seen one we have a decent estimate
  // for the rest. ETA = sum of predicted durations for unstarted entries.
  //
  // Prediction order: group median → global cell median → 1s default. Warmup has its
  // own table since its cost (parallel keepalive opens) doesn't track cell tuples.
  type PlanEntry =
    | { kind: 'warmup'; transport: string; inst: number; groupKey: string }
    | {
        kind: 'cell'
        transport: string
        inst: number
        conc: number
        scenario: string
        size: string
        groupKey: string
      }
  const planRef = useRef<PlanEntry[]>([])
  const planIdxRef = useRef<number>(0)
  const cellDurationsRef = useRef<Map<string, number[]>>(new Map())
  const warmupDurationsRef = useRef<number[]>([])

  useEffect(() => {
    onBenchInfo().then(setInfo)
  }, [])

  // Drive the running-status ticker. Stays at 1Hz to keep re-render overhead off the
  // bench's hot path (the flusher already covers per-cell updates at 500ms).
  useEffect(() => {
    if (running === null) return
    const id = setInterval(() => setTick((t) => t + 1), 1000)
    return () => clearInterval(id)
  }, [running])

  /** Compute remaining wall-time using observed per-(scenario, size, conc) durations.
   *  Returns null if the plan isn't built yet. Each unstarted entry's duration is
   *  predicted from its group's median observation, falling back to global cell median,
   *  then to 1s. Warmup uses its own table. Recomputed each tick (1Hz). */
  function predictRemainingMs(): number | null {
    const plan = planRef.current
    const idx = planIdxRef.current
    if (plan.length === 0 || idx >= plan.length) return null

    const median = (arr: number[]): number => {
      if (arr.length === 0) return 0
      const sorted = [...arr].sort((a, b) => a - b)
      return sorted[Math.floor(sorted.length / 2)]!
    }

    const allCellDurations: number[] = []
    for (const arr of cellDurationsRef.current.values()) allCellDurations.push(...arr)
    const globalCellMedian = allCellDurations.length > 0 ? median(allCellDurations) : 1000
    const warmupMedian = warmupDurationsRef.current.length > 0 ? median(warmupDurationsRef.current) : 5000

    let totalMs = 0
    for (let i = idx; i < plan.length; i++) {
      const entry = plan[i]!
      if (entry.kind === 'warmup') {
        totalMs += warmupMedian
      } else {
        const groupDurs = cellDurationsRef.current.get(entry.groupKey)
        totalMs += groupDurs && groupDurs.length > 0 ? median(groupDurs) : globalCellMedian
      }
    }
    return totalMs
  }

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
   *   msg/s  = sum(successful runs × COUNT) / max successful totalMs (in seconds)
   *   MB/s   = same, weighted by payload size
   *   p50/p99 = percentiles over the pooled per-message RTTs from successful runs
   *   notes  = setup failures, measure timeouts, multi-instance distribution, etc.
   *
   *  Timeout policy: a measure-timeout pushes a synthetic outcome with `instance: '?'` and
   *  `totalMs ≈ CELL_TIMEOUT_MS` but no real progress signal (we can't tell how many
   *  messages actually completed before the kill). Including those in the aggregate would
   *  inflate msg/s — `outcomes.length × COUNT` assumes every channel delivered all COUNT
   *  messages, while `max(totalMs)` would be the timeout floor (~10s). Result: a cell with
   *  half the channels timing out would report ~half the true throughput dragged toward
   *  the timeout wall. We instead aggregate only the successful runs; the cell's notes
   *  still carry the timeout marker so warnings flag it. If *every* run timed out, msg/s=0
   *  and the warning system catches it as 'no throughput'. */
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

    // Successful = real measure() resolution. Timeouts (and any other catch-block synthetic
    // outcomes) are tagged with instance='?' and don't contribute to the throughput math.
    const successfulOutcomes = outcomes.filter((o) => o.instance !== '?')
    const timeoutCount = outcomes.length - successfulOutcomes.length
    if (timeoutCount > 0) {
      noteParts.push(`${timeoutCount}/${outcomes.length} runs timed out`)
    }

    const transport = currentTransport.join('+')

    if (successfulOutcomes.length === 0) {
      // All runs timed out (or no runs at all). Throughput is unknowable; mark zero so
      // the warning system flags the cell.
      resultsRef.current.push({
        transport,
        instances: currentInstanceCount,
        scenario,
        size: sizeLabel,
        concurrency: conc,
        msgPerSec: 0,
        mbPerSec: 0,
        notes: noteParts.join(' · ') || 'no runs completed',
      })
      return
    }

    const totalMessages = successfulOutcomes.length * COUNT
    const maxTotalMs = Math.max(...successfulOutcomes.map((o) => o.totalMs))
    const pooled = successfulOutcomes.flatMap((o) => o.rtts ?? [])
    const seconds = maxTotalMs / 1000
    const r: Result = {
      transport,
      instances: currentInstanceCount,
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
    setRunStartTime(Date.now())
    // Build the deterministic schedule of (warmup, cell) entries. The ETA model treats
    // this list as the source of truth for "what's left to do".
    planRef.current = []
    for (const transports of TRANSPORTS) {
      const transport = transports.join('+')
      for (const inst of INSTANCE_COUNTS) {
        planRef.current.push({ kind: 'warmup', transport, inst, groupKey: 'warmup' })
        for (const conc of concurrencyFor(inst)) {
          for (const scenario of SCENARIOS) {
            for (const { label } of SIZES) {
              planRef.current.push({
                kind: 'cell',
                transport,
                inst,
                conc,
                scenario: scenario.name,
                size: label,
                groupKey: `${scenario.name}|${label}|${conc}`,
              })
            }
          }
        }
      }
    }
    planIdxRef.current = 0
    cellDurationsRef.current = new Map()
    warmupDurationsRef.current = []

    const stop = startFlusher()
    try {
      for (const transports of TRANSPORTS) {
        currentTransport = transports
        const transportLabel = transports.join('+')
        for (const instanceCount of INSTANCE_COUNTS) {
          currentInstanceCount = instanceCount
          const cellLabel = `${transportLabel}|${instanceCount}inst`
          let close: (() => Promise<void>) | null = null
          try {
            setRunning(`[${cellLabel}] warmup (${MAX_CONCURRENCY} connections × all code paths)`)
            const warmupStart = performance.now()
            close = await warmupConnections()
            warmupDurationsRef.current.push(performance.now() - warmupStart)
            planIdxRef.current++
            for (const conc of concurrencyFor(instanceCount)) {
              for (const scenario of SCENARIOS) {
                for (const { label, bytes } of SIZES) {
                  setRunning(`[${cellLabel}] conc ${conc}: ${scenario.name} @ ${label}`)
                  const cellStart = performance.now()
                  await runCell(scenario, conc, bytes, label)
                  const dur = performance.now() - cellStart
                  const key = `${scenario.name}|${label}|${conc}`
                  const arr = cellDurationsRef.current.get(key) ?? []
                  arr.push(dur)
                  cellDurationsRef.current.set(key, arr)
                  planIdxRef.current++
                }
              }
            }
          } finally {
            if (close) await close()
          }
        }
      }
    } finally {
      setRunning(null)
      setRunStartTime(null)
      stop()
    }
  }

  // ── Derived (memoized over results) ──────────────────────────────────────

  const scores = useMemo(() => deriveScores(results), [results])
  const familyScores = useMemo(() => deriveFamilyScores(results), [results])
  const transportScores = useMemo(() => deriveTransportScores(results), [results])
  const scalingScores = useMemo(() => deriveScalingScores(results), [results])
  const warnings = useMemo(() => deriveWarnings(results), [results])

  /** Compact, CI-friendly export — scores only, no per-cell rows. Stable shape so external
   *  tooling (PR comments, regression gates) can diff it across runs. The `schema` version
   *  is bumped on any breaking change to this object's shape, so consumers can fail fast
   *  rather than silently mis-diffing. */
  const scoresExport = useMemo(
    () => ({
      schema: 1 as const,
      ts: new Date().toISOString(),
      info,
      matrix: {
        count: COUNT,
        sizes: SIZES.map((s) => s.label),
        concurrency: ALL_CONCURRENCY_LEVELS,
        transports: TRANSPORTS.map((t) => t.join('+')),
        instances: [...INSTANCE_COUNTS],
        cells: results.length,
      },
      scores,
      byFamily: familyScores,
      byTransport: transportScores,
      scaling: scalingScores,
      warnings: warnings.map((w) => ({ level: w.level, reason: w.reason, cell: fmtRowKey(w.row) })),
    }),
    [info, results.length, scores, familyScores, transportScores, scalingScores, warnings],
  )

  const errorCount = warnings.filter((w) => w.level === 'error').length
  const warnCount = warnings.filter((w) => w.level === 'warn').length
  const infoCount = warnings.filter((w) => w.level === 'info').length

  /** Group info-level entries by reason: many cells often share the same platform note
   *  (e.g. SSE timer clamp). One summary row per group keeps the panel actionable. */
  const groupedWarnings = useMemo(() => {
    type Group =
      | { kind: 'individual'; level: WarnLevel; reason: string; row: Result }
      | { kind: 'aggregate'; level: WarnLevel; reason: string; cells: number; sample: Result }
    const out: Group[] = []
    const infoBuckets = new Map<string, { reason: string; rows: Result[] }>()
    for (const w of warnings) {
      if (w.level === 'info') {
        const b = infoBuckets.get(w.reason)
        if (b) b.rows.push(w.row)
        else infoBuckets.set(w.reason, { reason: w.reason, rows: [w.row] })
      } else {
        out.push({ kind: 'individual', level: w.level, reason: w.reason, row: w.row })
      }
    }
    for (const b of infoBuckets.values()) {
      out.push({ kind: 'aggregate', level: 'info', reason: b.reason, cells: b.rows.length, sample: b.rows[0]! })
    }
    return out
  }, [warnings])

  // ── JSX ──────────────────────────────────────────────────────────────────

  return (
    <div className="min-h-screen bg-zinc-50 text-zinc-900">
      <div className="max-w-7xl mx-auto px-6 py-8 space-y-6">
        {/* Header */}
        <header className="flex flex-wrap items-end justify-between gap-4 border-b border-zinc-200 pb-4">
          <div>
            <h1 className="text-xl font-semibold tracking-tight">Telefunc Benchmark</h1>
            {info && (
              <p className="mt-1 text-xs text-zinc-500 font-mono">
                INSTANCE_ID={info.instanceId} · {info.hasRedis ? 'redis substrate' : 'in-memory substrate'} · {COUNT}{' '}
                msgs × [{SIZES.map((s) => s.label).join(', ')}] × c[{ALL_CONCURRENCY_LEVELS.join(', ')}] × t[
                {TRANSPORTS.map((t) => t.join('+')).join(', ')}] × i[{INSTANCE_COUNTS.join(', ')}] · timeout{' '}
                {CELL_TIMEOUT_MS / 1000}s
              </p>
            )}
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              id="bench-run"
              onClick={runMatrix}
              disabled={running !== null}
              className="px-4 py-1.5 text-sm font-medium bg-zinc-900 text-white rounded hover:bg-zinc-800 disabled:opacity-50"
            >
              Run benchmark
            </button>
            <button
              onClick={() => navigator.clipboard.writeText(JSON.stringify(scoresExport, null, 2))}
              disabled={results.length === 0}
              className="px-3 py-1.5 text-sm border border-zinc-300 text-zinc-700 hover:bg-zinc-100 rounded disabled:opacity-40"
              title="Compact JSON, scores only — meant for CI to diff against a baseline"
            >
              Copy scores
            </button>
            <button
              onClick={() => navigator.clipboard.writeText(toMarkdown(results, info))}
              disabled={results.length === 0}
              className="px-3 py-1.5 text-sm border border-zinc-300 text-zinc-700 hover:bg-zinc-100 rounded disabled:opacity-40"
            >
              Copy as Markdown
            </button>
            <button
              onClick={() => navigator.clipboard.writeText(JSON.stringify({ info, results }, null, 2))}
              disabled={results.length === 0}
              className="px-3 py-1.5 text-sm border border-zinc-300 text-zinc-700 hover:bg-zinc-100 rounded disabled:opacity-40"
            >
              Copy raw JSON
            </button>
            <button
              onClick={reset}
              disabled={running !== null || results.length === 0}
              className="px-3 py-1.5 text-sm text-zinc-500 hover:text-zinc-900 disabled:opacity-40"
            >
              Clear
            </button>
          </div>
        </header>

        {/* Status (running) — progress bar + elapsed + matrix-aware ETA. Re-renders 1Hz
            via `tick` so the timing display stays current even during warmup phases when
            no cells flush. ETA = sum of predicted remaining-entry durations, where each
            entry's prediction is its group's median (group = scenario+size+conc). */}
        {running && (
          <RunningProgress
            running={running}
            completed={results.length}
            total={TOTAL_CELLS}
            startTime={runStartTime}
            remainingMs={predictRemainingMs()}
            tick={tick}
          />
        )}

        {/* Empty state */}
        {!running && results.length === 0 && (
          <div className="rounded-md border border-dashed border-zinc-300 bg-white px-6 py-10 text-center text-sm text-zinc-500">
            No data yet. Click <span className="font-medium text-zinc-700">Run benchmark</span> to start.
          </div>
        )}

        {/* Aggregate scores — 2 transport rows × 4 metric cards. Each card: transport-scoped
            geomean as hero + sub-bars per instance count to surface scaling behavior. */}
        {results.length > 0 && (
          <section className="space-y-4">
            <SectionHeader
              title="Aggregate scores"
              hint="Equal-per-family geomean — each scenario family (echo / server push / broadcast push / pub/sub) contributes one number, so a regression in any family moves the score equally. Throughput/bandwidth exclude ack-bound scenarios (RTT-bound, not telefunc throughput); latency/tail exclude SSE streamRequest timer-clamp cells. Each card breaks the metric down by instance count — longest bar wins."
            />
            <TransportScoreRow
              transport="sse"
              results={results}
              scalingScores={scalingScores}
              transportScores={transportScores}
            />
            <TransportScoreRow
              transport="ws"
              results={results}
              scalingScores={scalingScores}
              transportScores={transportScores}
            />
          </section>
        )}

        {/* By family */}
        {familyScores.length > 0 && (
          <section>
            <SectionHeader title="By scenario family" hint="What each family contributes to the aggregate." />
            <div className="overflow-x-auto rounded-md border border-zinc-200 bg-white">
              <table className="w-full text-xs">
                <thead className="text-left text-zinc-500 bg-zinc-50">
                  <tr>
                    <th className="py-2 px-3 font-medium">Family</th>
                    <th className="py-2 px-3 font-medium text-right">Cells</th>
                    <th className="py-2 px-3 font-medium text-right">msg/s</th>
                    <th className="py-2 px-3 font-medium text-right">MB/s</th>
                    <th className="py-2 px-3 font-medium text-right">p50 ms</th>
                    <th className="py-2 px-3 font-medium text-right">p99 ms</th>
                  </tr>
                </thead>
                <tbody>
                  {familyScores.map((f) => (
                    <tr key={f.family} className="border-t border-zinc-100">
                      <td className="py-2 px-3 font-mono">{f.family}</td>
                      <td className="py-2 px-3 text-right tabular-nums text-zinc-500">{f.cells}</td>
                      <td className="py-2 px-3 text-right tabular-nums">{f.throughput.toLocaleString()}</td>
                      <td className="py-2 px-3 text-right tabular-nums">{f.bandwidth}</td>
                      <td className="py-2 px-3 text-right tabular-nums">{f.latency ?? '—'}</td>
                      <td className="py-2 px-3 text-right tabular-nums">{f.tail ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* By transport: folded into the Aggregate-score cards above (each card now shows SSE/WS split). */}

        {/* Scaling */}
        {scalingScores.length > 0 && (
          <section>
            <SectionHeader
              title="Scaling"
              hint="Geomean per (transport × instance count). The ratio is msg/s relative to the same transport at the smallest cluster size — 1.00 = flat, &gt;1 = scales out, &lt;1 = substrate hop costs more than it saves. WS pins to one shard, so its ratio should hover around 1."
            />
            <div className="overflow-x-auto rounded-md border border-zinc-200 bg-white">
              <table className="w-full text-xs">
                <thead className="text-left text-zinc-500 bg-zinc-50">
                  <tr>
                    <th className="py-2 px-3 font-medium">Transport</th>
                    <th className="py-2 px-3 font-medium text-right">Inst</th>
                    <th className="py-2 px-3 font-medium text-right">msg/s</th>
                    <th className="py-2 px-3 font-medium text-right">scale ×</th>
                    <th className="py-2 px-3 font-medium text-right">MB/s</th>
                    <th className="py-2 px-3 font-medium text-right">p50 ms</th>
                    <th className="py-2 px-3 font-medium text-right">p99 ms</th>
                  </tr>
                </thead>
                <tbody>
                  {scalingScores.map((s) => {
                    const tone =
                      s.scaleRatio >= 1.1 ? 'text-emerald-700' : s.scaleRatio <= 0.9 ? 'text-rose-700' : 'text-zinc-500'
                    return (
                      <tr key={`${s.transport}-${s.instances}`} className="border-t border-zinc-100">
                        <td className="py-2 px-3 font-mono">{s.transport}</td>
                        <td className="py-2 px-3 text-right tabular-nums">{s.instances}</td>
                        <td className="py-2 px-3 text-right tabular-nums">{s.throughput.toLocaleString()}</td>
                        <td className={'py-2 px-3 text-right tabular-nums font-medium ' + tone}>
                          {s.scaleRatio.toFixed(2)}×
                        </td>
                        <td className="py-2 px-3 text-right tabular-nums">{s.bandwidth}</td>
                        <td className="py-2 px-3 text-right tabular-nums">{s.latency ?? '—'}</td>
                        <td className="py-2 px-3 text-right tabular-nums">{s.tail ?? '—'}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </section>
        )}

        {/* Peaks: folded into the Aggregate-score cards above (each card carries its own peak/best). */}

        {/* Warnings — errors and warns listed individually, info-level (timer clamp etc.) aggregated */}
        {groupedWarnings.length > 0 && (
          <section>
            <SectionHeader
              title={`Warnings (${errorCount} error${errorCount === 1 ? '' : 's'}, ${warnCount} warning${warnCount === 1 ? '' : 's'}${infoCount > 0 ? `, ${infoCount} info` : ''})`}
              hint="Failures and anomalies triaged out of the table. Info-level entries are platform behavior (e.g. browser timer quirks), aggregated by reason — not regressions."
            />
            <ul className="rounded-md border border-zinc-200 bg-white divide-y divide-zinc-100">
              {groupedWarnings.map((g, i) => {
                const tint =
                  g.level === 'error' ? 'bg-rose-50/50' : g.level === 'warn' ? 'bg-amber-50/50' : 'bg-sky-50/40'
                const dot = g.level === 'error' ? 'bg-rose-500' : g.level === 'warn' ? 'bg-amber-500' : 'bg-sky-500'
                const text =
                  g.level === 'error' ? 'text-rose-700' : g.level === 'warn' ? 'text-amber-700' : 'text-sky-700'
                return (
                  <li key={i} className={'flex items-start gap-3 px-3 py-2 text-xs ' + tint}>
                    <span className={'mt-0.5 inline-block w-1.5 h-1.5 rounded-full flex-shrink-0 ' + dot} />
                    <div className="flex-1 min-w-0">
                      <span className={'font-medium ' + text}>{g.reason}</span>
                      {g.kind === 'aggregate' ? (
                        <span className="text-zinc-500 font-mono ml-2">
                          — {g.cells} cell{g.cells === 1 ? '' : 's'} affected · e.g. {fmtRowKey(g.sample)}
                        </span>
                      ) : (
                        <span className="text-zinc-500 font-mono ml-2 break-all">— {fmtRowKey(g.row)}</span>
                      )}
                    </div>
                  </li>
                )
              })}
            </ul>
          </section>
        )}

        {/* All cells */}
        {results.length > 0 && (
          <section>
            <SectionHeader
              title={`All cells (${results.length})`}
              hint="One row per matrix cell. Tinted rows have warnings."
            />
            <div className="overflow-x-auto rounded-md border border-zinc-200 bg-white">
              <table className="w-full text-xs">
                <thead className="text-left text-zinc-500 bg-zinc-50">
                  <tr>
                    <th className="py-2 px-3 font-medium">Transport</th>
                    <th className="py-2 px-3 font-medium text-right">Inst</th>
                    <th className="py-2 px-3 font-medium">Scenario</th>
                    <th className="py-2 px-3 font-medium">Size</th>
                    <th className="py-2 px-3 font-medium text-right">Conc</th>
                    <th className="py-2 px-3 font-medium text-right">msg/s</th>
                    <th className="py-2 px-3 font-medium text-right">MB/s</th>
                    <th className="py-2 px-3 font-medium text-right">p50 ms</th>
                    <th className="py-2 px-3 font-medium text-right">p99 ms</th>
                    <th className="py-2 px-3 font-medium">Notes</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r, i) => {
                    const w = rowWarning(r)
                    const tint =
                      w?.level === 'error'
                        ? 'bg-rose-50/40'
                        : w?.level === 'warn'
                          ? 'bg-amber-50/40'
                          : w?.level === 'info'
                            ? 'bg-sky-50/40'
                            : ''
                    return (
                      <tr key={i} className={'border-t border-zinc-100 ' + tint}>
                        <td className="py-1.5 px-3 font-mono">{r.transport}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums">{r.instances}</td>
                        <td className="py-1.5 px-3">{r.scenario}</td>
                        <td className="py-1.5 px-3 font-mono">{r.size}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums">{r.concurrency}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums">{r.msgPerSec.toLocaleString()}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums">{r.mbPerSec}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums">{r.p50 ?? '—'}</td>
                        <td className="py-1.5 px-3 text-right tabular-nums">{r.p99 ?? '—'}</td>
                        <td className="py-1.5 px-3 text-zinc-500">{r.notes ?? ''}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </section>
        )}
      </div>
    </div>
  )
}

// ── Presentational helpers ─────────────────────────────────────────────────

/** Live progress panel during a run. Shows percentage, completed/total cells, elapsed
 *  time, matrix-aware ETA, and the current cell label.
 *
 *  ETA design: bench cells span ~50ms (small RPC) to ~10s (timeouts) with ~5s warmup
 *  gaps interleaved. A naive `elapsed/completed × remaining` model would jitter wildly
 *  because cells aren't uniform-cost and the warmup-vs-cell cost ratio is ~100×. Instead
 *  the parent enumerates the entire schedule up-front and feeds us a `remainingMs` that
 *  sums each unstarted entry's *group-median* duration (group = scenario+size+conc).
 *  This is bounded by observed data: a saturation 256KB cell predicts as a saturation
 *  256KB cell, not as the global average. Once the first iteration completes, every
 *  group has at least one observation, so the ETA stops drifting much. */
function RunningProgress({
  running,
  completed,
  total,
  startTime,
  remainingMs,
  tick: _tick,
}: {
  running: string
  completed: number
  total: number
  startTime: number | null
  remainingMs: number | null
  tick: number
}) {
  const elapsedMs = startTime != null ? Date.now() - startTime : 0
  const pct = total > 0 ? Math.min(100, (completed / total) * 100) : 0

  return (
    <div className="rounded-md border border-zinc-200 bg-white px-4 py-3 space-y-2">
      <div className="flex items-baseline justify-between gap-3">
        <div className="flex items-center gap-2 text-xs font-mono text-zinc-700">
          <span className="inline-block w-2 h-2 rounded-full bg-amber-500 animate-pulse" />
          <span className="font-semibold tabular-nums">{Math.floor(pct)}%</span>
          <span className="text-zinc-400">·</span>
          <span className="tabular-nums">
            {completed}/{total}
          </span>
          <span className="text-zinc-400">cells</span>
        </div>
        <div className="text-xs font-mono text-zinc-500 tabular-nums">
          <span>{fmtDuration(elapsedMs)} elapsed</span>
          {remainingMs !== null && (
            <>
              <span className="text-zinc-300 mx-2">·</span>
              <span>~{fmtDuration(remainingMs)} left</span>
            </>
          )}
        </div>
      </div>
      <div className="h-1.5 bg-zinc-100 rounded-full overflow-hidden">
        <div
          className="h-full bg-amber-500 rounded-full"
          style={{ width: `${pct}%`, transition: 'width 200ms ease-out' }}
        />
      </div>
      <div className="text-[11px] font-mono text-zinc-500 truncate" title={running}>
        {running}
      </div>
    </div>
  )
}

function fmtDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`
  const s = Math.floor(ms / 1000)
  if (s < 60) return `${s}s`
  const m = Math.floor(s / 60)
  const rem = s % 60
  return `${m}m${rem.toString().padStart(2, '0')}s`
}

function SectionHeader({ title, hint }: { title: string; hint?: string }) {
  return (
    <div className="mb-2">
      <h2 className="text-[11px] uppercase tracking-wider text-zinc-500 font-semibold">{title}</h2>
      {hint && <p className="text-[11px] text-zinc-400 mt-0.5">{hint}</p>}
    </div>
  )
}

/** Renders a row of 4 cards (Throughput / Bandwidth / Latency / Tail) scoped to one
 *  transport. The card is *self-consistent*: every number on it (hero, per-shard sub-bars,
 *  peak) is computed from the same population of cells — saturation-only, transport-scoped.
 *  This is what the user sees:
 *
 *      ┌─ THROUGHPUT                        msg/s ─┐
 *      │ 4,207                                     │  ← hero: weighted geomean across
 *      │ geomean · 88 saturation cells             │      saturation-conc cells, this transport
 *      │ ─────────────────────────────────────     │
 *      │ Per-shard scaling                         │
 *      │ 1 shard   ████████████  4,207  (winner)   │  ← sub-bars: same cell set, grouped
 *      │ 3 shards  ██████░░░░░░  3,124             │      by inst count. Ratio reads here.
 *      │ 6 shards  ████░░░░░░░░  2,201             │
 *      │ ─────────────────────────────────────     │
 *      │ Peak 86,022 — channel echo · 64B · c8     │  ← anchor: best single cell
 *      └───────────────────────────────────────────┘
 *
 *  Why saturation-only: the bars represent "given a saturating workload, how does this
 *  transport scale across N shards?" — if the hero included serial conc=1 cells too,
 *  it would be answering a different question and wouldn't equal the weighted aggregate
 *  of its own bars. The serial-conc=1 measurements still appear in the All Cells table.
 *  Latency/tail additionally exclude SSE timer-clamp cells (Chrome flush quantum ~15.6ms
 *  isn't a telefunc characteristic). */
function TransportScoreRow({
  transport,
  results,
  scalingScores,
}: {
  transport: 'sse' | 'ws'
  results: Result[]
  scalingScores: ScalingScore[]
  transportScores: TransportScore[] // unused but kept for shape stability with caller
}) {
  const transportLabel = transport.toUpperCase()
  const tRows = useMemo(() => results.filter((r) => r.transport === transport), [results, transport])
  // The cell set every number on this card is computed from. Saturation-only so per-shard
  // bars and the hero are in the same universe.
  const satRows = useMemo(() => tRows.filter(isSaturationConc), [tRows])
  // Latency/tail exclude timer-clamp cells (a Chrome platform floor, not telefunc behavior).
  const satRowsForLatency = useMemo(() => satRows.filter((r) => !isSseTimerClamp(r)), [satRows])

  // Throughput cell set excludes ack-bound scenarios — their msg/s is RTT-bound, not a
  // measure of how fast telefunc moves messages. Their data lives on the latency cards.
  const satRowsForThroughput = useMemo(() => satRows.filter((r) => !isAckBoundScenario(r)), [satRows])

  // Heroes — equal-family geomean across the same cells the sub-bars summarize. Each
  // family contributes one number; cell-count imbalances between families don't tilt the
  // hero, so a regression in any family is detectable.
  const heroThroughput = useMemo(
    () => Math.round(equalFamilyScore(satRowsForThroughput, (r) => r.msgPerSec).value),
    [satRowsForThroughput],
  )
  const heroBandwidth = useMemo(
    () => +equalFamilyScore(satRowsForThroughput, (r) => r.mbPerSec).value.toFixed(2),
    [satRowsForThroughput],
  )
  const heroLatency = useMemo(() => {
    const s = equalFamilyScore(satRowsForLatency, (r) => r.p50)
    return s.cells > 0 ? +s.value.toFixed(2) : null
  }, [satRowsForLatency])
  const heroTail = useMemo(() => {
    const s = equalFamilyScore(satRowsForLatency, (r) => r.p99)
    return s.cells > 0 ? +s.value.toFixed(2) : null
  }, [satRowsForLatency])

  const cellsThroughput = satRowsForThroughput.filter((r) => r.msgPerSec > 0).length
  const cellsLatency = satRowsForLatency.filter((r) => r.p50 != null && r.p50 > 0).length

  // Peaks — best single cell within the same population each metric uses.
  const peakThroughput = useMemo(() => bestBy(satRowsForThroughput, (r) => r.msgPerSec, 'max'), [satRowsForThroughput])
  const peakBandwidth = useMemo(() => bestBy(satRowsForThroughput, (r) => r.mbPerSec, 'max'), [satRowsForThroughput])
  const bestP50 = useMemo(() => bestBy(satRowsForLatency, (r) => r.p50, 'min'), [satRowsForLatency])
  const bestP99 = useMemo(() => bestBy(satRowsForLatency, (r) => r.p99, 'min'), [satRowsForLatency])

  function pickPerInstance(metric: 'throughput' | 'bandwidth' | 'latency' | 'tail') {
    return INSTANCE_COUNTS.map((inst) => {
      const row = scalingScores.find((s) => s.transport === transport && s.instances === inst)
      return { label: (inst as number) === 1 ? '1 shard' : `${inst} shards`, value: row?.[metric] }
    })
  }

  function fmtPeakAnchor(r: Result | null): string | undefined {
    if (!r) return undefined
    return `${r.scenario} · ${r.size} · c${r.concurrency}`
  }

  return (
    <div>
      <div className="mb-2 flex items-baseline gap-2 flex-wrap">
        <span
          className={
            'inline-flex items-center text-[10px] font-mono uppercase tracking-wider px-1.5 py-0.5 rounded ' +
            (transport === 'sse' ? 'bg-sky-50 text-sky-700' : 'bg-violet-50 text-violet-700')
          }
        >
          {transportLabel}
        </span>
        <span className="text-[10px] text-zinc-400">
          {transport === 'sse'
            ? 'Server-Sent Events · HTTP/2 streamRequest upload + downstream SSE'
            : 'WebSocket · single bidirectional connection, pins to one shard'}
        </span>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
        <MetricCard
          label="Throughput"
          unit="msg/s · higher is better"
          hero={heroThroughput.toLocaleString()}
          heroSubtitle={`weighted geomean · ${cellsThroughput} saturation cell${cellsThroughput === 1 ? '' : 's'}`}
          subBarsHeader="Per-shard scaling"
          subBars={pickPerInstance('throughput')}
          peakLabel="Peak"
          peakValue={peakThroughput ? peakThroughput.msgPerSec.toLocaleString() : undefined}
          peakAnchor={fmtPeakAnchor(peakThroughput)}
        />
        <MetricCard
          label="Bandwidth"
          unit="MB/s · higher is better"
          hero={heroBandwidth.toLocaleString()}
          heroSubtitle={`weighted geomean · ${cellsThroughput} saturation cell${cellsThroughput === 1 ? '' : 's'}`}
          subBarsHeader="Per-shard scaling"
          subBars={pickPerInstance('bandwidth')}
          peakLabel="Peak"
          peakValue={peakBandwidth ? peakBandwidth.mbPerSec.toLocaleString() : undefined}
          peakAnchor={fmtPeakAnchor(peakBandwidth)}
        />
        <MetricCard
          label="Latency"
          unit="ms p50 · lower is better"
          lowerIsBetter
          hero={heroLatency != null ? heroLatency.toFixed(2) : '—'}
          heroSubtitle={`weighted geomean · ${cellsLatency} ack cell${cellsLatency === 1 ? '' : 's'}`}
          subBarsHeader="Per-shard scaling"
          subBars={pickPerInstance('latency')}
          peakLabel="Best"
          peakValue={bestP50?.p50 != null ? bestP50.p50.toFixed(2) : undefined}
          peakAnchor={fmtPeakAnchor(bestP50)}
        />
        <MetricCard
          label="Tail"
          unit="ms p99 · lower is better"
          lowerIsBetter
          hero={heroTail != null ? heroTail.toFixed(2) : '—'}
          heroSubtitle={`weighted geomean · ${cellsLatency} ack cell${cellsLatency === 1 ? '' : 's'}`}
          subBarsHeader="Per-shard scaling"
          subBars={pickPerInstance('tail')}
          peakLabel="Best"
          peakValue={bestP99?.p99 != null ? bestP99.p99.toFixed(2) : undefined}
          peakAnchor={fmtPeakAnchor(bestP99)}
        />
      </div>
    </div>
  )
}

/** Per-(transport × metric) score card. Hero = transport-scoped geomean; sub-bars break
 *  it down by instance count (1/3/6) so the scaling story for that specific metric on that
 *  specific transport is visible at a glance. Bars normalize within the card so the best
 *  bucket is full-width. The "best" bucket is highlighted emerald.
 *
 *  Layout (4 cards × 2 rows = SSE row, WS row):
 *    ┌─ THROUGHPUT          msg/s ─┐
 *    │ 1,250                       │  ← hero (transport-scoped weighted geomean)
 *    │ ─────────────────────────── │
 *    │ 1inst  ████░░░  1,400       │
 *    │ 3inst  ██████░  1,800       │  ← winner (emerald)
 *    │ 6inst  ██████   1,750       │
 *    │ ─────────────────────────── │
 *    │ peak 86,022 · 238 cells     │  ← anchor for the geomean
 *    └─────────────────────────────┘ */
function MetricCard({
  label,
  unit,
  lowerIsBetter = false,
  hero,
  heroSubtitle,
  subBarsHeader = 'Per-shard scaling',
  subBars,
  peakLabel = 'Peak',
  peakValue,
  peakAnchor,
}: {
  label: string
  unit: string
  lowerIsBetter?: boolean
  hero: string
  heroSubtitle?: string
  subBarsHeader?: string
  subBars: Array<{ label: string; value?: number }>
  peakLabel?: string
  peakValue?: string
  peakAnchor?: string
}) {
  // Determine bar width and winner. With lowerIsBetter, the smallest value is best and
  // gets the longest bar (min/value); otherwise value/max.
  const positives = subBars.filter((s) => typeof s.value === 'number' && (s.value as number) > 0) as Array<{
    label: string
    value: number
  }>
  let winnerLabel: string | null = null
  if (positives.length > 0) {
    winnerLabel = positives.reduce((acc, s) =>
      lowerIsBetter ? (s.value < acc.value ? s : acc) : s.value > acc.value ? s : acc,
    ).label
  }
  const extreme =
    positives.length > 0
      ? lowerIsBetter
        ? Math.min(...positives.map((s) => s.value))
        : Math.max(...positives.map((s) => s.value))
      : 0

  return (
    <div className="rounded-md border border-zinc-200 bg-white px-4 py-3 flex flex-col">
      {/* Title row: metric label + unit-with-direction */}
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-[10px] uppercase tracking-wider text-zinc-500 font-semibold">{label}</span>
        <span className="text-[10px] text-zinc-400 text-right">{unit}</span>
      </div>

      {/* Hero block: the headline number + what cells it summarizes */}
      <div className="mt-1.5">
        <div className="text-3xl font-semibold tabular-nums text-zinc-900 leading-none">{hero}</div>
        {heroSubtitle && <div className="mt-1 text-[10px] text-zinc-500 leading-tight">{heroSubtitle}</div>}
      </div>

      {/* Sub-bars: per-shard breakdown of the same cells the hero summarizes */}
      <div className="mt-3 pt-3 border-t border-zinc-100">
        <div className="text-[9px] uppercase tracking-wider text-zinc-400 font-medium mb-1.5">{subBarsHeader}</div>
        <div className="space-y-1.5">
          {subBars.map((s) => {
            const v = s.value
            const pct =
              v != null && v > 0 && extreme > 0 ? (lowerIsBetter ? (extreme / v) * 100 : (v / extreme) * 100) : 0
            const isWinner = s.label === winnerLabel
            return (
              <div key={s.label}>
                <div className="flex items-baseline justify-between text-[10px] mb-0.5 gap-2">
                  <span className={'font-mono ' + (isWinner ? 'text-emerald-700 font-semibold' : 'text-zinc-500')}>
                    {s.label}
                  </span>
                  <span
                    className={
                      'tabular-nums font-mono ' + (isWinner ? 'text-emerald-700 font-semibold' : 'text-zinc-700')
                    }
                  >
                    {v != null ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : '—'}
                  </span>
                </div>
                <div className="h-1.5 bg-zinc-100 rounded-full overflow-hidden">
                  <div
                    className={'h-full rounded-full ' + (isWinner ? 'bg-emerald-500' : 'bg-zinc-400')}
                    style={{ width: `${Math.max(0, Math.min(100, pct))}%` }}
                  />
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* Footer: best single cell — anchors the geomean against the actual maximum observed */}
      {peakValue && (
        <div className="mt-3 pt-2 border-t border-zinc-100">
          <div className="flex items-baseline gap-1.5 text-[10px]">
            <span className="uppercase tracking-wider text-zinc-400 font-medium">{peakLabel}</span>
            <span className="tabular-nums font-mono text-emerald-700 font-semibold">{peakValue}</span>
          </div>
          {peakAnchor && (
            <div className="mt-0.5 text-[10px] font-mono text-zinc-400 truncate" title={peakAnchor}>
              {peakAnchor}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
