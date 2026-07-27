export {
  HEAD_TRANSITIONS,
  assertHeadDeleteLegal,
  assertHeadNextWellFormed,
  assertHeadTransition,
  generateLuaHeadTransitionTable,
}

import { MAX_CLOSE_LEASE_MS, MIN_CLOSE_LEASE_MS, type HeadCx, type HeadNext, type RoomHead } from './spi.js'

type HeadState = RoomHead['state'] | 'absent'
type HeadCxForm = 'absent' | 'generic' | 'takeover' | 'finalize'
type TransitionConstraint = 'fresh-inc' | 'same-inc' | 'replace-lease' | 'none'

/** The one normative transition table. TypeScript interprets it directly; Redis embeds the generated
 * Lua table returned by generateLuaHeadTransitionTable(), so legal-transition policy has one source. */
const HEAD_TRANSITIONS = [
  { from: 'absent', cx: 'absent', to: 'open', constraint: 'fresh-inc' },
  { from: 'closed', cx: 'generic', to: 'open', constraint: 'fresh-inc' },
  { from: 'open', cx: 'generic', to: 'open', constraint: 'same-inc' },
  { from: 'open', cx: 'generic', to: 'closing', constraint: 'same-inc' },
  { from: 'closing', cx: 'takeover', to: 'closing', constraint: 'replace-lease' },
  { from: 'closing', cx: 'finalize', to: 'closed', constraint: 'none' },
] as const satisfies readonly {
  from: HeadState
  cx: HeadCxForm
  to: RoomHead['state']
  constraint: TransitionConstraint
}[]

type HeadView = Pick<RoomHead, 'currentInc' | 'state' | 'config' | 'closeLease'>

function assertHeadNextWellFormed(next: HeadNext): void {
  if ('delete' in next) return
  const { head, ttlMs } = next
  if (head.state === 'closing') {
    if (head.closeLease === undefined) throw new Error('head CX: a head entering closing must carry a close lease')
    const { durationMs } = head.closeLease
    if (!(durationMs >= MIN_CLOSE_LEASE_MS && durationMs <= MAX_CLOSE_LEASE_MS)) {
      throw new Error(
        `head CX: close lease durationMs ${durationMs} outside [${MIN_CLOSE_LEASE_MS}, ${MAX_CLOSE_LEASE_MS}]`,
      )
    }
  } else if (head.closeLease !== undefined) {
    throw new Error(`head CX: a '${head.state}' head must not carry a close lease`)
  }
  if (ttlMs !== undefined && head.state !== 'closed') {
    throw new Error(`head CX: ttlMs is only valid for a 'closed' tombstone, got '${head.state}'`)
  }
  if (head.state === 'closed' && head.currentInc !== null) {
    throw new Error('head CX: a closed tombstone must clear currentInc to null')
  }
  if (head.state !== 'closed' && head.currentInc === null) {
    throw new Error(`head CX: a '${head.state}' head must name an incarnation`)
  }
}

function assertHeadDeleteLegal(next: HeadNext, current: HeadView | null): void {
  if (!('delete' in next)) return
  if (current?.state !== 'closed') {
    throw new Error(`head CX: {delete} is legal only against a 'closed' tombstone, not '${current?.state ?? 'absent'}'`)
  }
}

function assertHeadTransition(
  cx: HeadCx,
  next: Extract<HeadNext, { head: unknown }>,
  current: HeadView | null,
  generationExists: (inc: string) => boolean,
): void {
  const from = current?.state ?? 'absent'
  const form = headCxForm(cx)
  const rule = HEAD_TRANSITIONS.find((candidate) => {
    return candidate.from === from && candidate.cx === form && candidate.to === next.head.state
  })
  const transition = `${from} + ${form} -> ${next.head.state}`
  if (rule === undefined) throw new Error(`head CX: '${transition}' is not a legal head transition`)
  switch (rule.constraint) {
    case 'fresh-inc':
      assertFreshIncarnation(next.head.currentInc, generationExists)
      return
    case 'same-inc':
      if (next.head.currentInc !== current?.currentInc) {
        throw new Error(`head CX: ${transition} must keep the same incarnation`)
      }
      return
    case 'replace-lease':
      assertReplacesOnlyTheLease(next.head, current as HeadView)
      return
    case 'none':
      return
  }
}

function generateLuaHeadTransitionTable(variable = 'HEAD_TRANSITIONS'): string {
  const rows = HEAD_TRANSITIONS.map((rule) => {
    const key = `${rule.from}|${rule.cx}|${rule.to}`
    return `  ["${key}"] = "${rule.constraint}",`
  })
  return [`local ${variable} = {`, ...rows, '}'].join('\n')
}

function headCxForm(cx: HeadCx): HeadCxForm {
  if (cx.expect === 'absent') return 'absent'
  if ('closingLeaseExpired' in cx.expect) return 'takeover'
  if ('closingLease' in cx.expect) return 'finalize'
  return 'generic'
}

function assertFreshIncarnation(inc: string | null, generationExists: (inc: string) => boolean): void {
  if (inc !== null && generationExists(inc)) {
    throw new Error(
      `head CX: incarnation '${inc}' still has surviving generation state — creating a room on it would resurrect it`,
    )
  }
}

function assertReplacesOnlyTheLease(next: Extract<HeadNext, { head: unknown }>['head'], current: HeadView): void {
  if (next.currentInc !== current.currentInc) {
    throw new Error('head CX: an expired-close takeover must keep the same incarnation')
  }
  if (!bytesEqual(next.config, current.config)) {
    throw new Error('head CX: an expired-close takeover must not change the config — it replaces only the lease')
  }
  if (next.closeLease?.id === current.closeLease?.id) {
    throw new Error('head CX: an expired-close takeover must mint a different lease id')
  }
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  return a.byteLength === b.byteLength && a.every((byte, index) => byte === b[index])
}
