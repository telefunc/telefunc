import { describe, expect, it } from 'vitest'
import { HEAD_TRANSITIONS, assertHeadTransition, generateLuaHeadTransitionTable } from './head-transitions.js'
import type { HeadCx, HeadNext, RoomHead } from './spi.js'

describe('shared head-transition rules', () => {
  it('generates every Lua lookup row from the normative data table', () => {
    const lua = generateLuaHeadTransitionTable()

    expect(HEAD_TRANSITIONS).toHaveLength(6)
    for (const rule of HEAD_TRANSITIONS) {
      expect(lua).toContain(`["${rule.from}|${rule.cx}|${rule.to}"] = "${rule.constraint}"`)
    }
    expect(lua.match(/\["/g)).toHaveLength(HEAD_TRANSITIONS.length)
  })

  it.each([
    ['absent create', { expect: 'absent' }, null, next('open', 'inc-1')],
    ['closed recreation', { expect: { rev: 'r1' } }, head('closed', null), next('open', 'inc-1')],
    ['open edit', { expect: { rev: 'r1' } }, head('open', 'inc-1'), next('open', 'inc-1')],
    ['open close', { expect: { rev: 'r1' } }, head('open', 'inc-1'), next('closing', 'inc-1', 'lease-1')],
    [
      'expired takeover',
      { expect: { rev: 'r1', closingLeaseExpired: true } },
      head('closing', 'inc-1', 'lease-1'),
      next('closing', 'inc-1', 'lease-2'),
    ],
    [
      'lease finalization',
      { expect: { rev: 'r1', closingLease: 'lease-1' } },
      head('closing', 'inc-1', 'lease-1'),
      next('closed', null),
    ],
  ] satisfies Array<[string, HeadCx, RoomHead | null, Extract<HeadNext, { head: unknown }>]>)(
    'accepts the %s row through the TypeScript interpreter',
    (_name, cx, current, candidate) => {
      expect(() => assertHeadTransition(cx, candidate, current, () => false)).not.toThrow()
    },
  )

  it('rejects a transition absent from the shared data table', () => {
    expect(() =>
      assertHeadTransition({ expect: { rev: 'r1' } }, next('closed', null), head('open', 'inc-1'), () => false),
    ).toThrow("head CX: 'open + generic -> closed' is not a legal head transition")
  })

  it('enforces every constraint named by the shared table', () => {
    expect(() => assertHeadTransition({ expect: 'absent' }, next('open', 'inc-1'), null, () => true)).toThrow(
      "head CX: incarnation 'inc-1' still has surviving generation state",
    )
    expect(() =>
      assertHeadTransition(
        { expect: { rev: 'r1' } },
        next('closing', 'inc-2', 'lease-1'),
        head('open', 'inc-1'),
        () => false,
      ),
    ).toThrow('head CX: open + generic -> closing must keep the same incarnation')

    const takeover = { expect: { rev: 'r1', closingLeaseExpired: true as const } }
    const current = head('closing', 'inc-1', 'lease-1')
    expect(() => assertHeadTransition(takeover, next('closing', 'inc-2', 'lease-2'), current, () => false)).toThrow(
      'head CX: an expired-close takeover must keep the same incarnation',
    )
    expect(() =>
      assertHeadTransition(
        takeover,
        {
          ...next('closing', 'inc-1', 'lease-2'),
          head: { ...next('closing', 'inc-1', 'lease-2').head, config: new Uint8Array([2]) },
        },
        current,
        () => false,
      ),
    ).toThrow('head CX: an expired-close takeover must not change the config — it replaces only the lease')
    expect(() => assertHeadTransition(takeover, next('closing', 'inc-1', 'lease-1'), current, () => false)).toThrow(
      'head CX: an expired-close takeover must mint a different lease id',
    )
  })
})

function head(state: RoomHead['state'], currentInc: string | null, leaseId?: string): RoomHead {
  return {
    rev: 'r1',
    state,
    currentInc,
    config: new Uint8Array([1]),
    ...(leaseId === undefined ? {} : { closeLease: { id: leaseId, until: 10 } }),
  }
}

function next(
  state: RoomHead['state'],
  currentInc: string | null,
  leaseId?: string,
): Extract<HeadNext, { head: unknown }> {
  return {
    head: {
      state,
      currentInc,
      config: new Uint8Array([1]),
      ...(leaseId === undefined ? {} : { closeLease: { id: leaseId, durationMs: 1_000 } }),
    },
  }
}
