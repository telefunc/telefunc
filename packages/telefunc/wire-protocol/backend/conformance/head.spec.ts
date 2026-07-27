// Head invariants: I1 (linearizable CX, absence epochs, tombstone TTL), I2 (generation scoping),
// I9 (dropGeneration vs the current incarnation), I12 (the five-case closing-control matrix) and the
// whole of I13 (expired-close takeover race in two phases, skewed-caller mint, finalization race).
//
// The I13 suite is where the close ceremony's hardest property lives: an abandoned close is always
// recoverable and never stealable. Each of its four mutation killers is annotated on the assertion that
// detects it.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { RoomHead } from '../spi.js'
import { MAX_CLOSE_LEASE_MS, MIN_CLOSE_LEASE_MS } from '../spi.js'
import { type BackendFixture, installedBackends } from './harness.js'
import {
  accepted,
  binaryLane,
  bytes,
  CLOSE_LEASE_MS,
  cellsOf,
  collector,
  conflicted,
  CONTROL,
  enterClosing,
  finalizeClose,
  finalizeUnguarded,
  inboxLane,
  isStale,
  nextId,
  okDeleted,
  okHead,
  openRoom,
  readHeadOrThrow,
  SEMANTIC,
  takeoverClose,
  text,
  TOMBSTONE_TTL_MS,
} from './scenario.js'

const YEAR_MS = 365 * 24 * 60 * 60 * 1000

for (const harness of installedBackends) {
  describe(`head — ${harness.name}`, () => {
    let fx: BackendFixture

    beforeEach(async () => {
      fx = await harness.create()
    })

    afterEach(async () => {
      vi.useRealTimers()
      await fx.dispose()
    })

    // ── I1 ──

    describe('I1 — linearizable head CX', () => {
      it('creates on a fresh id and refuses a second create in the same absence epoch', async () => {
        const roomId = nextId('room')
        const created = okHead(
          await fx.backend.compareExchangeHead(
            roomId,
            { expect: 'absent' },
            { head: { currentInc: 'inc-a', state: 'open', config: bytes('cfg') } },
          ),
        )
        expect(created.state).toBe('open')
        expect(created.currentInc).toBe('inc-a')

        const second = conflicted(
          await fx.backend.compareExchangeHead(
            roomId,
            { expect: 'absent' },
            { head: { currentInc: 'inc-b', state: 'open', config: bytes('cfg') } },
          ),
        )
        expect(second?.rev).toBe(created.rev)
        expect(second?.currentInc).toBe('inc-a')
      })

      it('returns the STORED head on every write, with a fresh rev and the caller config', async () => {
        const roomId = nextId('room')
        const { head } = await openRoom(fx.backend, roomId, { config: bytes('v1') })
        const updated = okHead(
          await fx.backend.compareExchangeHead(
            roomId,
            { expect: { rev: head.rev } },
            { head: { currentInc: head.currentInc, state: 'open', config: bytes('v2') } },
          ),
        )
        expect(updated.rev).not.toBe(head.rev)
        expect(text(updated.config)).toBe('v2')
        expect(text((await readHeadOrThrow(fx.backend, roomId)).config)).toBe('v2')
      })

      it('conflicts on a stale rev and hands back the current head', async () => {
        const roomId = nextId('room')
        const { head } = await openRoom(fx.backend, roomId)
        const moved = okHead(
          await fx.backend.compareExchangeHead(
            roomId,
            { expect: { rev: head.rev } },
            { head: { currentInc: head.currentInc, state: 'open', config: bytes('moved') } },
          ),
        )
        const current = conflicted(
          await fx.backend.compareExchangeHead(
            roomId,
            { expect: { rev: head.rev } },
            { head: { currentInc: head.currentInc, state: 'open', config: bytes('late') } },
          ),
        )
        expect(current?.rev).toBe(moved.rev)
        expect(text(current?.config ?? bytes(''))).toBe('moved')
      })

      it('conflicts against an absent head with a null current', async () => {
        const current = conflicted(
          await fx.backend.compareExchangeHead(
            nextId('room'),
            { expect: { rev: 'never-existed' } },
            { head: { currentInc: 'inc', state: 'open', config: bytes('cfg') } },
          ),
        )
        expect(current).toBeNull()
      })

      it('serializes concurrent CXs issued from one read: exactly one commits', async () => {
        const roomId = nextId('room')
        const { head } = await openRoom(fx.backend, roomId)
        const results = await Promise.all(
          [1, 2, 3, 4, 5].map((n) =>
            fx.backend.compareExchangeHead(
              roomId,
              { expect: { rev: head.rev } },
              { head: { currentInc: head.currentInc, state: 'open', config: bytes(`writer-${n}`) } },
            ),
          ),
        )
        expect(results.filter((result) => 'ok' in result)).toHaveLength(1)
        expect(results.filter((result) => 'conflict' in result)).toHaveLength(4)
      })

      it('discriminates a delete success from a head success and never fabricates a head', async () => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

        const deleted = await fx.backend.compareExchangeHead(
          roomId,
          { expect: { rev: tombstone.rev } },
          { delete: true },
        )
        okDeleted(deleted)
        expect('head' in deleted).toBe(false)
        expect(await fx.backend.readHead(roomId)).toBeNull()
      })

      it('reads a lapsed tombstone as absent, which reopens the absence epoch', async () => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        okHead(await finalizeClose(fx.backend, roomId, closing, leaseId, TOMBSTONE_TTL_MS))

        expect(await fx.backend.readHead(roomId)).not.toBeNull()
        fx.advanceAuthority(TOMBSTONE_TTL_MS + 1)
        expect(await fx.backend.readHead(roomId)).toBeNull()

        // a fresh absence epoch: expect:'absent' succeeds again
        const recreated = okHead(
          await fx.backend.compareExchangeHead(
            roomId,
            { expect: 'absent' },
            { head: { currentInc: 'inc-fresh', state: 'open', config: bytes('cfg') } },
          ),
        )
        expect(recreated.state).toBe('open')
      })
    })

    // ── I2 ──

    describe('I2 — every datum lives under exactly one incarnation', () => {
      it('leaves a recreated incarnation empty and the old generation intact', async () => {
        const roomId = nextId('room')
        const { inc: oldInc, head } = await openRoom(fx.backend, roomId)
        const before = cellsOf(await fx.backend.readCells(roomId, oldInc, { prefix: '' }))
        expect(
          await fx.backend.compareExchangeCells(roomId, oldInc, before.revision, [
            { key: 'member/a', set: { bytes: bytes('A') } },
          ]),
        ).toBe('committed')
        accepted(await fx.backend.commitLane(roomId, oldInc, SEMANTIC, bytes('K1'), { retain: true }))

        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        accepted(await fx.backend.commitLane(roomId, oldInc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
        const { inc: newInc } = await openRoom(fx.backend, roomId, { prior: tombstone })

        const fresh = cellsOf(await fx.backend.readCells(roomId, newInc, { prefix: '' }))
        expect(fresh.cells.size).toBe(0)
        expect(await fx.backend.readRetained(roomId, newInc, SEMANTIC)).toBeNull()

        // the old generation is unreachable through the head but physically untouched until it is dropped
        expect(await fx.backend.readCells(roomId, oldInc, { prefix: '' })).toEqual({ staleInc: true })
        const retained = await fx.backend.readRetained(roomId, oldInc, SEMANTIC)
        expect(retained === null ? null : text(retained.payload)).toBe('K1')
      })
    })

    // ── I9 ──

    describe('I9 — generation lifecycle against the head', () => {
      it('refuses to drop the current incarnation', async () => {
        const roomId = nextId('room')
        const { inc } = await openRoom(fx.backend, roomId)
        await expect(fx.backend.dropGeneration(roomId, inc)).rejects.toThrow()
        expect(await fx.backend.listGenerations(roomId)).toContain(inc)
      })

      it('clears currentInc on finalization, which is what makes the old generation droppable', async () => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))

        // still current while closing — the janitor may not touch it yet
        await expect(fx.backend.dropGeneration(roomId, inc)).rejects.toThrow()

        const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
        expect(tombstone.currentInc).toBeNull()
        await fx.backend.dropGeneration(roomId, inc)
        expect(await fx.backend.listGenerations(roomId)).not.toContain(inc)
      })
    })

    // ── I12 ──

    describe('I12 — the closing-control precondition matrix', () => {
      let roomId: string
      let inc: string
      let closing: RoomHead
      let leaseId: string

      beforeEach(async () => {
        roomId = nextId('room')
        const opened = await openRoom(fx.backend, roomId)
        inc = opened.inc
        const entered = await enterClosing(fx.backend, roomId, opened.head)
        closing = entered.head
        leaseId = entered.leaseId
      })

      it('case 1 — semantic, binary and inbox commits are stale, with or without the closing lease', async () => {
        // KILLER: honoring the closing lease off the control lane turns this red.
        for (const lane of [SEMANTIC, binaryLane('alice', 'cam'), inboxLane('alice')]) {
          expect(isStale(await fx.backend.commitLane(roomId, inc, lane, bytes('x'), { closingLease: leaseId }))).toBe(
            true,
          )
          expect(isStale(await fx.backend.commitLane(roomId, inc, lane, bytes('x')))).toBe(true)
        }
      })

      it('case 2 — control with a wrong lease id is stale', async () => {
        // KILLER: dropping the lease-id compare turns this red.
        expect(
          isStale(
            await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: 'not-the-lease' }),
          ),
        ).toBe(true)
      })

      it('case 3 — control with the correct lease id but an EXPIRED lease is stale', async () => {
        // KILLER: ignoring closeLease.until in the commit precondition turns this red. The compare is in
        // authority time, so the scenario advances the backend's clock rather than waiting.
        fx.advanceAuthority(CLOSE_LEASE_MS + 1)
        expect(
          isStale(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId })),
        ).toBe(true)
      })

      it('case 4 — control with the correct unexpired lease is accepted, repeatably', async () => {
        const first = accepted(
          await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }),
        )
        // a resumed closer may retry its closed event while the head is still closing
        const second = accepted(
          await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }),
        )
        expect(second.seq).toBeGreaterThan(first.seq)
        await Promise.all([first.delivery, second.delivery])
      })

      it('case 5 — after closing→closed everything is stale, including control with the old lease', async () => {
        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

        expect(
          isStale(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('again'), { closingLease: leaseId })),
        ).toBe(true)
        expect(isStale(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('again')))).toBe(true)
        expect(isStale(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('again')))).toBe(true)
      })
    })

    // ── I13 ──

    describe('I13 (a) — expired-close takeover race', () => {
      it('phase 1 (pre-expiry): takeover conflicts and the original closer can still emit and finalize', async () => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        // the closer is paused here: it has entered closing but not yet emitted its closed event
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)

        // KILLER: making lease replacement unconditional (ignoring `until`) turns this red — the
        // pre-expiry takeover would succeed where a conflict is asserted.
        const { result } = await takeoverClose(fx.backend, roomId, closing)
        const current = conflicted(result)
        expect(current?.rev).toBe(closing.rev)
        expect(current?.closeLease).toEqual(closing.closeLease)

        // the head revision and lease are untouched by the failed takeover
        const after = await readHeadOrThrow(fx.backend, roomId)
        expect(after.rev).toBe(closing.rev)
        expect(after.closeLease?.id).toBe(leaseId)

        // and the original closer completes its tail normally
        const event = accepted(
          await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }),
        )
        await event.delivery
        const tombstone = okHead(await finalizeClose(fx.backend, roomId, after, leaseId))
        expect(tombstone.state).toBe('closed')
        expect(tombstone.currentInc).toBeNull()
      })

      it('phase 2 (expired): exactly one recovery actor wins, completes the close, and recreation succeeds', async () => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        const { head: closing, leaseId: abandoned } = await enterClosing(fx.backend, roomId, head)

        // KILLER: comparing expiry against a local caller clock turns this red — authority time moves
        // here and the caller clock does not.
        fx.advanceAuthority(CLOSE_LEASE_MS + 1)

        const seen = await readHeadOrThrow(fx.backend, roomId)
        const [first, second] = await Promise.all([
          takeoverClose(fx.backend, roomId, seen),
          takeoverClose(fx.backend, roomId, seen),
        ])
        const attempts = [first, second]
        const winners = attempts.filter((attempt) => 'ok' in attempt.result)
        const losers = attempts.filter((attempt) => 'conflict' in attempt.result)
        expect(winners).toHaveLength(1)
        expect(losers).toHaveLength(1)

        const winner = winners[0] as (typeof attempts)[number]
        const fresh = okHead(winner.result)
        expect(fresh.state).toBe('closing')
        expect(fresh.currentInc).toBe(inc) // takeover replaces the lease, never the incarnation
        expect(fresh.closeLease?.id).toBe(winner.leaseId)
        expect(fresh.closeLease?.id).not.toBe(abandoned)
        expect(fresh.closeLease?.until).toBe(fx.authorityNow() + CLOSE_LEASE_MS) // authority-fresh again

        // the loser is told to re-read
        expect(conflicted((losers[0] as (typeof attempts)[number]).result)?.rev).toBe(fresh.rev)

        // the abandoned lease is now stale for commits AND for finalization
        expect(
          isStale(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: abandoned })),
        ).toBe(true)
        conflicted(await finalizeClose(fx.backend, roomId, fresh, abandoned))

        // the winner finishes the normal tail
        const event = accepted(
          await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: winner.leaseId }),
        )
        await event.delivery
        const tombstone = okHead(await finalizeClose(fx.backend, roomId, fresh, winner.leaseId))
        expect(tombstone.currentInc).toBeNull()
        await fx.backend.dropGeneration(roomId, inc)
        expect(await fx.backend.listGenerations(roomId)).not.toContain(inc)

        // immediate recreation on the tombstone
        const recreated = await openRoom(fx.backend, roomId, { prior: tombstone })
        expect(recreated.head.state).toBe('open')
        expect(recreated.inc).not.toBe(inc)
      })
    })

    describe('I13 (b) — skewed-caller mint', () => {
      it.each([
        ['far ahead', 10 * YEAR_MS],
        ['far behind', -10 * YEAR_MS],
      ])('mints a bounded, authority-fresh lease when the caller clock is %s', async (_label, skewMs) => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)

        // KILLER: computing `until` from the caller clock (or accepting a caller deadline) turns this
        // red in both directions — far ahead makes the lease effectively permanent, far behind makes it
        // born expired.
        vi.useFakeTimers({ toFake: ['Date'] })
        vi.setSystemTime(fx.authorityNow() + skewMs)
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        vi.useRealTimers()

        expect(closing.closeLease?.until).toBe(fx.authorityNow() + CLOSE_LEASE_MS)

        // and behaviourally fresh: accepted now, stale once AUTHORITY time passes the deadline
        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        fx.advanceAuthority(CLOSE_LEASE_MS + 1)
        expect(
          isStale(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId })),
        ).toBe(true)
      })

      it('rejects a close-lease duration outside the normative bounds', async () => {
        const roomId = nextId('room')
        const { head } = await openRoom(fx.backend, roomId)
        for (const durationMs of [MIN_CLOSE_LEASE_MS - 1, MAX_CLOSE_LEASE_MS + 1]) {
          await expect(enterClosing(fx.backend, roomId, head, { durationMs })).rejects.toThrow()
        }
        for (const durationMs of [MIN_CLOSE_LEASE_MS, MAX_CLOSE_LEASE_MS]) {
          const room = nextId('room')
          const opened = await openRoom(fx.backend, room)
          const { head: closing } = await enterClosing(fx.backend, room, opened.head, { durationMs })
          expect(closing.closeLease?.until).toBe(fx.authorityNow() + durationMs)
        }
      })
    })

    describe('I13 (c) — finalization race', () => {
      // Both CXs are ISSUED from ONE paused head in a single synchronous turn with no intervening await,
      // then both are awaited. On a backend whose CX applies atomically-synchronously the first call has
      // already stored before the second is issued — the two are never simultaneously in flight, and no
      // test can make them be. That is not a weakness of the test: such a backend admits exactly two
      // schedules, so asserting BOTH serial linearizations exhausts the schedule space and IS the race
      // (spi.md I13 race-realization note). A backend with genuinely asynchronous CX application must
      // additionally run the barrier-forced variant below.
      const pauseBetweenEventAndFinalization = async () => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        const control = collector()
        const sub = fx.backend.subscribeLane(roomId, inc, CONTROL, control.receiver)
        await sub.ready

        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        const event = accepted(
          await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed-by-original'), { closingLease: leaseId }),
        )
        await event.delivery
        fx.advanceAuthority(CLOSE_LEASE_MS + 1)
        return { roomId, inc, paused: closing, originalLease: leaseId, control }
      }

      it('linearization (a): the original finalizes first and the takeover conflicts', async () => {
        const { roomId, paused, originalLease, control } = await pauseBetweenEventAndFinalization()

        // issued together, awaited together — no await separates the two calls
        const finalization = finalizeClose(fx.backend, roomId, paused, originalLease)
        const takeover = takeoverClose(fx.backend, roomId, paused)
        const [finalized, taken] = await Promise.all([finalization, takeover])

        // the original's lease is EXPIRED, yet finalization is id-equality only — so it wins
        const tombstone = okHead(finalized)
        expect(tombstone.state).toBe('closed')
        expect(tombstone.currentInc).toBeNull()
        conflicted(taken.result)
        // and the head went null only after the finalizing closer's own accepted event
        expect(control.payloads()).toEqual(['closed-by-original'])
      })

      it('linearization (b): the takeover wins, the original aborts, and only the winner clears the head', async () => {
        const { roomId, inc, paused, originalLease, control } = await pauseBetweenEventAndFinalization()

        // same pair, opposite issue order — the other of the two schedules
        const takeover = takeoverClose(fx.backend, roomId, paused)
        const finalization = finalizeClose(fx.backend, roomId, paused, originalLease)
        const [taken, finalized] = await Promise.all([takeover, finalization])

        const fresh = okHead(taken.result)
        expect(fresh.state).toBe('closing')
        expect(fresh.closeLease?.id).toBe(taken.leaseId)
        conflicted(finalized) // the superseded closer aborts its tail

        // no other head-null path exists for the superseded closer: not a re-read with its old lease…
        const still = await readHeadOrThrow(fx.backend, roomId)
        expect(still.state).toBe('closing')
        expect(still.currentInc).toBe(inc)
        conflicted(await finalizeClose(fx.backend, roomId, still, originalLease))
        // …and not the generic {rev} form, which is off the normative transition table entirely.
        // KILLER: admitting generic finalization, or dropping the lease-id compare, turns this red.
        await expect(finalizeUnguarded(fx.backend, roomId, still)).rejects.toThrow()
        expect(control.payloads()).toEqual(['closed-by-original'])

        // only the winner's OWN event, then its guarded finalization, clears the head
        const winnerEvent = accepted(
          await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed-by-winner'), {
            closingLease: taken.leaseId,
          }),
        )
        await winnerEvent.delivery
        expect(control.payloads()).toEqual(['closed-by-original', 'closed-by-winner'])
        const tombstone = okHead(await finalizeClose(fx.backend, roomId, still, taken.leaseId))
        expect(tombstone.state).toBe('closed')
        expect(tombstone.currentInc).toBeNull()
      })

      it('carries the barrier-forced concurrent obligation to backends whose CX is not synchronous', () => {
        if (fx.traces.cxAppliesSynchronously) return // the two linearizations above already exhaust it
        if (fx.concurrentHeadCxBarrier === undefined) {
          throw new Error(
            'this backend applies head CXs asynchronously, so the two serial linearizations do NOT ' +
              'exhaust I13(c). It must supply BackendFixture.concurrentHeadCxBarrier and run the ' +
              'barrier-forced variant — queue both CXs, assert both pending, release in each order ' +
              '(spi.md I13 race-realization note).',
          )
        }
      })

      it('lets an UNREPLACED closer finalize after its deadline passes (id equality, never expiry)', async () => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))

        // time passes and nobody took over: the lease is expired for COMMITS but still the current id
        fx.advanceAuthority(CLOSE_LEASE_MS + 1)
        expect(
          isStale(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('late'), { closingLease: leaseId })),
        ).toBe(true)
        const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
        expect(tombstone.currentInc).toBeNull()
      })
    })

    // ── the normative transition table ──

    describe('the normative head-transition table (spi.md §2)', () => {
      // Validation is by the triple (current state, HeadCx form, next); everything off the table throws.
      // These are the negative cases: each proves a guarded transition cannot be reached through some
      // OTHER compare form. Every backend must reject the same set.
      const closingHead = (inc: string | null, config: Uint8Array, leaseId: string) => ({
        head: {
          currentInc: inc,
          state: 'closing' as const,
          config,
          closeLease: { id: leaseId, durationMs: CLOSE_LEASE_MS },
        },
      })
      const openHead = (inc: string | null, config: Uint8Array) => ({
        head: { currentInc: inc, state: 'open' as const, config },
      })
      const closedHead = (config: Uint8Array) => ({
        head: { currentInc: null, state: 'closed' as const, config },
        ttlMs: TOMBSTONE_TTL_MS,
      })

      const expectHeadError = async (operation: Promise<unknown>, message: string) => {
        const error = await operation.then(
          () => null,
          (reason: unknown) => reason,
        )
        expect(error).toEqual(new Error(message))
      }

      it('rejects illegal transition keys and constraints with the same exact errors', async () => {
        await expectHeadError(
          fx.backend.compareExchangeHead(
            nextId('room'),
            { expect: 'absent' },
            closingHead('inc', bytes('cfg'), 'lease'),
          ),
          "head CX: 'absent + absent -> closing' is not a legal head transition",
        )

        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        await expectHeadError(
          fx.backend.compareExchangeHead(roomId, { expect: { rev: head.rev } }, closedHead(head.config)),
          "head CX: 'open + generic -> closed' is not a legal head transition",
        )
        await expectHeadError(
          fx.backend.compareExchangeHead(roomId, { expect: { rev: head.rev } }, openHead('smuggled', head.config)),
          'head CX: open + generic -> open must keep the same incarnation',
        )

        const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
        await expectHeadError(
          fx.backend.compareExchangeHead(
            roomId,
            { expect: { rev: closing.rev } },
            closingHead(inc, closing.config, 'stolen'),
          ),
          "head CX: 'closing + generic -> closing' is not a legal head transition",
        )
        fx.advanceAuthority(CLOSE_LEASE_MS + 1)
        const takeoverCx = { expect: { rev: closing.rev, closingLeaseExpired: true as const } }
        await expectHeadError(
          fx.backend.compareExchangeHead(roomId, takeoverCx, closingHead(inc, bytes('rewritten'), 'fresh')),
          'head CX: an expired-close takeover must not change the config — it replaces only the lease',
        )
        await expectHeadError(
          fx.backend.compareExchangeHead(roomId, takeoverCx, closingHead(inc, closing.config, leaseId)),
          'head CX: an expired-close takeover must mint a different lease id',
        )
        await expectHeadError(
          fx.backend.compareExchangeHead(roomId, takeoverCx, closedHead(closing.config)),
          "head CX: 'closing + takeover -> closed' is not a legal head transition",
        )
      })

      // FRESH-INC: create/recreate must refuse an incarnation that still has surviving generation state,
      // otherwise an old generation's cells silently become the "new" room's state.
      describe('stale-generation resurrection', () => {
        // Close a room WITHOUT dropping its generation, leaving an orphan whose id a caller might reuse.
        const orphanGeneration = async () => {
          const roomId = nextId('room')
          const { inc, head } = await openRoom(fx.backend, roomId)
          const seeded = cellsOf(await fx.backend.readCells(roomId, inc, { prefix: '' }))
          expect(
            await fx.backend.compareExchangeCells(roomId, inc, seeded.revision, [
              { key: 'member/old', set: { bytes: bytes('ghost') } },
            ]),
          ).toBe('committed')
          accepted(await fx.backend.commitLane(roomId, inc, SEMANTIC, bytes('ghost-retained'), { retain: true }))

          const { head: closing, leaseId } = await enterClosing(fx.backend, roomId, head)
          accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
          const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))
          expect(await fx.backend.listGenerations(roomId)).toContain(inc) // the orphan survives
          return { roomId, orphanInc: inc, tombstone }
        }

        it('refuses recreating a tombstoned room on an incarnation that still has state', async () => {
          // KILLER: admitting a reused inc turns this red — the probe that found it seeded member/old,
          // finalized without dropping, recreated on the old id, and read the ghost back as new state.
          const { roomId, orphanInc, tombstone } = await orphanGeneration()

          await expect(
            fx.backend.compareExchangeHead(
              roomId,
              { expect: { rev: tombstone.rev } },
              { head: { currentInc: orphanInc, state: 'open', config: tombstone.config } },
            ),
          ).rejects.toThrow(/surviving generation state/)

          // the ghost never became current: the room is still a tombstone
          const stillTombstone = await readHeadOrThrow(fx.backend, roomId)
          expect(stillTombstone.state).toBe('closed')
          expect(stillTombstone.currentInc).toBeNull()
        })

        it('refuses creating on a surviving incarnation after the tombstone lapses to absent', async () => {
          const { roomId, orphanInc } = await orphanGeneration()
          fx.advanceAuthority(TOMBSTONE_TTL_MS + 1)
          expect(await fx.backend.readHead(roomId)).toBeNull() // reads as absent…

          await expect(
            fx.backend.compareExchangeHead(
              roomId,
              { expect: 'absent' },
              { head: { currentInc: orphanInc, state: 'open', config: bytes('cfg') } },
            ),
          ).rejects.toThrow(/surviving generation state/)
          expect(await fx.backend.listGenerations(roomId)).toContain(orphanInc) // …but the orphan is real
        })

        it('admits a fresh incarnation, whose generation starts empty', async () => {
          const { roomId, orphanInc, tombstone } = await orphanGeneration()
          const fresh = okHead(
            await fx.backend.compareExchangeHead(
              roomId,
              { expect: { rev: tombstone.rev } },
              { head: { currentInc: 'genuinely-fresh', state: 'open', config: tombstone.config } },
            ),
          )
          expect(fresh.currentInc).toBe('genuinely-fresh')
          expect(cellsOf(await fx.backend.readCells(roomId, 'genuinely-fresh', { prefix: '' })).cells.size).toBe(0)
          expect(await fx.backend.readRetained(roomId, 'genuinely-fresh', SEMANTIC)).toBeNull()
          expect(await fx.backend.listGenerations(roomId)).toContain(orphanInc) // the orphan is untouched
        })

        it('the positive twin: a FULLY DROPPED id is reusable and inert', async () => {
          // A dropped generation exposes nothing, so its id is harmless by construction.
          const { roomId, orphanInc, tombstone } = await orphanGeneration()
          await fx.backend.dropGeneration(roomId, orphanInc)
          expect(await fx.backend.listGenerations(roomId)).not.toContain(orphanInc)

          const reborn = okHead(
            await fx.backend.compareExchangeHead(
              roomId,
              { expect: { rev: tombstone.rev } },
              { head: { currentInc: orphanInc, state: 'open', config: tombstone.config } },
            ),
          )
          expect(reborn.currentInc).toBe(orphanInc)
          // nothing of the old generation surfaces under the reused id
          expect(cellsOf(await fx.backend.readCells(roomId, orphanInc, { prefix: '' })).cells.size).toBe(0)
          expect(await fx.backend.readRetained(roomId, orphanInc, SEMANTIC)).toBeNull()
          expect(accepted(await fx.backend.commitLane(roomId, orphanInc, SEMANTIC, bytes('new'))).seq).toBe(1)
        })
      })

      it('CONFLICTS (never throws) when a guarded form is used on a head that is not closing', async () => {
        // A genuine race must stay a conflict: only the compare can fail here, so the caller re-reads.
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        conflicted(
          await fx.backend.compareExchangeHead(
            roomId,
            { expect: { rev: head.rev, closingLeaseExpired: true } },
            closingHead(inc, head.config, 'fresh'),
          ),
        )
        conflicted(
          await fx.backend.compareExchangeHead(
            roomId,
            { expect: { rev: head.rev, closingLease: 'whatever' } },
            closedHead(head.config),
          ),
        )
      })
    })

    describe('{delete:true} is legal only against a closed tombstone', () => {
      const enterClosingRoom = async () => {
        const roomId = nextId('room')
        const { inc, head } = await openRoom(fx.backend, roomId)
        const entered = await enterClosing(fx.backend, roomId, head)
        return { roomId, inc, ...entered }
      }

      it('throws against an ABSENT head, even though the compare would succeed', async () => {
        // KILLER: treating an absent head as deletable turns this red. Operation legality is decided
        // before the compare, so a delete can never become a second route to head-null.
        await expect(
          fx.backend.compareExchangeHead(nextId('room'), { expect: 'absent' }, { delete: true }),
        ).rejects.toThrow(/only against a 'closed' tombstone/)
      })

      it('throws against an open head', async () => {
        const roomId = nextId('room')
        const { head } = await openRoom(fx.backend, roomId)
        await expect(
          fx.backend.compareExchangeHead(roomId, { expect: { rev: head.rev } }, { delete: true }),
        ).rejects.toThrow(/only against a 'closed' tombstone/)
        expect((await readHeadOrThrow(fx.backend, roomId)).state).toBe('open')
      })

      it('throws against a closing head', async () => {
        const { roomId, head: closing } = await enterClosingRoom()
        await expect(
          fx.backend.compareExchangeHead(roomId, { expect: { rev: closing.rev } }, { delete: true }),
        ).rejects.toThrow(/only against a 'closed' tombstone/)
        expect((await readHeadOrThrow(fx.backend, roomId)).state).toBe('closing')
      })

      it('succeeds against a closed tombstone and conflicts on a stale rev', async () => {
        const { roomId, inc, head: closing, leaseId } = await enterClosingRoom()
        accepted(await fx.backend.commitLane(roomId, inc, CONTROL, bytes('closed'), { closingLease: leaseId }))
        const tombstone = okHead(await finalizeClose(fx.backend, roomId, closing, leaseId))

        conflicted(await fx.backend.compareExchangeHead(roomId, { expect: { rev: 'stale' } }, { delete: true }))
        okDeleted(await fx.backend.compareExchangeHead(roomId, { expect: { rev: tombstone.rev } }, { delete: true }))
        expect(await fx.backend.readHead(roomId)).toBeNull()
      })
    })
  })
}
