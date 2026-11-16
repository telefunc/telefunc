import { describe, expect, it } from 'vitest'
import { emptyBinaryWants } from './binaryWants.js'
import { PageIndex, type PageEnd } from './PageIndex.js'
import { runAll, toggle } from '../../utils/sets.js'
import { getOrCreate } from 'telefunc/__internal'

/** Each member the index reads of an end, of the model or tells of. */
type Visits = { count: number }

/** An end holding and suppressing whom a test says, receiving every track of every member. */
class End implements PageEnd {
  readonly held = new Set<string>()
  readonly suppressing = new Set<string>()
  readonly binaryWants = emptyBinaryWants()
  suppressed: ReadonlySet<string> = new Set()
  private readonly visits: Visits

  constructor(visits: Visits) {
    this.visits = visits
    this.binaryWants.everyMember.all = true
  }

  *heldMembers(): Generator<string> {
    for (const member of this.held) {
      this.visits.count++
      yield member
    }
  }

  holds(member: string): boolean {
    return this.held.has(member)
  }

  *selfSuppressed(): Generator<string> {
    for (const member of this.suppressing) {
      this.visits.count++
      yield member
    }
  }

  suppresses(member: string): boolean {
    return this.suppressing.has(member)
  }

  atPage(suppressed: ReadonlySet<string>): void {
    this.suppressed = suppressed
  }
}

function setup() {
  const visits: Visits = { count: 0 }
  const hidden = new Set<string>()
  const model = {
    memberHidden(member: string) {
      visits.count++
      return hidden.has(member)
    },
  }
  const index = new PageIndex({ model })
  const told = new Map<string, { page: string; by?: string }>()
  const report = {
    setWho(entry: { page: string; by?: string }, on: boolean) {
      visits.count++
      if (on) told.set(JSON.stringify(entry), entry)
      else told.delete(JSON.stringify(entry))
    },
  }
  const plan = () => runAll(index.report(report))
  /** The who-entries told, once the planning pass ran. */
  const whoIsTold = () => {
    plan()
    return [...told.keys()].sort()
  }
  /** Whom each page is told as, once the planning pass ran: the participants named, or `nobody`. */
  const toldAs = () => {
    plan()
    const byPage = new Map<string, Set<string>>()
    for (const { page, by } of told.values()) getOrCreate(byPage, page, () => new Set()).add(by ?? 'nobody')
    return [...byPage.values()].map((names) => [...names].sort().join()).sort()
  }
  return { index, visits, hidden, plan, whoIsTold, toldAs, newEnd: () => new End(visits) }
}

/** The same sequence of whole numbers below `n` on every run (Mulberry32). */
function seeded(seed: number): (n: number) => number {
  let state = seed
  return (n) => {
    state = (state + 0x6d2b79f5) | 0
    let t = Math.imul(state ^ (state >>> 15), state | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return Math.floor((((t ^ (t >>> 14)) >>> 0) / 2 ** 32) * n)
  }
}

describe('PageIndex', () => {
  it('reads and tells only the member that joined through an end, however many the end holds', () => {
    const { index, visits, plan, newEnd } = setup()
    const end = newEnd()
    index.place(end)
    const visitsOfJoin = (member: string) => {
      const before = visits.count
      end.held.add(member)
      end.suppressing.add(member)
      index.endChanged(end, member)
      index.memberChanged(member)
      plan()
      return visits.count - before
    }
    // The first join also stops the page being told as nobody.
    visitsOfJoin('m0')
    const second = visitsOfJoin('m1')
    for (let i = 2; i < 1000; i++) visitsOfJoin(`m${i}`)
    expect(visitsOfJoin('m1000')).toBe(second)
  })

  it('keeps what a full re-read of its ends gives, for any order of joins, leaves, takes, lets-go and hidings', () => {
    const { index, hidden, whoIsTold, toldAs, newEnd } = setup()
    // A page with two ends, such as a view and a participant it took, and a server-side room, which is its own page.
    const view = newEnd()
    const taken = newEnd()
    const local = newEnd()
    index.place(view, 'page')
    index.place(taken, 'page')
    index.place(local)
    const ends = [view, taken, local]
    /** Whom a page's who-entries name: those its ends hold that aren't hidden, or `nobody`. */
    const named = (page: End[]) => {
      const names = [...new Set(page.flatMap((end) => [...end.held]))].filter((member) => !hidden.has(member))
      return names.length > 0 ? names.sort().join() : 'nobody'
    }
    const random = seeded(436)
    for (let round = 0; round < 100; round++) {
      for (const end of ends) {
        for (const member of ['m0', 'm1', 'm2', 'm3']) {
          if (random(3) > 0) continue
          const wasSuppressed = end.suppressed.has(member)
          toggle(end.held, member, random(2) === 1)
          toggle(end.suppressing, member, random(2) === 1)
          const changed = index.endChanged(end, member)
          const page = end === local ? [local] : [view, taken]
          const suppressed = page.some((each) => each.suppressing.has(member))
          expect([...changed]).toEqual(wasSuppressed === suppressed ? [] : [member])
          expect(end.suppressed.has(member)).toBe(suppressed)
          const holding = [[view, taken], [local]].filter((at) => at.some((each) => each.held.has(member)))
          expect(index.pagesHolding(member).length).toBe(holding.length)
          if (random(4) === 0) {
            toggle(hidden, member, !hidden.has(member))
            index.memberChanged(member)
          }
          expect(toldAs()).toEqual([named([view, taken]), named([local])].sort())
          const told = whoIsTold()
          for (const each of ends) expect([...index.endChanged(each)]).toEqual([])
          expect(whoIsTold()).toEqual(told)
        }
      }
    }
  })
})
