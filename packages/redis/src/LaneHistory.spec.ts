import { afterEach, describe, expect, it, vi } from 'vitest'
import { LaneHistory, type LaneFrame, type LaneRoute } from './LaneHistory.js'
import { NO_ID, type LaneStart } from './orderRecord.js'

const ids = new Map<string, bigint>()
const idOf = (name: string) => ids.get(name) ?? ids.set(name, BigInt(ids.size + 1)).get(name)!

/** A Redis node's history of one lane: each commit names the one before it, as commands.ts's scripts write it. */
class Node {
  place = 0
  last = NO_ID
  /** A replica promoted from `from`, as it stood: its next commits replace what `from` committed since. */
  static promoted(from: Node): Node {
    return Object.assign(new Node(), { place: from.place, last: from.last })
  }
  commit(name: string, via: LaneRoute): LaneFrame {
    const frame = { place: ++this.place, id: idOf(name), prev: this.last, name, via }
    this.last = frame.id
    return { lane: '', payload: new Uint8Array(), info: { seq: frame.place, timestamp: 0 }, token: null, ...frame }
  }
  start(): LaneStart {
    return { place: this.place, id: this.last }
  }
}

/** A subscription on one channel that carries the lane: what it delivers and reports, and the highest place it
 *  brought. A fork makes it read where its key stands: until `read`, what it brings waits, as an attempt's early
 *  frames do. */
function route(log: string[]) {
  let passed = 0
  let reading: LaneFrame[] | null = null
  const route: LaneRoute & {
    offer(lane: LaneHistory, frame: LaneFrame): void
    pass(frame: LaneFrame): number
    read(lane: LaneHistory, start: LaneStart, switched?: boolean): void
  } = {
    deliver: (frame, rewound) => void log.push(`${name(frame)}${rewound ? ' rewound' : ''}`),
    dropped: () => {},
    reportMissed: (count) => void log.push(`lost ${count}`),
    replaced: () => void log.push('lost null'),
    forked(frame) {
      log.push(`forked ${name(frame)}`)
      reading ??= []
      reading.push(frame)
    },
    waitEnded: () => {},
    pass(frame) {
      const before = passed
      passed = Math.max(passed, frame.place)
      return before
    },
    offer(lane, frame) {
      if (reading !== null) reading.push(frame)
      else lane.offer(frame, route.pass(frame))
    },
    read(lane, start, switched = false) {
      const early = reading ?? []
      reading = null
      lane.establish(route, start, early, true, route.pass, switched)
    },
  }
  return route
}
const name = (frame: LaneFrame) => (frame as LaneFrame & { name: string }).name

function lane(...routes: LaneRoute[]): LaneHistory {
  const history = new LaneHistory()
  for (const each of routes) history.routes.add(each)
  return history
}
const fresh: LaneStart = { place: 0, id: NO_ID }

afterEach(() => vi.useRealTimers())

describe('LaneHistory', () => {
  it('delivers a first establish’s early frames of a promotion during its start read, with no report', () => {
    const log: string[] = []
    const E = route(log)
    const A = new Node()
    for (const i of [1, 2, 3]) A.commit(`x${i}`, E)
    const B = Node.promoted(A)
    for (const i of [4, 5, 6]) A.commit(`x${i}`, E)
    const history = lane(E)
    history.establish(E, A.start(), [B.commit('y4', E), B.commit('y5', E)], false, E.pass)
    expect(log).toEqual(['y4 rewound', 'y5'])
  })

  it('drops the early frames its start covers', () => {
    const log: string[] = []
    const E = route(log)
    const A = new Node()
    const early = [1, 2, 3, 4, 5, 6].map((i) => A.commit(`x${i}`, E)).slice(3)
    const history = lane(E)
    history.establish(E, A.start(), early, false, E.pass)
    E.offer(history, A.commit('x7', E))
    expect(log).toEqual(['x7'])
  })

  it('has each route that brings a fork read, then delivers both sides once each, as two routes bring them in either order', () => {
    for (const order of [
      ['E:x11', 'E:y11', 'M:y11', 'M:x11'],
      ['E:y11', 'E:x11', 'M:x11', 'M:y11'],
    ]) {
      const log: string[] = []
      const E = route(log)
      const M = route(log)
      const A = new Node()
      const history = lane(E, M)
      history.establish(E, fresh, [], false, E.pass)
      history.establish(M, fresh, [], false, M.pass)
      for (let i = 1; i <= 10; i++) {
        const frame = A.commit(`x${i}`, E)
        E.offer(history, frame)
        M.offer(history, { ...frame, via: M })
      }
      const B = Node.promoted(A)
      const frames = { x11: A.commit('x11', E), y11: B.commit('y11', E) }
      for (const step of order) {
        const [via, which] = step.split(':') as ['E' | 'M', 'x11' | 'y11']
        const brought = via === 'E' ? E : M
        brought.offer(history, { ...frames[which], via: brought })
      }
      // B is the key's master now: each route's read finds its history.
      for (const brought of [E, M]) brought.read(history, B.start(), true)
      const y12 = B.commit('y12', E)
      E.offer(history, y12)
      M.offer(history, { ...y12, via: M })
      const [first, second] = order[0] === 'E:x11' ? ['x11', 'y11'] : ['y11', 'x11']
      expect(log.slice(10)).toEqual([first, `forked ${second}`, `forked ${second}`, `${second} rewound`, 'y12'])
    }
  })

  it('tells a rewind deeper than the ring once, from its routes’ reads, and delivers every new frame once, on two routes', () => {
    const log: string[] = []
    const E = route(log)
    const M = route(log)
    const history = lane(E, M)
    history.establish(E, fresh, [], false, E.pass)
    history.establish(M, fresh, [], false, M.pass)
    for (const prefix of ['a', 'b']) {
      // The second run starts at place 1 again, as after its order record was deleted.
      const node = new Node()
      for (let i = 1; i <= 40; i++) {
        const frame = node.commit(`${prefix}${i}`, E)
        E.offer(history, frame)
        M.offer(history, { ...frame, via: M })
        // The record each route's read finds stands at the first frame of the second run.
        if (prefix === 'b' && i === 1) for (const brought of [E, M]) brought.read(history, node.start())
      }
    }
    expect(log.filter((entry) => entry.startsWith('forked'))).toEqual(['forked b1', 'forked b1'])
    expect(log.filter((entry) => entry.startsWith('lost'))).toEqual(['lost null'])
    const delivered = log.filter((entry) => !entry.startsWith('lost') && !entry.startsWith('forked'))
    expect(delivered.map((entry) => entry.split(' ')[0])).toEqual([
      ...Array.from({ length: 40 }, (_, i) => `a${i + 1}`),
      ...Array.from({ length: 40 }, (_, i) => `b${i + 1}`),
    ])
  })

  it('drops a copy that reaches it after its first copy left the ring, with no report where its route’s read shows the lane current', () => {
    const log: string[] = []
    const E = route(log)
    const M = route(log)
    const A = new Node()
    const history = lane(E, M)
    history.establish(E, fresh, [], false, E.pass)
    history.establish(M, fresh, [], false, M.pass)
    const x1 = A.commit('x1', E)
    for (const frame of [x1, ...Array.from({ length: 33 }, (_, i) => A.commit(`x${i + 2}`, E))]) E.offer(history, frame)
    M.offer(history, { ...x1, via: M })
    M.read(history, A.start())
    expect(log.slice(34)).toEqual(['forked x1'])
  })

  it('counts a hole once its wait ends, and drops its late original', () => {
    vi.useFakeTimers()
    const log: string[] = []
    const E = route(log)
    const A = new Node()
    const history = lane(E)
    history.establish(E, fresh, [], false, E.pass)
    const [x1, x2, x3] = ['x1', 'x2', 'x3'].map((name) => A.commit(name, E)) as [LaneFrame, LaneFrame, LaneFrame]
    E.offer(history, x1)
    E.offer(history, x3)
    vi.advanceTimersByTime(1_000)
    E.offer(history, x2)
    expect(log).toEqual(['x1', 'x3', 'lost 1'])
  })

  it('on a resume, counts only what it missed of a lane its first live frame started', () => {
    const log: string[] = []
    const E = route(log)
    const A = new Node()
    const history = lane(E)
    for (const i of [1, 2, 3]) E.offer(history, A.commit(`x${i}`, E))
    A.commit('x4', E)
    history.establish(E, A.start(), [], true, E.pass)
    expect(log).toEqual(['x1', 'x2', 'x3', 'lost 1'])
  })

  it('on a resume to a history it never saw, delivers that history’s commits once, its start’s own commit too', () => {
    for (const arrives of ['early', 'live'] as const) {
      const log: string[] = []
      const E = route(log)
      const A = new Node()
      const history = lane(E)
      history.establish(E, fresh, [], false, E.pass)
      E.offer(history, A.commit('x1', E))
      // Restarted empty: the new history's first commit is the start the resume reads.
      const B = new Node()
      const y1 = B.commit('y1', E)
      history.establish(E, B.start(), arrives === 'early' ? [y1] : [], true, E.pass)
      if (arrives === 'live') E.offer(history, y1)
      E.offer(history, y1)
      E.offer(history, B.commit('y2', E))
      expect(log).toEqual(['x1', 'lost null', 'y1 rewound', 'y2'])
    }
  })

  it('on a resume ahead to a history it never saw, delivers the early frames below its last place too, in order', () => {
    const log: string[] = []
    const E = route(log)
    const A = new Node()
    const history = lane(E)
    history.establish(E, fresh, [], false, E.pass)
    E.offer(history, A.commit('x1', E))
    // Restarted empty: two commits of the new history reached the new subscription before its start read.
    const B = new Node()
    const early = [B.commit('y1', E), B.commit('y2', E)]
    history.establish(E, B.start(), early, true, E.pass)
    E.offer(history, B.commit('y3', E))
    expect(log).toEqual(['x1', 'lost null', 'y1 rewound', 'y2', 'y3'])
  })

  it('on a resume from another authority, counts nothing and reports nothing itself, at the tip, ahead or behind', () => {
    for (const form of ['tip', 'ahead', 'behind'] as const) {
      const log: string[] = []
      const E = route(log)
      const A = new Node()
      const history = lane(E)
      history.establish(E, fresh, [], false, E.pass)
      for (const i of [1, 2, 3]) E.offer(history, A.commit(`x${i}`, E))
      // The new authority holds the lane as it stood at x3, at x2, or past it with two commits the route never brought.
      const B = Object.assign(
        new Node(),
        form === 'behind' ? { place: 2, last: idOf('x2') } : { place: 3, last: idOf('x3') },
      )
      if (form === 'ahead') for (const i of [4, 5]) B.commit(`y${i}`, E)
      history.establish(E, B.start(), [], true, E.pass, true)
      E.offer(history, B.commit('y6', E))
      expect(log.slice(3)).toEqual(form === 'behind' ? ['y6 rewound'] : ['y6'])
    }
  })

  it('on a resume from another authority, delivers the new history once, its start’s own commit too', () => {
    const log: string[] = []
    const E = route(log)
    const A = new Node()
    const history = lane(E)
    history.establish(E, fresh, [], false, E.pass)
    for (const i of [1, 2]) E.offer(history, A.commit(`x${i}`, E))
    // Another history whose start the lane never saw.
    const B = new Node()
    const y1 = B.commit('y1', E)
    history.establish(E, B.start(), [y1], true, E.pass, true)
    E.offer(history, y1)
    E.offer(history, B.commit('y2', E))
    expect(log).toEqual(['x1', 'x2', 'y1 rewound', 'y2'])
  })

  it('on a resume, counts what it missed ahead, waits at a place it knows, and reports a start it doesn’t', () => {
    const log: string[] = []
    const E = route(log)
    const A = new Node()
    const history = lane(E)
    history.establish(E, fresh, [], false, E.pass)
    for (const i of [1, 2, 3]) E.offer(history, A.commit(`x${i}`, E))
    for (const i of [4, 5]) A.commit(`x${i}`, E)
    history.establish(E, A.start(), [], true, E.pass)
    expect(log.slice(3)).toEqual(['lost 2'])

    // A rollback to a place it delivered: the next frame decides. What y4 replaces was counted, never delivered.
    const B = Object.assign(new Node(), { place: 3, last: idOf('x3') })
    history.establish(E, B.start(), [], true, E.pass)
    expect(log.slice(4)).toEqual([])
    E.offer(history, B.commit('y4', E))
    expect(log.slice(4)).toEqual(['y4 rewound'])

    // A start it never saw, below what it delivered.
    const C = Object.assign(new Node(), { place: 2, last: idOf('z2') })
    history.establish(E, C.start(), [], true, E.pass)
    E.offer(history, C.commit('z3', E))
    expect(log.slice(5)).toEqual(['lost null', 'z3 rewound'])
  })
})
