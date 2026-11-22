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
 *  brought. */
function route(log: string[]) {
  let passed = 0
  const route: LaneRoute & { offer(lane: LaneHistory, frame: LaneFrame): void; pass(frame: LaneFrame): number } = {
    deliver: (frame, rewound) => void log.push(`${name(frame)}${rewound ? ' rewound' : ''}`),
    dropped: () => {},
    reportMissed: (count) => void log.push(`lost ${count}`),
    waitEnded: () => {},
    pass(frame) {
      const before = passed
      passed = Math.max(passed, frame.place)
      return before
    },
    offer(lane, frame) {
      lane.offer(frame, route.pass(frame))
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

  it('delivers both sides of a fork once each, under one report, as two routes bring them in either order', () => {
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
      const y12 = B.commit('y12', E)
      E.offer(history, y12)
      M.offer(history, { ...y12, via: M })
      const [first, second] = order[0] === 'E:x11' ? ['x11', 'y11'] : ['y11', 'x11']
      expect(log.slice(10)).toEqual([first, 'lost null', `${second} rewound`, 'y12'])
    }
  })

  it('reports a rewind deeper than the ring once, and delivers every new frame once, on two routes', () => {
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
      }
    }
    expect(log.filter((entry) => entry.startsWith('lost'))).toEqual(['lost null'])
    expect(log.filter((entry) => !entry.startsWith('lost')).map((entry) => entry.split(' ')[0])).toEqual([
      ...Array.from({ length: 40 }, (_, i) => `a${i + 1}`),
      ...Array.from({ length: 40 }, (_, i) => `b${i + 1}`),
    ])
  })

  it('drops a copy that reaches it after its first copy left the ring, and reports it', () => {
    const log: string[] = []
    const E = route(log)
    const M = route(log)
    const A = new Node()
    const history = lane(E, M)
    history.establish(E, fresh, [], false, E.pass)
    history.establish(M, fresh, [], false, M.pass)
    const frames = Array.from({ length: 34 }, (_, i) => A.commit(`x${i + 1}`, E))
    for (const frame of frames) E.offer(history, frame)
    M.offer(history, { ...frames[0]!, via: M })
    expect(log.slice(34)).toEqual(['lost null'])
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
