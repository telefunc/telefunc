import { describe, expect, it } from 'vitest'
import { publishBody } from './PublishBody.js'
import { ServerReplay } from './ServerReplay.js'

/** A replay of no wire, its publishes at `behind` in the streams named, each `null` one at `never`. */
function replayOf(streams: (string | null)[]): { replay: ServerReplay; seqs: number[] } {
  const replay = new ServerReplay({ heldBytes: 0 })
  const seqs = streams.map((stream) => {
    const seq = replay.nextSeq()
    const shed = stream === null ? ({ level: 'never' } as const) : ({ level: 'behind', stream } as const)
    replay.pushPublish(seq, new Uint8Array(8), publishBody('"x"', { seq, timestamp: 0 }, shed))
    return seq
  })
  return { replay, seqs }
}

describe('ServerReplay.dropOldestPublishes', () => {
  it("keeps the newest publish of each stream, and every one its page can't miss", () => {
    const { replay, seqs } = replayOf(['a', 'a', 'a', null, 'b', 'b'])
    const [a1, a2, , , b1] = seqs
    expect(replay.dropOldestPublishes(1, Number.POSITIVE_INFINITY)).toEqual({ seqs: [a1, a2, b1], keptOver: true })
  })

  it('lets every one go at 0', () => {
    const { replay, seqs } = replayOf(['a', null, 'b'])
    expect(replay.dropOldestPublishes(0, Number.POSITIVE_INFINITY)).toEqual({ seqs, keptOver: false })
  })
})
