import { expect, test, vi } from 'vitest'

import { decode, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { PublishBody } from './PublishBody.js'
import { ServerReplay } from './ServerReplay.js'

test('partial confirmation and dropping a queued publish keep the newest route and timing', () => {
  vi.useFakeTimers({ toFake: ['performance'] })
  const holder = { heldBytes: 0 }
  const replay = new ServerReplay(holder)
  const bodies = [1, 3, 4, 5, 6].map(
    (seq) => new PublishBody(encode.publishBinary(Uint8Array.of(seq), { seq, timestamp: 1 })),
  )
  const push = (index: number, body: PublishBody | undefined) => {
    if (body === undefined) throw new Error('missing test publish')
    replay.pushPublish(replay.nextSeq(), index, body)
  }
  try {
    push(7, bodies[0])
    vi.advanceTimersByTime(50)
    replay.acknowledge(1)
    expect(replay.leastConfirmMs).toBe(50)

    const textSeq = replay.nextSeq()
    replay.push(textSeq, encode.text(7, '"plain"', textSeq))
    push(300, bodies[1])
    vi.advanceTimersByTime(1)
    push(301, bodies[2])
    vi.advanceTimersByTime(1)
    push(302, bodies[3])
    vi.advanceTimersByTime(1)
    push(303, bodies[4])
    vi.advanceTimersByTime(53)

    replay.acknowledge(4, 3)
    expect(replay.leastConfirmMs).toBe(50)
    expect(replay.confirmRate).toBeGreaterThan(0)
    replay.dropBeforeNewestPublish(2)
    const [route, body] = replay.getAfter(4)
    if (route === undefined) throw new Error('missing route')
    expect(decode(route)).toMatchObject({ tag: TAG.PUBLISH_TO, index: 303, seq: 6 })
    expect(body).toBe(bodies[4]?.frame)
    expect(bodies.map((publish) => publish.refs)).toEqual([0, 0, 0, 0, 1])
    expect(replay.unconfirmedSince).toBe(53)

    replay.acknowledge(6)
    expect(replay.isEmpty).toBe(true)
    expect(holder.heldBytes).toBe(0)
  } finally {
    replay.dispose()
    vi.useRealTimers()
  }
})
