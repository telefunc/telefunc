import { expect, test, vi } from 'vitest'
import { ROOM_HORIZON_MS } from '../../../../room/constants.js'
import { Fanout, type RoomSessionNamespace } from './fanout.js'
import type { RoomSessionDeliveryRequest } from './subscription.js'

const route = { roomId: 'room', inc: 'inc', laneKey: 'semantic', sessionDoId: 'session', leaseId: 'lease' }

/** Session DOs whose stubs hand each call to `deliver`, with the stub's index. */
function sessions(deliver: (request: RoomSessionDeliveryRequest, stub: number) => Promise<void>): RoomSessionNamespace {
  let opened = 0
  return {
    idFromString: (id) => id,
    get() {
      const stub = opened++
      return { telefuncRoomDeliver: (request) => deliver(request, stub) }
    },
  }
}

function tokenOfSent(fanout: Fanout, seq: number): string {
  return fanout.tokenOf(fanout.send([route], new Uint8Array([seq]), { seq, timestamp: 1 }))
}

test('does not alias an old delivery token to a reconstructed authority attempt', async () => {
  const delivered = sessions(async () => {})
  const oldToken = tokenOfSent(new Fanout(delivered), 1)
  const reconstructedAuthority = new Fanout(delivered)
  const newToken = tokenOfSent(reconstructedAuthority, 2)

  await expect(reconstructedAuthority.awaitDelivery(oldToken)).rejects.toThrow('unknown delivery token')
  await expect(reconstructedAuthority.awaitDelivery(newToken)).resolves.toBeUndefined()
})

test("a failed handoff is loss: its delivery settles, the loss is logged, and the session's next frame still goes out", async () => {
  const report = vi.spyOn(console, 'error').mockImplementation(() => {})
  try {
    const handedTo: number[] = []
    const fanout = new Fanout(
      sessions(async ({ info }, stub) => {
        if (info.seq === 1) throw new Error('session reset')
        handedTo.push(stub)
      }),
    )
    await expect(fanout.awaitDelivery(tokenOfSent(fanout, 1))).resolves.toBeUndefined()
    expect(report).toHaveBeenCalledWith('Cloudflare Room delivery lost to 1/1 Durable Objects: Error: session reset')
    await expect(fanout.awaitDelivery(tokenOfSent(fanout, 2))).resolves.toBeUndefined()
    // A stub that rejected may be broken, so the frame after the loss went through a fresh one.
    expect(handedTo).toEqual([1])
  } finally {
    report.mockRestore()
  }
})

test('forgets a delivery token nobody awaits within two horizons', async () => {
  vi.useFakeTimers({ toFake: ['Date'] })
  try {
    const fanout = new Fanout(sessions(async () => {}))
    const unawaited = tokenOfSent(fanout, 1)
    vi.advanceTimersByTime(ROOM_HORIZON_MS)
    const late = tokenOfSent(fanout, 2)
    vi.advanceTimersByTime(ROOM_HORIZON_MS)
    await expect(fanout.awaitDelivery(late)).resolves.toBeUndefined()
    await expect(fanout.awaitDelivery(unawaited)).rejects.toThrow('unknown delivery token')
  } finally {
    vi.useRealTimers()
  }
})
