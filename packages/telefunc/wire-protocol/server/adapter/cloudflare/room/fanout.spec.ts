import { expect, test, vi } from 'vitest'
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

test('does not alias an old delivery token to a reconstructed authority attempt', async () => {
  const delivered = sessions(async () => {})
  const oldToken = new Fanout(delivered).send([route], new Uint8Array([1]), 1, 1)
  const reconstructedAuthority = new Fanout(delivered)
  const newToken = reconstructedAuthority.send([route], new Uint8Array([2]), 2, 1)

  await expect(reconstructedAuthority.await(oldToken)).rejects.toThrow('unknown delivery token')
  await expect(reconstructedAuthority.await(newToken)).resolves.toBeUndefined()
})

test("a failed handoff is loss: its delivery settles, the loss is logged, and the session's next frame still goes out", async () => {
  const report = vi.spyOn(console, 'error').mockImplementation(() => {})
  try {
    const handedTo: number[] = []
    const fanout = new Fanout(
      sessions(async ({ seq }, stub) => {
        if (seq === 1) throw new Error('session reset')
        handedTo.push(stub)
      }),
    )
    await expect(fanout.await(fanout.send([route], new Uint8Array([1]), 1, 1))).resolves.toBeUndefined()
    expect(report).toHaveBeenCalledWith('Cloudflare Room delivery lost to 1/1 Durable Objects: Error: session reset')
    await expect(fanout.await(fanout.send([route], new Uint8Array([2]), 2, 1))).resolves.toBeUndefined()
    // A stub that rejected may be broken, so the frame after the loss went through a fresh one.
    expect(handedTo).toEqual([1])
  } finally {
    report.mockRestore()
  }
})
