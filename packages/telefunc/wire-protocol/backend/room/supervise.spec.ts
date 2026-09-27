import { describe, expect, it, vi } from 'vitest'
import { superviseRoomDriver } from './supervise.js'
import type { RoomDriver } from './contract.js'
import { MemoryBackend } from '../memory/backend.js'
import { DriverAttempt } from '../attempt.js'
const encoder = new TextEncoder()

describe('Room driver supervision', () => {
  it("sends a close's leased commit at once while its lane's subscription establishes, as the lease is shorter than the hold", async () => {
    const attempt = new ManualAttempt()
    const committed: string[] = []
    const driver = {
      subscriptions: { bind: () => ({ partition: '', open: () => attempt }), partitionHere: () => '' },
      commitLane: async (_roomId: string, _inc: string, _lane: unknown, payload: Uint8Array) => {
        committed.push(new TextDecoder().decode(payload))
        return { accepted: true, seq: committed.length, timestamp: 1, delivery: Promise.resolve() }
      },
    } as unknown as RoomDriver
    const backend = superviseRoomDriver(driver)
    const control = { kind: 'control' } as const
    const subscription = backend.subscribeLane('room', 'inc', control, () => {})
    const held = backend.commitLane('room', 'inc', control, new TextEncoder().encode('join'))
    const closing = backend.commitLane('room', 'inc', control, new TextEncoder().encode('closed'), {
      closingLease: 'lease',
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(committed).toEqual(['closed'])
    attempt.ready()
    await Promise.all([held, closing])
    expect(committed).toEqual(['closed', 'join'])
    await subscription.unsubscribe()
  })
  it('sends commits held for an establishing lane before any commit that arrives once it is ready', async () => {
    // Whatever the microtask distance of the later commit from the lane's readiness.
    for (let distance = 0; distance < 12; distance++) {
      const order: string[] = []
      let attempt!: ManualAttempt
      const driver = {
        subscriptions: {
          bind: () => ({ partition: '', open: () => (attempt = new ManualAttempt()) }),
          partitionHere: () => '',
        },
        commitLane: async (_roomId: string, _inc: string, _lane: unknown, payload: Uint8Array) => {
          order.push(new TextDecoder().decode(payload))
          return { accepted: true, seq: order.length, timestamp: 1, delivery: Promise.resolve() }
        },
      } as unknown as RoomDriver
      const backend = superviseRoomDriver(driver)
      const semantic = { kind: 'semantic' } as const
      const subscription = backend.subscribeLane('room', 'inc', semantic, () => {})
      const held = ['a', 'b'].map((text) => backend.commitLane('room', 'inc', semantic, new TextEncoder().encode(text)))
      attempt.ready()
      let later: Promise<unknown> = Promise.resolve()
      for (let hop = 0; hop < distance; hop++) later = later.then(() => {})
      const overtaking = later.then(() => backend.commitLane('room', 'inc', semantic, new TextEncoder().encode('c')))
      await Promise.all([...held, overtaking])
      expect(order).toEqual(['a', 'b', 'c'])
      await subscription.unsubscribe()
      await backend.dispose()
    }
  })
  it('validates HeadNext shape before delegating to any raw driver', async () => {
    const driver = new MemoryBackend()
    const backend = superviseRoomDriver(driver)
    const opened = await backend.compareExchangeHead(
      'head-shape',
      { form: 'absent' },
      { head: { state: 'open', currentInc: 'inc-1', config: encoder.encode('config') } },
    )
    if (!('head' in opened)) throw new Error('head create failed')
    const delegated = vi.spyOn(driver, 'compareExchangeHead')
    for (const durationMs of [0, Number.POSITIVE_INFINITY]) {
      await expect(
        backend.compareExchangeHead(
          'head-shape',
          { form: 'rev', rev: opened.head.rev },
          {
            head: {
              state: 'closing',
              currentInc: 'inc-1',
              config: opened.head.config,
              closeLease: { id: 'lease-1', durationMs },
            },
          },
        ),
      ).rejects.toThrow(`close lease durationMs ${durationMs} must be finite and positive`)
    }
    expect(delegated).not.toHaveBeenCalled()
  })
})

class ManualAttempt extends DriverAttempt {
  async unsubscribe() {
    this.transition('closed')
  }
  ready() {
    this.transition('ready')
  }
}
