import { describe, expect, it } from 'vitest'
import { BackendError, createDeferred, type BackendListener, type BackendSubscription } from 'telefunc/__internal'
import { SEMANTIC_LANE } from './lanes.js'
import { LaneSubscription, type LaneAddress, type LaneSubscriptionOwner } from './LaneSubscription.js'
import type { RoomSubscriptionSource } from '../../backend/contract.js'
import { assert } from '../../utils/assert.js'

type AttemptState = ReturnType<BackendSubscription['state']>

/** A store's refusal, as its driver reports it. */
function refusal(): BackendError {
  return new BackendError({ code: 'denied', outcome: 'may-have-run', message: 'refused the channel (NOPERM)' })
}

/** A consumer's subscription as core hands it out: its driver moves it, and `ready` rejects once it is closed. */
function fakeSubscription() {
  let state: AttemptState = 'establishing'
  let readiness = createDeferred()
  void readiness.promise.catch(() => {})
  const listeners = new Set<(state: AttemptState) => void>()
  const enter = (next: AttemptState) => {
    state = next
    for (const listener of listeners) listener(next)
  }
  const subscription: BackendSubscription = {
    get ready() {
      return readiness.promise
    },
    state: () => state,
    onStateChange(listener) {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    async unsubscribe() {},
  }
  return {
    subscription,
    ready() {
      readiness.resolve()
      enter('ready')
    },
    lose() {
      if (state === 'ready') {
        readiness = createDeferred()
        void readiness.promise.catch(() => {})
      }
      enter('lost')
    },
    /** As core closes it: with the store's `BackendError` itself, with another reason of its driver's as the cause, or
     *  with none where the backend ended it on purpose. */
    close(reason?: Error) {
      if (state === 'ready') {
        readiness = createDeferred()
        void readiness.promise.catch(() => {})
      }
      enter('closed')
      readiness.reject(
        reason instanceof BackendError
          ? reason
          : new Error('Backend subscription closed: lane', reason === undefined ? {} : { cause: reason }),
      )
    },
  }
}

function setup() {
  const subscriptions: ReturnType<typeof fakeSubscription>[] = []
  const calls = { ended: 0, recovered: 0 }
  /** What each subscription said registers it. */
  const registeredBy: Array<RoomSubscriptionSource['registeredBy']> = []
  const owner: LaneSubscriptionOwner = {
    roomId: 'room',
    subscribeLane(_address: LaneAddress, _listener: BackendListener<Uint8Array>, by?: 'operation') {
      registeredBy.push(by)
      const fake = fakeSubscription()
      subscriptions.push(fake)
      return fake.subscription
    },
    onLaneDelivery() {},
    onLaneEnded: () => void calls.ended++,
    onLaneRecovered: () => void calls.recovered++,
    onLanePending() {},
  }
  const lane = new LaneSubscription({ owner, address: SEMANTIC_LANE })
  /** The lane's first subscription, which its `start()` made. */
  const first = () => {
    const [subscription] = subscriptions
    assert(subscription !== undefined)
    return subscription
  }
  return { lane, subscriptions, first, calls, registeredBy }
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

describe('a Room lane', () => {
  it("started for the operation that registers it says so on that start's subscription only: one made again after a stop registers itself", () => {
    const { lane, registeredBy } = setup()
    lane.start('operation')
    lane.start('operation')
    lane.stop()
    lane.start()
    expect(registeredBy).toEqual(['operation', undefined])
  })

  it('that Redis refuses fails its consumer at once with the refusal itself, and nothing subscribes it again', async () => {
    const { lane, first, subscriptions, calls } = setup()
    lane.start()
    const refused = refusal()
    first().close(refused)
    const failure = await Promise.race([
      lane.ready.then(
        () => 'ready',
        (error: unknown) => error,
      ),
      tick().then(() => 'pending'),
    ])
    expect(failure).toBe(refused)
    // A planning pass starts it again, as it does every wanted lane.
    lane.start()
    await tick()
    expect(subscriptions).toHaveLength(1)
    expect(calls).toEqual({ ended: 1, recovered: 0 })
  })

  it('that its backend ends with a reason that is no store failure fails its consumer with that reason as the cause', async () => {
    const { lane, subscriptions } = setup()
    lane.start()
    const reason = new Error('the platform refused it')
    subscriptions[0]?.close(reason)
    await expect(lane.ready).rejects.toMatchObject({ cause: reason })
  })

  it('whose driver lost it is ready again on the same subscription, and its owner catches up', async () => {
    const { lane, first, subscriptions, calls } = setup()
    lane.start()
    first().ready()
    await lane.ready
    first().lose()
    first().ready()
    await lane.ready
    expect(subscriptions).toHaveLength(1)
    expect(calls).toEqual({ ended: 0, recovered: 1 })
  })

  it('whose incarnation the backend dropped before it was ready fails its consumer at once with the closed room', async () => {
    const { lane, subscriptions, calls } = setup()
    lane.start()
    subscriptions[0]?.close()
    const failure = await Promise.race([
      lane.ready.then(
        () => 'ready',
        (error: unknown) => (error instanceof Error ? error.message : error),
      ),
      tick().then(() => 'pending'),
    ])
    expect(failure).toBe('Room is closed: room')
    expect(calls.ended).toBe(1)
  })

  it('whose incarnation the backend dropped tells its owner, which finds the close, and subscribes nothing again', async () => {
    const { lane, first, subscriptions, calls } = setup()
    lane.start()
    first().ready()
    first().close()
    await tick()
    lane.start()
    expect(subscriptions).toHaveLength(1)
    expect(calls.ended).toBe(1)
  })
})
