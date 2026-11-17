import { describe, expect, it } from 'vitest'
import { createDeferred, type BackendSubscription } from 'telefunc/__internal'
import { SEMANTIC_LANE } from './lanes.js'
import { LaneSubscription, type LaneSubscriptionOwner } from './LaneSubscription.js'

type AttemptState = ReturnType<BackendSubscription['state']>

function refusal(): Error {
  const error = new Error('NOPERM this user has no permissions to access the channel')
  error.name = 'ReplyError'
  return error
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
    /** As core closes it: with the driver's reason as the cause, or none where the backend ended it on purpose. */
    close(reason?: Error) {
      if (state === 'ready') {
        readiness = createDeferred()
        void readiness.promise.catch(() => {})
      }
      enter('closed')
      readiness.reject(new Error('Backend subscription closed: lane', reason === undefined ? {} : { cause: reason }))
    },
  }
}

function setup() {
  const subscriptions: ReturnType<typeof fakeSubscription>[] = []
  const calls = { ended: 0, recovered: 0, terminal: 0 }
  const owner = {
    subscribeLane() {
      const fake = fakeSubscription()
      subscriptions.push(fake)
      return fake.subscription
    },
    onLaneDelivery() {},
    onLaneEnded: () => void calls.ended++,
    onLaneRecovered: () => void calls.recovered++,
    onLaneTerminal: () => void calls.terminal++,
    onLanePending() {},
  } as LaneSubscriptionOwner
  const lane = new LaneSubscription({ owner, address: SEMANTIC_LANE })
  return { lane, subscriptions, calls }
}

const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

describe('a Room lane', () => {
  it('that Redis refuses fails its consumer at once with the refusal, and nothing subscribes it again', async () => {
    const { lane, subscriptions, calls } = setup()
    lane.start()
    const refused = refusal()
    subscriptions[0]!.close(refused)
    const failure = await Promise.race([
      lane.ready.then(
        () => 'ready',
        (error: Error) => error,
      ),
      tick().then(() => 'pending'),
    ])
    expect(failure).toBeInstanceOf(Error)
    expect((failure as Error).cause).toBe(refused)
    // A planning pass starts it again, as it does every wanted lane.
    lane.start()
    await tick()
    expect(subscriptions).toHaveLength(1)
    expect(calls).toEqual({ ended: 1, recovered: 0, terminal: 0 })
  })

  it('whose driver lost it is ready again on the same subscription, and its owner catches up', async () => {
    const { lane, subscriptions, calls } = setup()
    lane.start()
    subscriptions[0]!.ready()
    await lane.ready
    subscriptions[0]!.lose()
    subscriptions[0]!.ready()
    await lane.ready
    expect(subscriptions).toHaveLength(1)
    expect(calls).toEqual({ ended: 0, recovered: 1, terminal: 0 })
  })

  it('whose incarnation the backend dropped tells its owner, which finds the close, and subscribes nothing again', async () => {
    const { lane, subscriptions, calls } = setup()
    lane.start()
    subscriptions[0]!.ready()
    subscriptions[0]!.close()
    await tick()
    lane.start()
    expect(subscriptions).toHaveLength(1)
    expect(calls.ended).toBe(1)
  })
})
