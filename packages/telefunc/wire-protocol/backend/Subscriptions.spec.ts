import { describe, expect, it } from 'vitest'
import { Subscriptions } from './Subscriptions.js'
import type { BackendPayload, Subscriber, Unsubscribe } from './driver.js'
import type { OrderingInfo } from '../wire/orderingFrame.js'

/** A driver whose one subscription the test drives by hand. */
function createDriver() {
  const driver = {
    subscriber: null as Subscriber | null,
    subscribe(_source: string, subscriber: Subscriber): Unsubscribe {
      driver.subscriber = subscriber
      return async () => {}
    },
  }
  return driver
}

const at = (seq: number): OrderingInfo => ({ seq, timestamp: seq })

function subscribe(subscriptions: Subscriptions<string>, events: string[], name: string) {
  const subscription = subscriptions.subscribe('source', { key: 'key', delivers: 'text' }, (payload: BackendPayload) =>
    events.push(`${name} message ${payload as string}`),
  )
  subscription.onStateChange((state) => events.push(`${name} ${state}`))
  return subscription
}

describe('a message the driver delivers before its first ready()', () => {
  it('reaches the consumers right after ready, in the order delivered', () => {
    const driver = createDriver()
    const events: string[] = []
    subscribe(new Subscriptions({ driver }), events, 'a')
    driver.subscriber!.message('1', at(1))
    driver.subscriber!.message('2', at(2))
    expect(events).toEqual([])
    driver.subscriber!.ready()
    driver.subscriber!.message('3', at(3))
    expect(events).toEqual(['a ready', 'a message 1', 'a message 2', 'a message 3'])
  })

  it('reaches only the consumers attached when it was delivered, and not one that left since', () => {
    const driver = createDriver()
    const events: string[] = []
    const subscriptions = new Subscriptions({ driver })
    subscribe(subscriptions, events, 'a')
    const b = subscribe(subscriptions, events, 'b')
    driver.subscriber!.message('1', at(1))
    subscribe(subscriptions, events, 'c')
    void b.unsubscribe()
    driver.subscriber!.ready()
    expect(events).toEqual(['b closed', 'a ready', 'c ready', 'a message 1'])
  })

  it('after a loss, a message is delivered as it arrives', () => {
    const driver = createDriver()
    const events: string[] = []
    subscribe(new Subscriptions({ driver }), events, 'a')
    driver.subscriber!.ready()
    driver.subscriber!.lost()
    driver.subscriber!.message('1', at(1))
    driver.subscriber!.ready()
    expect(events).toEqual(['a ready', 'a lost', 'a message 1', 'a ready'])
  })

  it('is dropped when the subscription ends instead', async () => {
    const driver = createDriver()
    const events: string[] = []
    const subscription = subscribe(new Subscriptions({ driver }), events, 'a')
    driver.subscriber!.message('1', at(1))
    driver.subscriber!.ended()
    driver.subscriber!.ready()
    await expect(subscription.ready).rejects.toThrow('Backend subscription closed: key')
    expect(events).toEqual(['a closed'])
  })
})
