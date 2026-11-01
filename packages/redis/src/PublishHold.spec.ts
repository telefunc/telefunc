import { describe, expect, it } from 'vitest'
import { PublishHold } from './PublishHold.js'
import { RedisBackend } from './RedisBackend.js'
import type { RedisConnection } from './RedisConnection.js'
import type { RedisSubscriber } from './subscriber.js'

const PREFIX = 'tf:0:'

/** A backend whose commands record the order they are sent in, and whose subscriber reports what `subscribe()`
 *  starts on key `held`. */
function setup() {
  const subscriber = {
    subscribing: false,
    establishing: null as Promise<void> | null,
    firstReady: (channels: readonly string[]) =>
      channels.includes(`${PREFIX}t:{held}`) ? subscriber.establishing : null,
  }
  const sent: string[] = []
  const connection = {
    prefix: PREFIX,
    reportsReceivers: true,
    hold: new PublishHold({ subscriber: subscriber as unknown as RedisSubscriber, prefix: PREFIX }),
    run: (_command: unknown, { payload }: { payload: string | Uint8Array }) => {
      sent.push(typeof payload === 'string' ? payload : new TextDecoder().decode(payload))
      return Promise.resolve({ seq: sent.length, timestamp: 0, receivers: 0 })
    },
  }
  const backend = new RedisBackend({ connection: connection as unknown as RedisConnection })
  /** Starts a subscription on key `held`; the returned function makes it ready. */
  const subscribe = () => {
    let ready!: () => void
    subscriber.establishing = new Promise<void>((resolve) => (ready = resolve))
    subscriber.subscribing = true
    return () => {
      subscriber.establishing = null
      subscriber.subscribing = false
      ready()
    }
  }
  return { backend, sent, subscribe }
}

describe('RedisBackend.publish() with a held publish', () => {
  it('sends a publish called on any microtask after the wait ends after the held one', async () => {
    const { backend, sent, subscribe } = setup()
    const ready = subscribe()
    const publishes = [backend.publish({ key: 'held', kind: 'text' }, 'held')]
    ready()
    let tick = Promise.resolve()
    for (let i = 0; i < 12; i++) {
      tick = tick.then(() => {
        publishes.push(
          i % 2 === 0
            ? backend.publish({ key: 'held', kind: 'text' }, `${i}`)
            : backend.publish({ key: 'held', kind: 'binary' }, new TextEncoder().encode(`${i}`)),
        )
      })
    }
    await tick
    await new Promise((resolve) => setImmediate(resolve))
    publishes.push(backend.publish({ key: 'held', kind: 'text' }, 'after'))
    await Promise.all(publishes)
    expect(sent).toEqual(['held', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', 'after'])
  })

  it('holds a publish for a subscription that starts right after the wait ends', async () => {
    const { backend, sent, subscribe } = setup()
    const readyFirst = subscribe()
    const first = backend.publish({ key: 'held', kind: 'text' }, 'first')
    readyFirst()
    await first
    const readySecond = subscribe()
    const second = backend.publish({ key: 'held', kind: 'text' }, 'second')
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(sent).toEqual(['first'])
    readySecond()
    await second
    expect(sent).toEqual(['first', 'second'])
  })

  it('sends a publish on another key at once while one is held', () => {
    const { backend, sent, subscribe } = setup()
    subscribe()
    void backend.publish({ key: 'held', kind: 'text' }, 'held')
    void backend.publish({ key: 'other', kind: 'text' }, 'other')
    expect(sent).toEqual(['other'])
  })
})
