import { afterEach, describe, expect, it, vi } from 'vitest'
import type { BackendReceiver, SubscriptionState } from './spi.js'
import {
  SUBSCRIPTION_ESTABLISH_TIMEOUT_MS,
  SubscriptionManager,
  type SubscriptionAttempt,
  type SubscriptionDriver,
} from './subscriptions.js'
import {
  SUBSCRIPTION_RETRY_ATTEMPTS,
  SUBSCRIPTION_RETRY_BASE_MS,
  SUBSCRIPTION_RETRY_MAX_MS,
} from '../server/adapter/cloudflare/room/subscription.js'

afterEach(() => vi.useRealTimers())

describe('shared subscription lifecycle and fan-out', () => {
  it('opens one upstream source, fans out locally, and releases it after the last receiver', async () => {
    const driver = new ControlledDriver()
    const manager = new SubscriptionManager(driver)
    const received: string[] = []
    const first = manager.subscribe('same', 'source', (payload) => received.push(`a:${decode(payload)}`))
    const second = manager.subscribe('same', 'source', (payload) => received.push(`b:${decode(payload)}`))

    expect(driver.opens).toHaveLength(1)
    await driver.deliver(0, 'one')
    expect(received).toEqual(['a:one', 'b:one'])
    expect(driver.opens[0]!.localReceiverCount()).toBe(2)

    await first.unsubscribe()
    expect(driver.opens[0]!.attempt.unsubscribeCalls).toBe(0)
    await second.unsubscribe()
    expect(driver.opens[0]!.attempt.unsubscribeCalls).toBe(1)
  })

  it('treats repeated attachment of the same receiver function as distinct demand', async () => {
    const driver = new ControlledDriver()
    const manager = new SubscriptionManager(driver)
    const received: string[] = []
    const receiver: BackendReceiver = (payload) => received.push(decode(payload))
    const first = manager.subscribe('duplicate', 'source', receiver)
    const second = manager.subscribe('duplicate', 'source', receiver)

    await driver.deliver(0, 'both')
    expect(received).toEqual(['both', 'both'])
    await first.unsubscribe()
    await driver.deliver(0, 'second')
    expect(received).toEqual(['both', 'both', 'second'])
    await second.unsubscribe()
  })

  it('closes an individual attachment even while the shared source remains live', async () => {
    const driver = new ControlledDriver()
    const manager = new SubscriptionManager(driver)
    const first = manager.subscribe('individual-close', 'source', () => {})
    const second = manager.subscribe('individual-close', 'source', () => {})
    const states: SubscriptionState[] = []
    first.onStateChange((state) => states.push(state))

    await first.unsubscribe()

    expect(first.state()).toBe('closed')
    expect(second.state()).toBe('ready')
    expect(states).toEqual(['closed'])
    expect(driver.opens[0]!.attempt.unsubscribeCalls).toBe(0)
  })

  it('terminally closes a deliberately destroyed source without replanning it', async () => {
    const driver = new ControlledDriver()
    const manager = new SubscriptionManager(driver)
    const first = manager.subscribe('destroyed', 'source', () => {})
    await first.ready

    manager.terminate((source) => source === 'source')
    await settle()

    expect(first.state()).toBe('closed')
    expect(driver.openCalls).toBe(1)
    const recreated = manager.subscribe('destroyed', 'source', () => {})
    await recreated.ready
    expect(driver.openCalls).toBe(2)
  })

  it('rejects stale delivery from a retired attempt epoch', async () => {
    const driver = new ControlledDriver()
    const manager = new SubscriptionManager(driver)
    const received: string[] = []
    const subscription = manager.subscribe('epoch', 'source', (payload) => received.push(decode(payload)))
    await subscription.ready

    driver.opens[0]!.attempt.close()
    await settle()
    expect(driver.opens).toHaveLength(2)
    await driver.deliver(0, 'stale')
    await driver.deliver(1, 'current')

    expect(received).toEqual(['current'])
  })

  it('replans after initial rejection instead of staying permanently deaf', async () => {
    const driver = new ControlledDriver()
    driver.plan(() => {
      const attempt = new ControlledAttempt()
      queueMicrotask(() => attempt.reject(new Error('initial refusal')))
      return attempt
    })
    const report = vi.fn()
    const manager = new SubscriptionManager(driver, report)
    const subscription = manager.subscribe('initial', 'source', () => {})

    await expect(subscription.ready).resolves.toBeUndefined()
    expect(driver.opens).toHaveLength(2)
    expect(String(report.mock.calls[0]?.[0])).toContain('initial refusal')
  })

  it('renews the readiness generation across lost → ready without replacing the attempt', async () => {
    const driver = new ControlledDriver()
    const manager = new SubscriptionManager(driver, () => {})
    const subscription = manager.subscribe('renewal', 'source', () => {})
    const firstReady = subscription.ready
    await firstReady

    driver.opens[0]!.attempt.lose()
    const renewedReady = subscription.ready
    expect(renewedReady).not.toBe(firstReady)
    expect(subscription.state()).toBe('lost')
    driver.opens[0]!.attempt.establish()

    await expect(renewedReady).resolves.toBeUndefined()
    expect(driver.opens).toHaveLength(1)
    expect(subscription.state()).toBe('ready')
  })

  it('reports a synchronous replacement throw and continues to the next replacement', async () => {
    const driver = new ControlledDriver()
    driver.planReady()
    driver.plan(() => {
      throw new Error('replacement subscribe threw')
    })
    const report = vi.fn()
    const manager = new SubscriptionManager(driver, report)
    const subscription = manager.subscribe('throw', 'source', () => {})
    await subscription.ready

    driver.opens[0]!.attempt.close()
    await settle()

    expect(driver.openCalls).toBe(3)
    await expect(subscription.ready).resolves.toBeUndefined()
    expect(report.mock.calls.some(([error]) => String(error).includes('replacement subscribe threw'))).toBe(true)
  })

  it('rejects after exactly five failed replacements and creates a fresh generation on later demand', async () => {
    const driver = new ControlledDriver()
    driver.planReady()
    for (let attempt = 1; attempt <= 5; attempt++) {
      driver.plan(() => {
        throw new Error(`replacement ${attempt}`)
      })
    }
    const report = vi.fn()
    const manager = new SubscriptionManager(driver, report)
    const subscription = manager.subscribe('exhaustion', 'source', () => {})
    await subscription.ready

    driver.opens[0]!.attempt.close()
    await settle(12)
    const terminalReady = subscription.ready

    expect(driver.openCalls).toBe(6)
    expect(report).toHaveBeenCalledTimes(5)
    await expect(terminalReady).rejects.toThrow('failed after 5 replacement attempts')

    const later = manager.subscribe('exhaustion', 'source', () => {})
    await expect(later.ready).resolves.toBeUndefined()
    expect(driver.openCalls).toBe(7)
    expect(subscription.ready).not.toBe(terminalReady)
  })

  it('starts fresh demand while retired remote cleanup remains pending', async () => {
    const cleanup = deferred<void>()
    const driver = new ControlledDriver()
    driver.plan(() => ControlledAttempt.ready({ cleanup: cleanup.promise }))
    const manager = new SubscriptionManager(driver)
    const first = manager.subscribe('cleanup', 'source', () => {})
    await first.ready

    const stopping = first.unsubscribe()
    const second = manager.subscribe('cleanup', 'source', () => {})

    await expect(second.ready).resolves.toBeUndefined()
    expect(driver.openCalls).toBe(2)
    cleanup.resolve()
    await stopping
  })

  it('retires a hung initial establishment and replans before resolving readiness', async () => {
    vi.useFakeTimers()
    const driver = new ControlledDriver()
    driver.plan(() => new ControlledAttempt())
    const report = vi.fn()
    const manager = new SubscriptionManager(driver, report)
    const subscription = manager.subscribe('hung-initial', 'source', () => {})

    await vi.advanceTimersByTimeAsync(SUBSCRIPTION_ESTABLISH_TIMEOUT_MS - 1)
    expect(driver.openCalls).toBe(1)
    await vi.advanceTimersByTimeAsync(1)

    expect(driver.openCalls).toBe(2)
    expect(driver.opens[0]!.attempt.unsubscribeCalls).toBe(1)
    expect(String(report.mock.calls[0]?.[0])).toContain('establishment did not settle within the deadline')
    await expect(subscription.ready).resolves.toBeUndefined()
  })

  it('replaces an established attempt whose renewal remains lost past the deadline', async () => {
    vi.useFakeTimers()
    const driver = new ControlledDriver()
    const report = vi.fn()
    const manager = new SubscriptionManager(driver, report)
    const subscription = manager.subscribe('hung-renewal', 'source', () => {})
    await subscription.ready

    driver.opens[0]!.attempt.lose()
    const renewal = subscription.ready
    await vi.advanceTimersByTimeAsync(SUBSCRIPTION_ESTABLISH_TIMEOUT_MS)

    expect(driver.openCalls).toBe(2)
    expect(report).toHaveBeenCalledTimes(2)
    expect(String(report.mock.calls[0]?.[0])).toContain('subscription lost')
    expect(String(report.mock.calls[1]?.[0])).toContain('establishment did not settle within the deadline')
    await expect(renewal).resolves.toBeUndefined()
  })

  it('terminally rejects after an initial hung attempt and five hung replacements', async () => {
    vi.useFakeTimers()
    const driver = new ControlledDriver()
    for (let attempt = 0; attempt <= 5; attempt++) driver.plan(() => new ControlledAttempt())
    const report = vi.fn()
    const manager = new SubscriptionManager(driver, report)
    const subscription = manager.subscribe('hung-exhaustion', 'source', () => {})
    const readiness = subscription.ready

    for (let attempt = 0; attempt <= 5; attempt++) {
      await vi.advanceTimersByTimeAsync(SUBSCRIPTION_ESTABLISH_TIMEOUT_MS)
    }

    expect(driver.openCalls).toBe(6)
    expect(driver.opens.map(({ attempt }) => attempt.unsubscribeCalls)).toEqual([1, 1, 1, 1, 1, 1])
    expect(report).toHaveBeenCalledTimes(6)
    await expect(readiness).rejects.toThrow('failed after 5 replacement attempts')
  })

  it('never fires the watchdog for a synchronous in-memory establishment', async () => {
    vi.useFakeTimers()
    const driver = new ControlledDriver()
    const manager = new SubscriptionManager(driver)
    const subscription = manager.subscribe('sync', 'source', () => {})

    await expect(subscription.ready).resolves.toBeUndefined()
    await vi.advanceTimersByTimeAsync(6 * SUBSCRIPTION_ESTABLISH_TIMEOUT_MS + 1)
    expect(driver.openCalls).toBe(1)
    expect(subscription.state()).toBe('ready')
  })

  it('keeps the core deadline above the complete Cloudflare retry envelope plus one operation margin', () => {
    let retryDelay = 0
    for (let retry = 0; retry < SUBSCRIPTION_RETRY_ATTEMPTS - 1; retry++) {
      retryDelay += Math.min(SUBSCRIPTION_RETRY_BASE_MS * 2 ** retry, SUBSCRIPTION_RETRY_MAX_MS)
    }
    const jitteredEnvelope = retryDelay * 1.5
    expect(SUBSCRIPTION_ESTABLISH_TIMEOUT_MS).toBeGreaterThanOrEqual(jitteredEnvelope + SUBSCRIPTION_RETRY_MAX_MS)
  })
})

type OpenRecord = {
  source: string
  receiver: BackendReceiver
  localReceiverCount: () => number
  attempt: ControlledAttempt
}

class ControlledDriver implements SubscriptionDriver<string> {
  readonly opens: OpenRecord[] = []
  readonly #plans: Array<() => ControlledAttempt> = []
  openCalls = 0

  plan(plan: () => ControlledAttempt): void {
    this.#plans.push(plan)
  }

  planReady(): void {
    this.plan(() => ControlledAttempt.ready())
  }

  open(source: string, receiver: BackendReceiver, localReceiverCount: () => number): SubscriptionAttempt {
    this.openCalls++
    const attempt = (this.#plans.shift() ?? (() => ControlledAttempt.ready()))()
    this.opens.push({ source, receiver, localReceiverCount, attempt })
    return attempt
  }

  label(source: string): string {
    return source
  }

  async deliver(openIndex: number, payload: string): Promise<void> {
    const receiver = this.opens[openIndex]?.receiver
    if (receiver === undefined) throw new Error(`No open record ${openIndex}`)
    await (receiver(new TextEncoder().encode(payload), { seq: openIndex + 1, timestamp: 1 }) as unknown)
  }
}

class ControlledAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  readonly #readiness = deferred<void>()
  readonly #listeners = new Set<(state: SubscriptionState) => void>()
  readonly #cleanup: Promise<void>
  #state: SubscriptionState = 'establishing'
  unsubscribeCalls = 0

  constructor(options?: { cleanup?: Promise<void> }) {
    this.ready = this.#readiness.promise
    this.#cleanup = options?.cleanup ?? Promise.resolve()
    void this.ready.catch(() => {})
  }

  static ready(options?: { cleanup?: Promise<void> }): ControlledAttempt {
    const attempt = new ControlledAttempt(options)
    attempt.establish()
    return attempt
  }

  state(): SubscriptionState {
    return this.#state
  }

  onStateChange(listener: (state: SubscriptionState) => void): () => void {
    this.#listeners.add(listener)
    return () => this.#listeners.delete(listener)
  }

  async unsubscribe(): Promise<void> {
    this.unsubscribeCalls++
    this.#transition('closed')
    await this.#cleanup
  }

  establish(): void {
    this.#transition('ready')
    this.#readiness.resolve()
  }

  lose(): void {
    this.#transition('lost')
  }

  close(): void {
    this.#transition('closed')
  }

  reject(error: Error): void {
    this.#transition('closed')
    this.#readiness.reject(error)
  }

  #transition(state: SubscriptionState): void {
    if (this.#state === state) return
    this.#state = state
    for (const listener of [...this.#listeners]) listener(state)
  }
}

function deferred<T>(): { promise: Promise<T>; resolve(value?: T): void; reject(error: unknown): void } {
  let resolve!: (value?: T) => void
  let reject!: (error: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise as (value?: T) => void
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

function decode(payload: Uint8Array): string {
  return new TextDecoder().decode(payload)
}

async function settle(turns = 4): Promise<void> {
  for (let turn = 0; turn < turns; turn++) await Promise.resolve()
}
