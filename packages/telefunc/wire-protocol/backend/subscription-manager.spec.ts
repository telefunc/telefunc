import { describe, expect, it, vi } from 'vitest'
import { SubscriptionManager } from './subscription-manager.js'
import type {
  BackendReceiver,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionDriver,
  SubscriptionState,
} from './subscription.js'

const encoder = new TextEncoder()
const decoder = new TextDecoder()
const deferred = <T>() => Promise.withResolvers<T>()

describe('shared subscription supervision', () => {
  it('owns fan-out, refcount, epochs, and raw terminal signalling once', async () => {
    const firstCleanup = deferred<void>()
    const secondCleanup = deferred<void>()
    const raw = new ControlledDriver()
    raw.plan(() => ControlledAttempt.ready(firstCleanup.promise))
    raw.plan(() => ControlledAttempt.ready(secondCleanup.promise))
    const manager = new SubscriptionManager(raw, vi.fn())
    const received: string[] = []
    const first = manager.subscribe('source', (payload) => void received.push(`a:${decoder.decode(payload)}`))
    const second = manager.subscribe('source', (payload) => void received.push(`b:${decoder.decode(payload)}`))
    await first.ready
    expect(raw.opens).toHaveLength(1)
    expect(raw.opens[0]!.localReceiverCount()).toBe(2)
    raw.opens[0]!.attempt.close()
    expect(first.state()).toBe('closed')
    expect(second.state()).toBe('closed')
    expect(raw.opens).toHaveLength(1)
    await raw.deliver(0, 'stale')
    expect(received).toEqual([])
    const replacement = manager.subscribe('source', (payload) => void received.push(`c:${decoder.decode(payload)}`))
    await replacement.ready
    expect(raw.opens).toHaveLength(2)
    await raw.deliver(1, 'current')
    expect(received).toEqual(['c:current'])
    const retiring = Promise.all([first.unsubscribe(), second.unsubscribe()])
    firstCleanup.resolve()
    await retiring
    const stopping = replacement.unsubscribe()
    secondCleanup.resolve()
    await stopping
  })
  it('does not convert pending raw cleanup into successful settlement', async () => {
    const unsubscribeCleanup = deferred<void>()
    const disposeCleanup = deferred<void>()
    const terminalCleanup = deferred<void>()
    const raw = new ControlledDriver()
    raw.plan(() => ControlledAttempt.ready(unsubscribeCleanup.promise))
    raw.plan(() => ControlledAttempt.ready(disposeCleanup.promise))
    raw.plan(() => ControlledAttempt.ready(terminalCleanup.promise))
    const manager = new SubscriptionManager(raw)
    const first = manager.subscribe('unsubscribe', () => {})
    const second = manager.subscribe('dispose', () => {})
    const terminal = manager.subscribe('already-terminal', () => {})
    await Promise.all([first.ready, second.ready, terminal.ready])
    raw.opens[2]!.attempt.close()
    let unsubscribeSettled = false
    let terminalUnsubscribeSettled = false
    let disposeSettled = false
    const unsubscribing = first.unsubscribe().then(() => (unsubscribeSettled = true))
    const terminalUnsubscribing = terminal.unsubscribe().then(() => (terminalUnsubscribeSettled = true))
    const disposing = manager.dispose().then(() => (disposeSettled = true))
    expect(unsubscribeSettled).toBe(false)
    expect(terminalUnsubscribeSettled).toBe(false)
    expect(disposeSettled).toBe(false)
    unsubscribeCleanup.resolve()
    await unsubscribing
    expect(unsubscribeSettled).toBe(true)
    expect(terminalUnsubscribeSettled).toBe(false)
    expect(disposeSettled).toBe(false)
    terminalCleanup.resolve()
    await terminalUnsubscribing
    await Promise.resolve()
    expect(disposeSettled).toBe(false)
    disposeCleanup.resolve()
    await disposing
    expect(unsubscribeSettled).toBe(true)
    expect(terminalUnsubscribeSettled).toBe(true)
    expect(disposeSettled).toBe(true)
  })
  it('isolates throwing state listeners from siblings and last-detach cleanup', async () => {
    const cleanup = deferred<void>()
    const raw = new ControlledDriver()
    raw.plan(() => ControlledAttempt.ready(cleanup.promise))
    const reports: unknown[] = []
    const subscription = new SubscriptionManager(raw, (error) => reports.push(error)).subscribe('listeners', () => {})
    await subscription.ready
    let siblingCalls = 0
    subscription.onStateChange(() => {
      throw new Error('listener exploded')
    })
    subscription.onStateChange(() => siblingCalls++)
    let settled = false
    const stopping = subscription.unsubscribe().then(
      () => {
        settled = true
        return 'resolved'
      },
      () => 'rejected',
    )
    await Promise.resolve()
    expect(siblingCalls).toBe(1)
    expect(raw.opens[0]!.attempt.unsubscribeCalls).toBe(1)
    expect(settled).toBe(false)
    cleanup.resolve()
    await expect(stopping).resolves.toBe('resolved')
    expect(reports).toHaveLength(1)
  })
  it('does not emit a stale nonterminal state after re-entrant unsubscribe', async () => {
    const raw = new ControlledDriver()
    const subscription = new SubscriptionManager(raw, vi.fn()).subscribe('reentrant-listener', () => {})
    await subscription.ready
    const siblingStates: SubscriptionState[] = []
    subscription.onStateChange((state) => {
      if (state === 'lost') void subscription.unsubscribe()
    })
    subscription.onStateChange((state) => siblingStates.push(state))
    raw.opens[0]!.attempt.lose()
    expect(siblingStates).toEqual(['closed'])
  })
  it('normalizes initial readiness and surfaces raw recovery or terminal failure', async () => {
    const raw = new ControlledDriver()
    raw.plan(() => new ControlledAttempt())
    const manager = new SubscriptionManager(raw, vi.fn())
    const subscription = manager.subscribe('async-ready', () => {})
    const states: SubscriptionState[] = []
    subscription.onStateChange((state) => states.push(state))
    raw.opens[0]!.attempt.establish()
    await subscription.ready
    expect(states).toEqual([])
    raw.opens[0]!.attempt.lose()
    const recovered = subscription.ready
    raw.opens[0]!.attempt.establish()
    await recovered
    expect(states).toEqual(['lost', 'ready'])
    expect(raw.openCalls).toBe(1)
    raw.opens[0]!.attempt.close()
    expect(states).toEqual(['lost', 'ready', 'closed'])
    await expect(subscription.ready).rejects.toThrow('Backend subscription closed')
    expect(raw.openCalls).toBe(1)
    await subscription.unsubscribe()
    expect(states).toEqual(['lost', 'ready', 'closed'])
    const failedRaw = new ControlledDriver()
    failedRaw.plan(() => new ControlledAttempt())
    const failed = new SubscriptionManager(failedRaw).subscribe('initial-failure', () => {})
    const failedStates: SubscriptionState[] = []
    failed.onStateChange((state) => failedStates.push(state))
    const failedReadiness = failed.ready
    failedRaw.opens[0]!.attempt.close()
    await expect(failedReadiness).rejects.toThrow('Backend subscription closed')
    expect(failedStates).toEqual(['closed'])
    expect(failedRaw.openCalls).toBe(1)
    await failed.unsubscribe()
  })
  it("keeps a driver's reason for an end as the failure's cause", async () => {
    const raw = new ControlledDriver()
    raw.plan(() => new ControlledAttempt())
    const subscription = new SubscriptionManager(raw).subscribe('with-reason', () => {})
    const readiness = subscription.ready
    const reason = new Error('room has no open incarnation')
    raw.opens[0]!.attempt.close(reason)
    await expect(readiness).rejects.toMatchObject({
      message: 'Backend subscription closed: with-reason',
      cause: reason,
    })
    await subscription.unsubscribe()
  })
  it('includes the opaque driver partition in source identity', async () => {
    const raw = new ControlledDriver()
    raw.plan(() => ControlledAttempt.ready())
    raw.plan(() => ControlledAttempt.ready())
    const manager = new SubscriptionManager(raw)
    const received: string[] = []
    raw.partition = 'session-a'
    const first = manager.subscribe('same-source', (payload) => void received.push(`a:${decoder.decode(payload)}`))
    raw.partition = 'session-b'
    const second = manager.subscribe('same-source', (payload) => void received.push(`b:${decoder.decode(payload)}`))
    await Promise.all([first.ready, second.ready])
    expect(raw.opens).toHaveLength(2)
    await raw.deliver(0, 'one')
    await raw.deliver(1, 'two')
    expect(received).toEqual(['a:one', 'b:two'])
    await Promise.all([first.unsubscribe(), second.unsubscribe()])
  })
  it('maps raw ownership termination to public closed without replanning', async () => {
    const raw = new ControlledDriver()
    raw.plan(() => new ControlledAttempt())
    raw.plan(() => ControlledAttempt.ready())
    const manager = new SubscriptionManager(raw)
    const subscription = manager.subscribe('session', () => {})
    const states: SubscriptionState[] = []
    subscription.onStateChange((state) => states.push(state))
    raw.opens[0]!.attempt.establish()
    await subscription.ready
    raw.opens[0]!.attempt.terminate()
    await vi.waitFor(() => expect(subscription.state()).toBe('closed'))
    expect(states).toEqual(['closed'])
    expect(raw.openCalls).toBe(1)
    const replacement = manager.subscribe('session', () => {})
    await replacement.ready
    expect(raw.openCalls).toBe(2)
    await subscription.unsubscribe()
    const sibling = manager.subscribe('session', () => {})
    await sibling.ready
    expect(raw.openCalls).toBe(2)
    await Promise.all([replacement.unsubscribe(), sibling.unsubscribe()])
    const pendingRaw = new ControlledDriver()
    pendingRaw.plan(() => new ControlledAttempt())
    const pending = new SubscriptionManager(pendingRaw).subscribe('pending-session', () => {})
    const readiness = pending.ready
    pendingRaw.opens[0]!.attempt.terminate()
    await expect(readiness).rejects.toThrow('ownership terminated')
    expect(pending.state()).toBe('closed')
    expect(pendingRaw.openCalls).toBe(1)
    await pending.unsubscribe()
  })
})
type OpenRecord = {
  receiver: BackendReceiver
  localReceiverCount: () => number
  attempt: ControlledAttempt
}
class ControlledDriver implements SubscriptionDriver<string> {
  readonly opens: OpenRecord[] = []
  readonly #plans: Array<() => ControlledAttempt> = []
  partition = ''
  openCalls = 0
  plan(plan: () => ControlledAttempt): void {
    this.#plans.push(plan)
  }
  bind(_source: string) {
    const partition = this.partition
    return {
      partition,
      open: (receiver: BackendReceiver, localReceiverCount: () => number): SubscriptionAttempt => {
        this.openCalls++
        const attempt = (this.#plans.shift() ?? (() => ControlledAttempt.ready()))()
        this.opens.push({ receiver, localReceiverCount, attempt })
        return attempt
      },
    }
  }
  async deliver(index: number, value: string): Promise<void> {
    await (this.opens[index]!.receiver(encoder.encode(value), { seq: index + 1, timestamp: 1 }) as unknown)
  }
}
class ControlledAttempt implements SubscriptionAttempt {
  unsubscribeCalls = 0
  readonly #listeners = new Set<(state: SubscriptionAttemptState, reason?: Error) => void>()
  readonly #cleanup: Promise<void>
  #state: SubscriptionAttemptState = 'establishing'
  constructor(cleanup: Promise<void> = Promise.resolve()) {
    this.#cleanup = cleanup
  }
  static ready(cleanup?: Promise<void>): ControlledAttempt {
    const attempt = new ControlledAttempt(cleanup)
    attempt.establish()
    return attempt
  }
  state(): SubscriptionAttemptState {
    return this.#state
  }
  onStateChange(listener: (state: SubscriptionAttemptState, reason?: Error) => void): () => void {
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
  }
  lose(): void {
    this.#transition('lost')
  }
  close(reason?: Error): void {
    this.#transition('closed', reason)
  }
  terminate(): void {
    this.#transition('terminated')
  }
  #transition(state: SubscriptionAttemptState, reason?: Error): void {
    this.#state = state
    for (const listener of this.#listeners) listener(state, reason)
  }
}
