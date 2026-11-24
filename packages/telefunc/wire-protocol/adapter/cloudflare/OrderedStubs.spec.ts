import { afterEach, describe, expect, it, vi } from 'vitest'
import { CallNotSent, CallTimedOut, OrderedStubs } from './OrderedStubs.js'

type Stub = { send: (what: string) => Promise<void> }

function recordingStub(name: string, record: string[]): Stub {
  return { send: async (what) => void record.push(`${name}:${what}`) }
}

describe("a Durable Object's calls to another, through OrderedStubs", () => {
  it("that can't open a stub rejects the call instead of throwing, and the next call opens one again", async () => {
    const stubs = new OrderedStubs<Stub>()
    const record: string[] = []
    const refusal = new Error('the stub could not be opened')
    let opens = 0
    const open = () => {
      opens++
      if (opens === 1) throw refusal
      return recordingStub(`stub ${opens}`, record)
    }
    const calls: Promise<void>[] = []
    expect(() => calls.push(stubs.call('target', open, (stub) => stub.send('first')))).not.toThrow()
    await expect(calls[0]).rejects.toBe(refusal)
    await stubs.call('target', open, (stub) => stub.send('second'))
    expect(record).toEqual(['stub 2:second'])
  })

  it('made in one turn reach the stub in that turn, in the order they were made', () => {
    const stubs = new OrderedStubs<Stub>()
    const record: string[] = []
    const open = () => recordingStub('stub', record)
    void stubs.call('target', open, (stub) => stub.send('first'))
    void stubs.call('target', open, (stub) => stub.send('second'))
    expect(record).toEqual(['stub:first', 'stub:second'])
  })

  // A lease write is called before its grant is answered, and the calls made after it must not overtake it.
  it('made after one whose invoke is still to come reach the stub after it, whether it is sent or not', async () => {
    const stubs = new OrderedStubs<Stub>()
    const record: string[] = []
    // Its first call is never answered, so the line stays open throughout.
    const open = () => ({
      send: (what: string) => (record.push(what), what === 'first' ? new Promise<void>(() => {}) : Promise.resolve()),
    })
    const turns = () => new Promise((resolve) => setTimeout(resolve, 0))
    const write = Promise.withResolvers<(stub: Stub) => Promise<void>>()
    const refused = Promise.withResolvers<(stub: Stub) => Promise<void>>()
    void stubs.call('target', open, (stub) => stub.send('first'))
    const written = stubs.call('target', open, write.promise)
    void stubs.call('target', open, (stub) => stub.send('after the write'))
    const notSent = stubs.call('target', open, refused.promise).catch((error: Error) => error.message)
    void stubs.call('target', open, (stub) => stub.send('after the refused one'))
    refused.reject(new Error('no grant'))
    await turns()
    expect(record).toEqual(['first'])
    write.resolve((stub) => stub.send('the write'))
    await written
    expect(await notSent).toBe('no grant')
    await turns()
    expect(record).toEqual(['first', 'the write', 'after the write', 'after the refused one'])
    void stubs.call('target', open, (stub) => stub.send('last'))
    expect(record.at(-1)).toBe('last')
  })
})

describe("a Durable Object's call with a deadline, through OrderedStubs", () => {
  afterEach(() => void vi.useRealTimers())

  /** A target whose stubs answer each call as `answers` says, by what it sends, and record what reached them. */
  function target(answers: Record<string, Promise<void>>) {
    const record: string[] = []
    let opened = 0
    const open = (): Stub => {
      const name = `stub ${++opened}`
      return { send: (what) => (record.push(`${name}:${what}`), answers[what] ?? Promise.resolve()) }
    }
    return { record, open }
  }
  const outcome = (call: Promise<unknown>) =>
    call.then(
      () => 'answered',
      (error: unknown) => (error instanceof Error ? error.constructor.name : 'thrown'),
    )
  const overloaded = () => Object.assign(new Error('overloaded'), { overloaded: true })

  it("that waits behind a call of a failed stub the target never answers rejects at its deadline as not sent, and isn't sent after", async () => {
    vi.useFakeTimers()
    const stubs = new OrderedStubs<Stub>()
    const { record, open } = target({ hung: new Promise(() => {}), failed: Promise.reject(new Error('reset')) })
    void stubs.call('target', open, (stub) => stub.send('hung'))
    void stubs.call('target', open, (stub) => stub.send('failed')).catch(() => {})
    await vi.advanceTimersByTimeAsync(0)
    const later = outcome(stubs.call('target', open, (stub) => stub.send('later'), { deadline: Date.now() + 10_000 }))
    await vi.advanceTimersByTimeAsync(10_000)
    expect(await later).toBe(CallNotSent.name)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(record).toEqual(['stub 1:hung', 'stub 1:failed'])
  })

  it('that its target answers after its deadline keeps its place until then: a call made after it on a fresh stub goes once it is answered', async () => {
    vi.useFakeTimers()
    const stubs = new OrderedStubs<Stub>()
    const slow = Promise.withResolvers<void>()
    const { record, open } = target({ slow: slow.promise, failed: Promise.reject(new Error('reset')) })
    const timedOut = outcome(stubs.call('target', open, (stub) => stub.send('slow'), { deadline: Date.now() + 10_000 }))
    void stubs.call('target', open, (stub) => stub.send('failed')).catch(() => {})
    await vi.advanceTimersByTimeAsync(10_000)
    expect(await timedOut).toBe(CallTimedOut.name)
    void stubs.call('target', open, (stub) => stub.send('next'))
    await vi.advanceTimersByTimeAsync(5_000)
    expect(record).toEqual(['stub 1:slow', 'stub 1:failed'])
    slow.resolve()
    await vi.advanceTimersByTimeAsync(0)
    expect(record).toEqual(['stub 1:slow', 'stub 1:failed', 'stub 2:next'])
  })

  it('that its target answers overloaded after its deadline pauses the target for the calls that obey its pause', async () => {
    vi.useFakeTimers()
    const stubs = new OrderedStubs<Stub>()
    const slow = Promise.withResolvers<void>()
    const { record, open } = target({ slow: slow.promise })
    const timedOut = outcome(stubs.call('target', open, (stub) => stub.send('slow'), { deadline: Date.now() + 10_000 }))
    await vi.advanceTimersByTimeAsync(11_000)
    slow.reject(overloaded())
    await vi.advanceTimersByTimeAsync(0)
    const paused = outcome(stubs.call('target', open, (stub) => stub.send('paused'), { paused: true }))
    expect({ timedOut: await timedOut, paused: await paused, record }).toEqual({
      timedOut: CallTimedOut.name,
      paused: CallNotSent.name,
      record: ['stub 1:slow'],
    })
  })

  it('on a fresh stub whose calls all reached their deadlines unsent leaves the failed stub ahead of the calls after them', async () => {
    vi.useFakeTimers()
    const stubs = new OrderedStubs<Stub>()
    const hung = Promise.withResolvers<void>()
    const { record, open } = target({ hung: hung.promise, failed: Promise.reject(new Error('reset')) })
    void stubs.call('target', open, (stub) => stub.send('hung'))
    void stubs.call('target', open, (stub) => stub.send('failed')).catch(() => {})
    await vi.advanceTimersByTimeAsync(0)
    const unsent = outcome(stubs.call('target', open, (stub) => stub.send('unsent'), { deadline: Date.now() + 5_000 }))
    await vi.advanceTimersByTimeAsync(6_000)
    void stubs.call('target', open, (stub) => stub.send('after'))
    await vi.advanceTimersByTimeAsync(0)
    expect({ unsent: await unsent, record }).toEqual({
      unsent: CallNotSent.name,
      record: ['stub 1:hung', 'stub 1:failed'],
    })
    hung.resolve()
    await vi.advanceTimersByTimeAsync(0)
    expect(record).toEqual(['stub 1:hung', 'stub 1:failed', 'stub 2:after'])
  })

  it('answered in its time leaves no timer', async () => {
    vi.useFakeTimers()
    const stubs = new OrderedStubs<Stub>()
    const { open } = target({})
    await stubs.call('target', open, (stub) => stub.send('answered'), { deadline: Date.now() + 10_000 })
    expect(vi.getTimerCount()).toBe(0)
  })
})
