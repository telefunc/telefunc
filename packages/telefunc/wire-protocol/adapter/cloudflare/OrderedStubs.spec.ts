import { describe, expect, it } from 'vitest'
import { OrderedStubs } from './OrderedStubs.js'

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
})
