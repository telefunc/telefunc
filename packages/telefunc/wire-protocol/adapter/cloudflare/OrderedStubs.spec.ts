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
