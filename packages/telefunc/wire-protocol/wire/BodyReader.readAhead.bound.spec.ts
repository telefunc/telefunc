// BodyReader.fileCancel.spec.ts's W9 past the bound: a File the app began and left without a reader has its rest read
// for it while a later File is read, held on the channels' ledger, so at most `config.channel.maxPendingBytes` of it,
// here 2 MiB. Past that, the later File still reads its own bytes, and the earlier one fails its next read, saying why.
// `config.channel` can't change once a channel used it, so these rows have a file of their own.

import { Readable } from 'node:stream'
import { expect, test } from 'vitest'
import { BodyReader } from './BodyReader.js'
import { config } from '../../node/server/serverConfig.js'
import { queueMemory } from '../channel/server/channelMemory.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'

// The least it may be.
const MAX_PENDING_BYTES = 2 * 1024 * 1024
config.channel = { maxPendingBytes: MAX_PENDING_BYTES }

/** What the ledger holds, for every holder together. */
const ledger = getGlobalObject<{ total: number }>('channelMemory.ts', { total: 0 })
const PIECE = 64 * 1024
const FIRST_SIZE = 3 * 1024 * 1024
const SECOND = 'b'.repeat(10)

/** A body the row feeds, a piece at a time: `push` hands bytes, `end` its end. */
function sourceOf(kind: 'node' | 'web') {
  if (kind === 'node') {
    const readable = new Readable({ read() {} })
    return {
      body: readable,
      push: (bytes: Uint8Array) => void readable.push(bytes),
      end: () => void readable.push(null),
    }
  }
  const sink: { controller: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>> | null } = { controller: null }
  const body = new ReadableStream<Uint8Array<ArrayBuffer>>(
    {
      start(controller) {
        sink.controller = controller
      },
    },
    { highWaterMark: 0 },
  )
  return {
    body,
    push: (bytes: Uint8Array<ArrayBuffer>) => sink.controller?.enqueue(bytes),
    end: () => sink.controller?.close(),
  }
}

/** The event loop's queued work, run to its end: `turns` macrotask turns, each after its microtasks. */
async function settle(turns = 50) {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** A stream read to its end, as text. */
async function textOf(stream: ReadableStream<Uint8Array<ArrayBuffer>>): Promise<string> {
  const decoder = new TextDecoder()
  let text = ''
  for await (const chunk of stream) text += decoder.decode(chunk, { stream: true })
  return text
}

test.each<'node' | 'web'>(['node', 'web'])(
  "%s: file 0's first piece read and its reader released, its rest past what the ledger holds for it: file 1 reads its own bytes, and file 0's next read fails, saying why",
  async (kind) => {
    const source = sourceOf(kind)
    source.push(new Uint8Array([0, 0, 0, 2, ...new TextEncoder().encode('{}')]))
    const reader = new BodyReader(source.body, () => false, queueMemory)
    await reader.readFrame(1024)
    const files = [reader.registerFile(0, FIRST_SIZE), reader.registerFile(1, SECOND.length)]
    const base = ledger.total
    const first = files[0]?.stream()
    if (!first) throw new Error('no file 0')
    const peek = first.getReader()
    source.push(new Uint8Array(PIECE))
    await peek.read()
    peek.releaseLock()
    const second = textOf(files[1]?.stream() ?? new ReadableStream())
    let peak = 0
    for (let sent = PIECE; sent < FIRST_SIZE; sent += PIECE) {
      source.push(new Uint8Array(PIECE))
      await settle(2)
      peak = Math.max(peak, ledger.total - base)
    }
    source.push(new TextEncoder().encode(SECOND))
    source.end()
    expect(await second).toBe(SECOND)
    const rest = await textOf(first).then(
      () => 'read',
      (error: unknown) => (error instanceof Error ? error.message : String(error)),
    )
    // It held what it could: within the bound, a piece short of it at most.
    expect({
      rest,
      held: peak > MAX_PENDING_BYTES - PIECE && peak <= MAX_PENDING_BYTES,
      after: ledger.total - base,
    }).toEqual({
      rest: expect.stringContaining('File argument 0 was read in part and left without a reader'),
      held: true,
      after: 0,
    })
  },
)
