// A request body's files, read, cancelled and failed in each order the body's one cursor must hold, on a Node
// `Readable` and on a Web body. The row sends the bytes, the body's end and its failure itself, at the points it names;
// it then lets the event loop run dry (`settle`, a count of turns, no clock) and reads what each promise came to. A read
// that waits for good reads `pending`, decided by the row's own gates, never by a timeout.

import { Readable } from 'node:stream'
import { expect, test } from 'vitest'
import { BodyReader } from './BodyReader.js'
import { StreamTruncatedError } from './StreamTruncatedError.js'
import { queueMemory } from '../channel/server/channelMemory.js'
import { createDeferred } from '../../utils/createDeferred.js'
import { LazyFile } from '../values/LazyFile.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'

type Kind = 'node' | 'web'
type Outcome = { state: 'pending' } | { state: 'done'; value: string } | { state: 'failed'; error: string }

/** A body the row feeds: `push` hands bytes (latin1), `end` its end, `fail` its failure; `unread` says whether it holds
 *  bytes nobody read yet. */
function sourceOf(kind: Kind) {
  if (kind === 'node') {
    const readable = new Readable({ read() {} })
    return {
      body: readable,
      push: (text: string) => void readable.push(Buffer.from(text, 'latin1')),
      end: () => void readable.push(null),
      fail: (error: Error) => void readable.destroy(error),
      unread: () => readable.readableLength > 0,
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
  const controller = () => {
    if (!sink.controller) throw new Error('no controller')
    return sink.controller
  }
  return {
    body,
    push: (text: string) => controller().enqueue(Uint8Array.from(text, (c) => c.charCodeAt(0))),
    end: () => controller().close(),
    fail: (error: Error) => controller().error(error),
    unread: () => (controller().desiredSize ?? 0) < 0,
  }
}

/** The event loop's queued work, run to its end: `turns` macrotask turns, each after its microtasks. */
async function settle(turns = 50) {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** What `promise` came to so far. */
function track(promise: Promise<string>) {
  const outcome: { now: Outcome } = { now: { state: 'pending' } }
  promise.then(
    (value) => void (outcome.now = { state: 'done', value }),
    (error: unknown) =>
      void (outcome.now = { state: 'failed', error: error instanceof Error ? error.message : String(error) }),
  )
  return outcome
}

/** A stream read to its end: its text, or its failure with the text read before it. */
const text = (stream: ReadableStream<Uint8Array<ArrayBuffer>>) => textOf(stream.getReader())

/** A stream's reader read to the stream's end, as `text`. */
async function textOf(reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>): Promise<string> {
  let read = ''
  try {
    for (;;) {
      const { value, done } = await reader.read()
      if (done) return read
      read += String.fromCharCode(...value)
    }
  } catch (error) {
    throw new Error(
      `${read === '' ? '' : `after "${read}": `}${error instanceof Error ? error.message : String(error)}`,
    )
  }
}

const settled = (promise: Promise<unknown>) => track(promise.then(() => 'settled'))
/** What the ledger holds, for every holder together: a row reads it against what it held at the row's start, as a row
 *  that leaves a file unread and its reader open keeps what was read ahead for it. */
const ledger = getGlobalObject<{ total: number }>('channelMemory.ts', { total: 0 })
const turn = () => new Promise((resolve) => setImmediate(resolve))
const latin1 = (bytes: Uint8Array | undefined) => String.fromCharCode(...(bytes ?? []))
/** What a truncated read throws, by who stopped the body. */
const CLIENT_LEFT = new StreamTruncatedError('client').message
const BODY_ENDED = new StreamTruncatedError('body').message
const ANSWERED = new StreamTruncatedError('server').message
const A = 'a'.repeat(10)
const B = 'b'.repeat(10)
const C = 'c'.repeat(10)

/** A reader of a body of `{}` and files of `sizes` (a, b and c), its metadata read; `file(i)` is file `i`'s stream, as
 *  the File reviver gives it. */
async function setup(kind: Kind, sizes = [10, 10, 10], clientLeft = false) {
  const source = sourceOf(kind)
  source.push('\0\0\0\x02{}')
  const reader = new BodyReader(source.body, () => clientLeft, queueMemory)
  await reader.readFrame(1024)
  const files = sizes.map((size, index) => reader.registerFile(index, size))
  const file = (index: number) => {
    const registered = files[index]
    if (!registered) throw new Error(`no file ${index}`)
    return registered.stream()
  }
  /** No File reads file `index` anymore, as when its File is collected. */
  const letGo = (index: number) => files[index]?.letGo()
  return { source, reader, file, letGo }
}

/** File 0's first 4 bytes read by the app, its stream left reading ahead for more. */
async function midFile(kind: Kind, clientLeft = false) {
  const { source, file } = await setup(kind, undefined, clientLeft)
  const first = file(0).getReader()
  source.push('aaaa')
  const got = track(first.read().then(({ value }) => String.fromCharCode(...(value ?? []))))
  await settle()
  return { source, file, first, got }
}

/** File 0 locked and unread, file 1's read waiting behind the first piece read ahead for file 0. */
async function waitingBehindFirst(kind: Kind) {
  const { source, reader, file } = await setup(kind, [20, 10, 10])
  const first = file(0).getReader()
  const next = track(text(file(1)))
  await settle()
  source.push(A)
  await settle()
  return { source, reader, first, next }
}

const rows: [string, (kind: Kind) => Promise<unknown>, unknown][] = [
  [
    "W1. file 0's reader taken, file 1's read waiting behind it, then file 0's reader released unread: file 1 reads its own bytes",
    async (kind) => {
      const { source, file } = await setup(kind)
      const first = file(0).getReader()
      const next = track(text(file(1)))
      await settle()
      first.releaseLock()
      source.push(A + B + C)
      source.end()
      await settle()
      return { next: next.now }
    },
    { next: { state: 'done', value: B } },
  ],
  [
    "W2. file 0 read, file 2's read waiting behind it and cancelled, then file 1 read once file 0 is done: file 1 reads its own bytes",
    async (kind) => {
      const { source, file } = await setup(kind)
      const first = file(0).getReader()
      const whole = track(textOf(first))
      const third = file(2).getReader()
      const waiting = track(third.read().then(({ done }) => (done ? 'done' : 'value')))
      await settle()
      const cancel = settled(third.cancel())
      await settle()
      source.push(A + B + C)
      source.end()
      await settle()
      const next = track(text(file(1)))
      await settle()
      return { whole: whole.now, waiting: waiting.now, cancel: cancel.now, next: next.now }
    },
    {
      whole: { state: 'done', value: A },
      waiting: { state: 'done', value: 'done' },
      cancel: { state: 'done', value: 'settled' },
      next: { state: 'done', value: B },
    },
  ],
  [
    "W3. file 0's first piece read and second piece read ahead, then its reader released: file 1 reads its own bytes",
    async (kind) => {
      const { source, file } = await setup(kind, [30, 10, 10])
      const first = file(0).getReader()
      source.push(A)
      const { value } = await first.read()
      const next = track(text(file(1)))
      await settle()
      source.push(A)
      await settle()
      const ahead = !source.unread()
      first.releaseLock()
      source.push(A + B + C)
      source.end()
      await settle()
      return { got: String.fromCharCode(...(value ?? [])), ahead, next: next.now }
    },
    { got: A, ahead: true, next: { state: 'done', value: B } },
  ],
  [
    "W4. file 1's read waits behind file 0, locked and unread, while two pieces of file 0 come: one is read for file 0, the other stays in the body; file 0 then reads both, and file 1 its own bytes",
    async (kind) => {
      const { source, file } = await setup(kind, [20, 10, 10])
      const first = file(0).getReader()
      const next = track(text(file(1)))
      await settle()
      source.push('a'.repeat(10))
      await settle()
      source.push('a'.repeat(10))
      await settle()
      const unread = source.unread()
      source.push(B + C)
      source.end()
      const whole = track(textOf(first))
      await settle()
      return { unread, whole: whole.now, next: next.now }
    },
    { unread: true, whole: { state: 'done', value: 'a'.repeat(20) }, next: { state: 'done', value: B } },
  ],
  [
    'W5. concurrent File reads finish when the earlier File releases its reader after read-ahead',
    async (kind) => {
      const { source, file } = await setup(kind, [20, 10])
      const first = new LazyFile(20, '', 'first.txt', 0, () => file(0))
      const second = new LazyFile(10, '', 'second.txt', 0, () => file(1))
      const stream = first.stream()
      const release = createDeferred()
      async function skipFirst() {
        const locked = stream.getReader()
        try {
          await release.promise
        } finally {
          locked.releaseLock()
        }
        await expect(locked.closed).rejects.toBeInstanceOf(TypeError)
        return 'skipped'
      }
      const both = track(
        Promise.all([skipFirst(), second.text()]).then(([skipped, contents]) => `${skipped}: ${contents}`),
      )
      await settle()
      source.push(A)
      await settle()
      const ahead = !source.unread()
      release.resolve()
      source.push(A + B)
      source.end()
      await settle()
      const outcome = both.now
      const skipped = track(text(stream))
      await settle()
      const unreadable =
        skipped.now.state === 'failed' &&
        skipped.now.error.includes('has already been consumed') &&
        !skipped.now.error.startsWith('after')
      return { ahead, outcome, unreadable }
    },
    { ahead: true, outcome: { state: 'done', value: 'skipped: ' + B }, unreadable: true },
  ],
  [
    "W6. file 0's reader is cancelled after a piece was read for it: file 1 reads its own bytes",
    async (kind) => {
      const { source, first, next } = await waitingBehindFirst(kind)
      const before = next.now
      const cancel = settled(first.cancel())
      const closed = settled(first.closed)
      source.push(A + B + C)
      source.end()
      await settle()
      return { before, cancel: cancel.now, closed: closed.now, next: next.now }
    },
    {
      before: { state: 'pending' },
      cancel: { state: 'done', value: 'settled' },
      closed: { state: 'done', value: 'settled' },
      next: { state: 'done', value: B },
    },
  ],
  [
    "W7. file 1 waits while file 0 stays locked after read-ahead: file 1 reads the client's truncation once the client leaves",
    async (kind) => {
      const { reader, next } = await waitingBehindFirst(kind)
      const before = next.now
      reader.clientGaveUp()
      await settle()
      return { before, next: next.now }
    },
    { before: { state: 'pending' }, next: { state: 'failed', error: CLIENT_LEFT } },
  ],
  [
    "W8. file 0's first piece read, its reader released, and a new one taken a turn later for its rest, while file 1 is read: both read their own bytes",
    async (kind) => {
      const { source, file } = await setup(kind, [20, 10])
      const stream = file(0)
      const peek = stream.getReader()
      source.push(A)
      const { value } = await peek.read()
      peek.releaseLock()
      const next = track(text(file(1)))
      source.push(A + B)
      source.end()
      await turn()
      const rest = track(text(stream))
      await settle()
      return { first: latin1(value), rest: rest.now, next: next.now }
    },
    { first: A, rest: { state: 'done', value: A }, next: { state: 'done', value: B } },
  ],
  [
    "W9. file 0's first piece read and its reader released, its rest coming in two pieces while file 1 is read: the piece behind the first read for it is held on the ledger till file 0's new reader takes it",
    async (kind) => {
      const { source, file } = await setup(kind, [30, 10])
      const base = ledger.total
      const stream = file(0)
      const peek = stream.getReader()
      source.push(A)
      const { value } = await peek.read()
      peek.releaseLock()
      const next = track(text(file(1)))
      await settle()
      source.push(A)
      await settle()
      source.push(A)
      await settle()
      const held = ledger.total - base
      source.push(B)
      source.end()
      await settle()
      const again = stream.getReader()
      const second = latin1((await again.read()).value)
      // The piece behind it is now the one the reader keeps for file 0.
      const heldOnceOneTaken = ledger.total - base
      const rest = track(textOf(again))
      await settle()
      return {
        first: latin1(value),
        held,
        next: next.now,
        second,
        heldOnceOneTaken,
        rest: rest.now,
        after: ledger.total - base,
      }
    },
    {
      first: A,
      held: A.length,
      next: { state: 'done', value: B },
      second: A,
      heldOnceOneTaken: 0,
      rest: { state: 'done', value: A },
      after: 0,
    },
  ],
  [
    "W12. file 0's first piece read and its reader released, its rest read for it while file 1 is read, then its File let go of: what was read ahead for it leaves the ledger",
    async (kind) => {
      const { source, file, letGo } = await setup(kind, [30, 10])
      const base = ledger.total
      const peek = file(0).getReader()
      source.push(A)
      await peek.read()
      peek.releaseLock()
      const next = track(text(file(1)))
      await settle()
      source.push(A)
      await settle()
      source.push(A + B)
      await settle()
      const held = ledger.total - base
      letGo(0)
      return { held, next: next.now, after: ledger.total - base }
    },
    { held: A.length, next: { state: 'done', value: B }, after: 0 },
  ],
  [
    "W13. file 0's first piece read and its reader released, its rest read for it while file 1 is read, then file 0 cancelled: what was read ahead for it leaves the ledger",
    async (kind) => {
      const { source, file } = await setup(kind, [30, 10])
      const base = ledger.total
      const stream = file(0)
      const peek = stream.getReader()
      source.push(A)
      await peek.read()
      peek.releaseLock()
      const next = track(text(file(1)))
      await settle()
      source.push(A)
      await settle()
      source.push(A + B)
      await settle()
      const held = ledger.total - base
      const cancel = settled(stream.cancel())
      await settle()
      return { held, cancel: cancel.now, next: next.now, after: ledger.total - base }
    },
    { held: A.length, cancel: { state: 'done', value: 'settled' }, next: { state: 'done', value: B }, after: 0 },
  ],
  [
    "W10. file 0's first piece read through `values({ preventCancel: true })` and a `break`, while file 1 is read: file 0's next reader reads its rest",
    async (kind) => {
      const { source, file } = await setup(kind, [20, 10])
      const base = ledger.total
      const stream = file(0)
      source.push(A)
      let first = ''
      for await (const chunk of stream.values({ preventCancel: true })) {
        first = latin1(chunk)
        break
      }
      const next = track(text(file(1)))
      source.push(A + B)
      source.end()
      await turn()
      const rest = track(text(stream))
      await settle()
      return { first, rest: rest.now, next: next.now, after: ledger.total - base }
    },
    { first: A, rest: { state: 'done', value: A }, next: { state: 'done', value: B }, after: 0 },
  ],
  [
    "W11. file 0 piped with `preventCancel: true` to a sink that fails at its first piece, while file 1 is read: file 0's next reader reads its rest",
    async (kind) => {
      const { source, file } = await setup(kind, [20, 10])
      const base = ledger.total
      const stream = file(0)
      let first = ''
      const sink = new WritableStream<Uint8Array>({
        write(chunk) {
          first += latin1(chunk)
          throw new Error('the sink is full')
        },
      })
      const piped = track(stream.pipeTo(sink, { preventCancel: true }).then(() => 'piped'))
      source.push(A)
      await settle()
      const next = track(text(file(1)))
      source.push(A + B)
      source.end()
      await settle()
      const rest = track(text(stream))
      await settle()
      return { first, piped: piped.now, rest: rest.now, next: next.now, after: ledger.total - base }
    },
    {
      first: A,
      piped: { state: 'failed', error: 'the sink is full' },
      rest: { state: 'done', value: A },
      next: { state: 'done', value: B },
      after: 0,
    },
  ],
  [
    '1. file 0 cancelled before its first byte: file 1 reads its own bytes',
    async (kind) => {
      const { source, file } = await setup(kind)
      const cancel = settled(file(0).cancel())
      source.push(A + B + C)
      source.end()
      const next = track(text(file(1)))
      await settle()
      return { cancel: cancel.now, next: next.now }
    },
    { cancel: { state: 'done', value: 'settled' }, next: { state: 'done', value: B } },
  ],
  [
    '2. file 0 cancelled mid-file between two reads of it, its rest still to come: file 1 reads its own bytes',
    async (kind) => {
      const { source, file, first, got } = await midFile(kind)
      const cancel = settled(first.cancel())
      source.push('aaaaaa' + B + C)
      source.end()
      const next = track(text(file(1)))
      await settle()
      return { got: got.now, cancel: cancel.now, next: next.now }
    },
    {
      got: { state: 'done', value: 'aaaa' },
      cancel: { state: 'done', value: 'settled' },
      next: { state: 'done', value: B },
    },
  ],
  [
    "3. file 0 cancelled mid-file, the app's own read waiting: that read ends, file 1 reads its own bytes",
    async (kind) => {
      const { source, file, first } = await midFile(kind)
      const waiting = track(first.read().then(({ done }) => (done ? 'ended' : 'a chunk')))
      await settle()
      const cancel = settled(first.cancel())
      source.push('aaaaaa' + B + C)
      source.end()
      const next = track(text(file(1)))
      await settle()
      return { waiting: waiting.now, cancel: cancel.now, next: next.now }
    },
    {
      waiting: { state: 'done', value: 'ended' },
      cancel: { state: 'done', value: 'settled' },
      next: { state: 'done', value: B },
    },
  ],
  [
    '4. file 0 cancelled mid-file, the rest of it received already: file 1 reads its own bytes',
    async (kind) => {
      const { source, file, first } = await midFile(kind)
      source.push('aaaaaa' + B + C)
      await settle()
      const cancel = settled(first.cancel())
      source.end()
      const next = track(text(file(1)))
      await settle()
      return { cancel: cancel.now, next: next.now }
    },
    { cancel: { state: 'done', value: 'settled' }, next: { state: 'done', value: B } },
  ],
  [
    '5. file 0 cancelled after its last byte: file 1 reads its own bytes',
    async (kind) => {
      const { source, file } = await setup(kind)
      source.push(A + B + C)
      source.end()
      const first = file(0).getReader()
      const whole = track(textOf(first))
      await settle()
      const cancel = settled(first.cancel())
      const next = track(text(file(1)))
      await settle()
      return { whole: whole.now, cancel: cancel.now, next: next.now }
    },
    {
      whole: { state: 'done', value: A },
      cancel: { state: 'done', value: 'settled' },
      next: { state: 'done', value: B },
    },
  ],
  [
    '6. file 1 cancelled as it is asked for, file 0 untouched: file 2 reads its own bytes',
    async (kind) => {
      const { source, file } = await setup(kind)
      const cancel = settled(file(1).cancel())
      source.push(A + B + C)
      source.end()
      const next = track(text(file(2)))
      await settle()
      return { cancel: cancel.now, next: next.now }
    },
    { cancel: { state: 'done', value: 'settled' }, next: { state: 'done', value: C } },
  ],
  [
    '7. file 0 cancelled mid-file, then the body ends short: file 1 reads a truncation',
    async (kind) => {
      const { source, file, first } = await midFile(kind)
      const cancel = settled(first.cancel())
      source.end()
      const next = track(text(file(1)))
      await settle()
      return { cancel: cancel.now, next: next.now }
    },
    { cancel: { state: 'done', value: 'settled' }, next: { state: 'failed', error: BODY_ENDED } },
  ],
  [
    '8. file 0 cancelled mid-file, then its client leaves: file 1 reads a truncation',
    async (kind) => {
      const { source, file, first } = await midFile(kind, true)
      const cancel = settled(first.cancel())
      source.fail(new Error('read ECONNRESET'))
      const next = track(text(file(1)))
      await settle()
      return { cancel: cancel.now, next: next.now }
    },
    { cancel: { state: 'done', value: 'settled' }, next: { state: 'failed', error: CLIENT_LEFT } },
  ],
  [
    "9. file 0 cancelled mid-file, then the body fails as the app's own error: file 1 reads that error",
    async (kind) => {
      const { source, file, first } = await midFile(kind)
      const cancel = settled(first.cancel())
      source.fail(new Error("the app's body failed"))
      const next = track(text(file(1)))
      await settle()
      return { cancel: cancel.now, next: next.now }
    },
    { cancel: { state: 'done', value: 'settled' }, next: { state: 'failed', error: "the app's body failed" } },
  ],
  [
    '10. file 1 cancelled while its read passes file 0, 4 of its bytes in: file 2 reads its own bytes',
    async (kind) => {
      const { source, file } = await setup(kind)
      const second = file(1).getReader()
      void second.read()
      source.push('aaaa')
      await settle()
      const cancel = settled(second.cancel())
      source.push('aaaaaa' + B + C)
      source.end()
      const next = track(text(file(2)))
      await settle()
      return { cancel: cancel.now, next: next.now }
    },
    { cancel: { state: 'done', value: 'settled' }, next: { state: 'done', value: C } },
  ],
  [
    '11. an empty file 1 cancelled while file 0 waits for data, the app awaiting that cancel: it settles, then file 0 and file 2 read their own bytes',
    async (kind) => {
      const { source, file } = await setup(kind, [10, 0, 10])
      const first = file(0).getReader()
      source.push('aaaa')
      const got = track(first.read().then(({ value }) => String.fromCharCode(...(value ?? []))))
      await settle()
      const cancel = settled(file(1).cancel())
      await settle()
      const beforeMoreData = cancel.now
      source.push('aaaaaa' + C)
      source.end()
      const rest = track(textOf(first))
      const next = track(text(file(2)))
      await settle()
      return { got: got.now, beforeMoreData, rest: rest.now, next: next.now }
    },
    {
      got: { state: 'done', value: 'aaaa' },
      beforeMoreData: { state: 'done', value: 'settled' },
      rest: { state: 'done', value: 'aaaaaa' },
      next: { state: 'done', value: C },
    },
  ],
  [
    '12 (R1). a read of file 0 after file 1 was read: it fails with its usage error, and file 2 reads its own bytes',
    async (kind) => {
      const { source, file } = await setup(kind)
      source.push(A + B + C)
      source.end()
      const second = track(text(file(1)))
      await settle()
      const first = track(text(file(0)))
      const third = track(text(file(2)))
      await settle()
      const usage = first.now.state === 'failed' && first.now.error.includes('has already been consumed')
      return { second: second.now, usage, third: third.now }
    },
    { second: { state: 'done', value: B }, usage: true, third: { state: 'done', value: C } },
  ],
  [
    "13. a read of file 1 whose pass of file 0 the body's end cuts: it reads a truncation, and so does file 2",
    async (kind) => {
      const { source, file } = await setup(kind)
      const second = track(text(file(1)))
      source.push('aaaa')
      source.end()
      await settle()
      const third = track(text(file(2)))
      await settle()
      return { second: second.now, third: third.now }
    },
    { second: { state: 'failed', error: BODY_ENDED }, third: { state: 'failed', error: BODY_ENDED } },
  ],
  [
    "14. a read of file 1 whose pass of file 0 fails as the app's own error: it reads that error, and so does file 2",
    async (kind) => {
      const { source, file } = await setup(kind)
      const second = track(text(file(1)))
      source.push('aaaa')
      await settle()
      source.fail(new Error("the app's body failed"))
      await settle()
      const third = track(text(file(2)))
      await settle()
      return { second: second.now, third: third.now }
    },
    {
      second: { state: 'failed', error: "the app's body failed" },
      third: { state: 'failed', error: "the app's body failed" },
    },
  ],
  [
    "15. the request ends while the body stays open, file 0's read waiting: it reads a truncation, and so does file 1; the body is read off to its end",
    async (kind) => {
      const { source, reader, file } = await setup(kind)
      source.push('aaaa')
      const first = track(text(file(0)))
      await settle()
      const readOff = settled(reader.discardRest('server'))
      await settle()
      const next = track(text(file(1)))
      await settle()
      const before = readOff.now
      source.push('aaaaaa' + B + C)
      source.end()
      await settle()
      return { first: first.now, next: next.now, readOffBeforeTheEnd: before, readOff: readOff.now }
    },
    {
      first: { state: 'failed', error: `after "aaaa": ${ANSWERED}` },
      next: { state: 'failed', error: ANSWERED },
      readOffBeforeTheEnd: { state: 'pending' },
      readOff: { state: 'done', value: 'settled' },
    },
  ],
  [
    '16. files 0 and 1 cancelled in one turn: file 2 reads its own bytes',
    async (kind) => {
      const { source, file } = await setup(kind)
      const cancels = [settled(file(0).cancel()), settled(file(1).cancel())]
      source.push(A + B + C)
      source.end()
      const next = track(text(file(2)))
      await settle()
      return { cancels: cancels.map(({ now }) => now), next: next.now }
    },
    {
      cancels: [
        { state: 'done', value: 'settled' },
        { state: 'done', value: 'settled' },
      ],
      next: { state: 'done', value: C },
    },
  ],
  [
    "17. the last file cancelled mid-file, its last bytes in with the body's end still to come: the cancel settles",
    async (kind) => {
      const { source, file } = await setup(kind, [10])
      const first = file(0).getReader()
      source.push('aaaa')
      await first.read()
      await settle()
      const cancel = settled(first.cancel())
      source.push('aaaaaa')
      await settle()
      const beforeTheEnd = cancel.now
      source.end()
      return { beforeTheEnd }
    },
    { beforeTheEnd: { state: 'done', value: 'settled' } },
  ],
  [
    '18. a cancel asked again while the first waits: both settle, file 1 reads its own bytes',
    async (kind) => {
      const { source, file, first } = await midFile(kind)
      const cancels = [settled(first.cancel()), settled(first.cancel())]
      source.push('aaaaaa' + B + C)
      source.end()
      const next = track(text(file(1)))
      await settle()
      return { cancels: cancels.map(({ now }) => now), next: next.now }
    },
    {
      cancels: [
        { state: 'done', value: 'settled' },
        { state: 'done', value: 'settled' },
      ],
      next: { state: 'done', value: B },
    },
  ],
  [
    "19. file 0's reader released with a read waiting and locked again, then cancelled: file 1 reads its own bytes",
    async (kind) => {
      const { source, file } = await setup(kind)
      const stream = file(0)
      const first = stream.getReader()
      source.push('aaaa')
      await first.read()
      const waiting = track(first.read().then(() => 'read'))
      await settle()
      first.releaseLock()
      const cancel = settled(stream.getReader().cancel())
      source.push('aaaaaa' + B + C)
      source.end()
      const next = track(text(file(1)))
      await settle()
      return { waitingFailed: waiting.now.state === 'failed', cancel: cancel.now, next: next.now }
    },
    { waitingFailed: true, cancel: { state: 'done', value: 'settled' }, next: { state: 'done', value: B } },
  ],
  [
    "20. an empty file 2 cancelled while file 1's start skips file 0 for it and waits for more, the app awaiting that cancel: it settles, then file 1 and file 3 read their own bytes",
    async (kind) => {
      const { source, file } = await setup(kind, [10, 10, 0, 10])
      const second = file(1)
      source.push('aaaa')
      await settle()
      const cancel = settled(file(2).cancel())
      await settle()
      const beforeMoreData = cancel.now
      source.push('aaaaaa' + B + C)
      source.end()
      const own = track(text(second))
      const next = track(text(file(3)))
      await settle()
      return { beforeMoreData, own: own.now, next: next.now }
    },
    {
      beforeMoreData: { state: 'done', value: 'settled' },
      own: { state: 'done', value: B },
      next: { state: 'done', value: C },
    },
  ],
  [
    "R2. file 0's reader released unread before file 1 is read: file 1 reads its own bytes, and file 0's read fails with its usage error",
    async (kind) => {
      const { source, file } = await setup(kind)
      const unread = file(0)
      unread.getReader().releaseLock()
      source.push(A + B + C)
      source.end()
      const next = track(text(file(1)))
      await settle()
      const late = track(text(unread))
      await settle()
      const usage = late.now.state === 'failed' && late.now.error.includes('has already been consumed')
      return { next: next.now, usage }
    },
    { next: { state: 'done', value: B }, usage: true },
  ],
  [
    'R3. file 1 piped to a sink and aborted while file 0 is read: file 0 and file 2 read their own bytes',
    async (kind) => {
      const { source, file } = await setup(kind)
      const first = track(text(file(0)))
      source.push('aaaa')
      await settle()
      const abort = new AbortController()
      const piped: string[] = []
      const piping = file(1).pipeTo(
        new WritableStream<Uint8Array>({ write: (chunk) => void piped.push(String.fromCharCode(...chunk)) }),
        { signal: abort.signal },
      )
      piping.catch(() => {})
      await settle()
      abort.abort()
      source.push('aaaaaa' + B + C)
      source.end()
      const next = track(text(file(2)))
      await settle()
      return { first: first.now, piped, next: next.now }
    },
    { first: { state: 'done', value: A }, piped: [], next: { state: 'done', value: C } },
  ],
  [
    'R5. file 0 read 4 bytes in, its reader released without a cancel, then file 1 read: file 1 reads its own bytes',
    async (kind) => {
      const { source, file, first, got } = await midFile(kind)
      first.releaseLock()
      source.push('aaaaaa' + B + C)
      source.end()
      const next = track(text(file(1)))
      await settle()
      return { got: got.now, next: next.now }
    },
    { got: { state: 'done', value: 'aaaa' }, next: { state: 'done', value: B } },
  ],
  [
    "21. the request ends while file 1's read passes file 0, part of the way: the pass reads no more, and the body is read off to its end",
    async (kind) => {
      const { source, reader, file } = await setup(kind)
      const second = track(text(file(1)))
      await settle()
      source.push('aaaa')
      await settle()
      const readOff = settled(reader.discardRest('server'))
      await settle()
      source.push('aa')
      await settle()
      source.push('aaaa' + B + C)
      source.end()
      await settle()
      return { second: second.now, readOff: readOff.now }
    },
    { second: { state: 'failed', error: ANSWERED }, readOff: { state: 'done', value: 'settled' } },
  ],
  [
    '22. a file asked for after the body was let go of: it reads a truncation',
    async (kind) => {
      const { reader, file } = await setup(kind)
      reader.letGo()
      const first = track(text(file(0)))
      await settle()
      return { first: first.now }
    },
    { first: { state: 'failed', error: ANSWERED } },
  ],
  [
    'R6. file 2 read while file 1 passes file 0, then the body ends short: both read a truncation',
    async (kind) => {
      const { source, file } = await setup(kind)
      const second = track(text(file(1)))
      await settle()
      const third = track(text(file(2)))
      await settle()
      source.push('aaaa')
      source.end()
      await settle()
      return { second: second.now, third: third.now }
    },
    { second: { state: 'failed', error: BODY_ENDED }, third: { state: 'failed', error: BODY_ENDED } },
  ],
  [
    "R13. the request ends through its client's abort between two reads of file 0: the next read reads the client's truncation",
    async (kind) => {
      const { source, reader, file } = await setup(kind)
      const first = file(0).getReader()
      source.push('aaaa')
      const got = track(first.read().then(({ value }) => String.fromCharCode(...(value ?? []))))
      await settle()
      settled(reader.discardRest('client'))
      const next = track(textOf(first))
      await settle()
      return { got: got.now, next: next.now }
    },
    { got: { state: 'done', value: 'aaaa' }, next: { state: 'failed', error: CLIENT_LEFT } },
  ],
]

for (const kind of ['node', 'web'] as const) {
  for (const [name, row, expected] of rows) {
    test(`${kind}: ${name}`, async () => {
      expect(await row(kind)).toEqual(expected)
    })
  }
}
