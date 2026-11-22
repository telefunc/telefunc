// A file's chunk the app hands on with its buffer transferred (`postMessage(chunk, [chunk.buffer])`), which detaches every
// other view of that buffer: what the reader reads after it is still whole. Each source chunk has an ArrayBuffer of its
// own, as Node's HTTP server gives a request body's chunks. On a Node `Readable` and on a Web body.

import { Readable } from 'node:stream'
import { expect, test } from 'vitest'
import { BodyReader } from './BodyReader.js'

type Kind = 'node' | 'web'

/** One chunk of `text` (latin1), in an ArrayBuffer of its own. */
function chunkOf(text: string): Uint8Array<ArrayBuffer> {
  return Uint8Array.from(text, (c) => c.charCodeAt(0))
}

/** A body that gives `chunks` and ends. */
function bodyOf(kind: Kind, chunks: Uint8Array<ArrayBuffer>[]) {
  if (kind === 'node')
    return Readable.from(chunks.map((chunk) => Buffer.from(chunk.buffer, chunk.byteOffset, chunk.length)))
  return new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk)
      controller.close()
    },
  })
}

/** Reads a file's stream to its end, transferring the buffer of each chunk as it comes, as an app that hands each on. */
async function readTransferring(stream: ReadableStream<Uint8Array<ArrayBuffer>>): Promise<string> {
  const reader = stream.getReader()
  let read = ''
  for (;;) {
    const { value, done } = await reader.read()
    if (done) return read
    read += String.fromCharCode(...value)
    structuredClone(value.buffer, { transfer: [value.buffer] })
  }
}

/** A reader of `chunks`, a body of `{}` and files of `sizes`, its metadata read; its files' streams. */
async function setup(kind: Kind, chunks: string[], sizes: number[]) {
  const reader = new BodyReader(bodyOf(kind, chunks.map(chunkOf)), () => false)
  await reader.readFrame(1024)
  return sizes.map((size, index) => reader.registerFile(index, size))
}

const META = '\0\0\0\x02{}'
const A = 'a'.repeat(10)
const B = 'b'.repeat(10)

async function textOf(getStream: (() => ReadableStream<Uint8Array<ArrayBuffer>>) | undefined) {
  if (!getStream) throw new Error('no such file')
  try {
    return await new Response(getStream()).text()
  } catch (error) {
    return `failed: ${error instanceof Error ? error.message : String(error)}`
  }
}

for (const kind of ['node', 'web'] as const) {
  test(`${kind}: file 0's chunk transferred, file 1's bytes in the same chunk as it and the metadata: file 1 reads its own bytes`, async () => {
    const [first, second] = await setup(kind, [META + A + B], [10, 10])
    if (!first) throw new Error('no file 0')
    expect({ first: await readTransferring(first()), second: await textOf(second) }).toEqual({ first: A, second: B })
  })

  test(`${kind}: file 0's chunk transferred, file 1's bytes in the chunk it was read from: file 1 reads its own bytes`, async () => {
    const [first, second] = await setup(kind, [META, A + B], [10, 10])
    if (!first) throw new Error('no file 0')
    expect({ first: await readTransferring(first()), second: await textOf(second) }).toEqual({ first: A, second: B })
  })

  // A chunk inside a file is handed on whole, and the reader keeps no other view of it.
  test(`${kind}: a chunk inside file 0 transferred: the rest of file 0 and file 1 read their own bytes`, async () => {
    const [first, second] = await setup(kind, [META, A, A, B], [20, 10])
    if (!first) throw new Error('no file 0')
    expect({ first: await readTransferring(first()), second: await textOf(second) }).toEqual({
      first: A + A,
      second: B,
    })
  })
}
