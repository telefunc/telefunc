export {
  onReturnReadableStream,
  onReturnAsyncGenerator,
  onReturnGeneratorWithMeta,
  onReturnEmptyGenerator,
  onReturnDelayedStream,
  onReturnDelayedGenerator,
  onReturnDelayedGeneratorWithMeta,
  onReturnStreamWithMeta,
  onReturnTwoGenerators,
  onReturnStreamAndGenerator,
  onReturnMultiplePromises,
  onReturnMixedEndless,
  onReturnDeadlockStream,
  onGeneratorAbortMidStream,
  onGeneratorAbortWithValue,
  onGeneratorBugMidStream,
  onUploadWithProgress,
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

// ── Streaming primitives (e2e tests) ────────────────────────────────

async function* onReturnAsyncGenerator(): AsyncGenerator<number> {
  for (const n of [1, 2, 3, 4, 5]) yield n
}

async function* onReturnEmptyGenerator(): AsyncGenerator<number> {}

async function* onReturnDelayedGenerator(): AsyncGenerator<string> {
  for (const word of ['alpha', 'beta', 'gamma', 'delta']) {
    await sleep(200)
    yield word
  }
}

const onReturnReadableStream = async (): Promise<ReadableStream<Uint8Array>> => {
  const encoder = new TextEncoder()
  const chunks = ['hello', ' ', 'stream']
  let i = 0
  return new ReadableStream({
    pull(controller) {
      if (i >= chunks.length) return controller.close()
      controller.enqueue(encoder.encode(chunks[i]!))
      i++
    },
  })
}

const onReturnDelayedStream = async (): Promise<ReadableStream<Uint8Array>> => {
  const encoder = new TextEncoder()
  const chunks = ['chunk1', 'chunk2', 'chunk3']
  let i = 0
  return new ReadableStream({
    async pull(controller) {
      if (i >= chunks.length) return controller.close()
      await sleep(200)
      controller.enqueue(encoder.encode(chunks[i]!))
      i++
    },
  })
}

// ── Streaming + metadata (e2e tests) ────────────────────────────────

const onReturnGeneratorWithMeta = async () => {
  async function* messages(): AsyncGenerator<string> {
    yield 'hello'
    yield 'world'
  }
  return { stream: messages(), timestamp: 1234567890, tags: ['a', 'b'] }
}

const onReturnDelayedGeneratorWithMeta = async () => {
  async function* countdown(): AsyncGenerator<number> {
    for (let i = 3; i >= 0; i--) {
      await sleep(200)
      yield i
    }
  }
  return { stream: countdown(), label: 'countdown' }
}

const onReturnStreamWithMeta = async () => {
  const encoder = new TextEncoder()
  const chunks = ['foo', 'bar', 'baz']
  let i = 0
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i >= chunks.length) return controller.close()
      controller.enqueue(encoder.encode(chunks[i]!))
      i++
    },
  })
  return { stream, count: 3 }
}

// ── Multiplexed streaming (e2e tests) ────────────────────────────────

import { getContext } from 'telefunc'
import { cleanupState } from '../abort/cleanup-state'

const onReturnTwoGenerators = async () => {
  cleanupState.twoGeneratorsAborted = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    cleanupState.twoGeneratorsAborted = String(Date.now())
  })
  async function* gen1(): AsyncGenerator<number> {
    for (const n of [1, 2, 3]) yield n
  }
  async function* gen2(): AsyncGenerator<number> {
    for (const n of [10, 20, 30]) yield n
  }
  return { first: gen1(), second: gen2() }
}

const onReturnStreamAndGenerator = async () => {
  cleanupState.streamAndGeneratorAborted = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    cleanupState.streamAndGeneratorAborted = String(Date.now())
  })
  const encoder = new TextEncoder()
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode('hi'))
      controller.close()
    },
  })
  async function* gen(): AsyncGenerator<number> {
    yield 1
  }
  return { stream, gen: gen() }
}

const onReturnMultiplePromises = async () => {
  cleanupState.multiplePromisesAborted = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    cleanupState.multiplePromisesAborted = String(Date.now())
  })
  const fast = Promise.resolve('quick')
  const slow = new Promise<string>((resolve) => setTimeout(() => resolve('delayed'), 1000))
  return { fast, slow, label: 'promises' }
}

// ── Deadlock test (e2e tests) ────────────────────────────────────────

// Stream is large enough (2 MB) to exceed the client-side demuxer buffer
// (1 MB) so that the demuxer stalls and cannot deliver further frames
// (including the promise resolution frame) until the client starts consuming
// the stream and drains the buffer.
const onReturnDeadlockStream = async () => {
  const encoder = new TextEncoder()
  const CHUNK = encoder.encode('x'.repeat(64 * 1024)) // 64 KB
  const CHUNKS = 32 // 32 × 64 KB = 2 MB
  let i = 0
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i >= CHUNKS) return controller.close()
      controller.enqueue(CHUNK)
      i++
    },
  })
  const promise = new Promise<string>((resolve) => setTimeout(() => resolve('uncorked'), 300))
  return { stream, promise }
}

// ── Per-tag cancel test (e2e tests) ──────────────────────────────────

const onReturnMixedEndless = async () => {
  console.time('onReturnMixedEndless')
  cleanupState.mixedEndless = 'running'
  cleanupState.mixedEndlessAborted = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    console.timeLog('onReturnMixedEndless', 'aborted')
    cleanupState.mixedEndless = 'cleaned-up'
    cleanupState.mixedEndlessAborted = String(Date.now())
  })
  async function* gen(): AsyncGenerator<string> {
    try {
      let i = 0
      while (true) {
        await sleep(100)
        console.timeLog('onReturnMixedEndless', `yielding g-${i}`)
        yield `g-${i++}`
      }
    } finally {
      console.timeLog('onReturnMixedEndless', 'gen finally')
    }
  }
  const slow = new Promise<string>((resolve) =>
    setTimeout(() => {
      console.timeLog('onReturnMixedEndless', 'slow resolved')
      resolve('done')
    }, 1000),
  )
  return { gen: gen(), slow }
}

// ── Mid-stream error cases (e2e tests) ──────────────────────────────

import { Abort } from 'telefunc'

async function* onGeneratorAbortMidStream(): AsyncGenerator<string> {
  yield 'before-abort'
  throw Abort()
}

async function* onGeneratorAbortWithValue(): AsyncGenerator<string> {
  yield 'before-abort'
  throw Abort({ reason: 'not-allowed', code: 403 })
}

async function* onGeneratorBugMidStream(): AsyncGenerator<string> {
  yield 'before-bug'
  throw new Error('Unexpected generator error')
}

// ── Upload progress via streaming (e2e tests) ──────────────────────

async function* onUploadWithProgress(file: File): AsyncGenerator<{ bytesRead: number; totalSize: number }> {
  const totalSize = file.size
  const stream = file.stream()
  const reader = stream.getReader()
  let bytesRead = 0
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    bytesRead += value.byteLength
    yield { bytesRead, totalSize }
  }
}
