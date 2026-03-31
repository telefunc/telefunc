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
  onReturnAsymmetricGenerators,
  onGeneratorAbortMidStream,
  onGeneratorAbortWithValue,
  onGeneratorBugMidStream,
  onAbortOneOfManyStreamingValues,
  onChannelAbortAbortsSiblingStreamingValues,
  onUploadWithProgress,
}

import { Abort, channel, getContext } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { sleep } from '../../sleep'

// ── Streaming primitives ─────────────────────────────────────────────

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

// ── Streaming + metadata ─────────────────────────────────────────────

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

// ── Multiplexed streaming ────────────────────────────────────────────

const onReturnTwoGenerators = async () => {
  cleanupState.twoGeneratorsClosed = ''
  const context = getContext()
  context.onClose(() => {
    cleanupState.twoGeneratorsClosed = String(Date.now())
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
  cleanupState.streamAndGeneratorClosed = ''
  const context = getContext()
  context.onClose(() => {
    cleanupState.streamAndGeneratorClosed = String(Date.now())
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
  cleanupState.multiplePromisesClosed = ''
  const context = getContext()
  context.onClose(() => {
    cleanupState.multiplePromisesClosed = String(Date.now())
  })
  const fast = Promise.resolve('quick')
  const slow = new Promise<string>((resolve) => setTimeout(() => resolve('delayed'), 1000))
  return { fast, slow, label: 'promises' }
}

// ── Deadlock test ────────────────────────────────────────────────────
// Stream is large enough (2 MB) to exceed the client-side demuxer buffer
// (1 MB) so that the demuxer stalls and cannot deliver further frames
// until the client starts consuming the stream.

const onReturnDeadlockStream = async () => {
  const encoder = new TextEncoder()
  const CHUNK = encoder.encode('x'.repeat(64 * 1024)) // 64 KB
  const CHUNKS = 32 // 32 x 64 KB = 2 MB
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

// ── Per-tag cancel test ──────────────────────────────────────────────

const onReturnMixedEndless = async () => {
  cleanupState.mixedEndless = 'running'
  cleanupState.mixedEndlessClosed = ''
  const context = getContext()
  context.onClose(() => {
    cleanupState.mixedEndless = 'cleaned-up'
    cleanupState.mixedEndlessClosed = String(Date.now())
  })
  async function* gen(): AsyncGenerator<string> {
    let i = 0
    while (true) {
      await sleep(100)
      yield `g-${i++}`
    }
  }
  const slow = new Promise<string>((resolve) => setTimeout(() => resolve('done'), 1000))
  return { gen: gen(), slow }
}

// ── Asymmetric completion test ───────────────────────────────────────
// fast finishes in 1 yield; slow takes 3 yields x 200 ms each.
// Tests that the per-index done frame correctly closes the fast consumer
// while the slow consumer is still streaming.

const onReturnAsymmetricGenerators = async () => {
  async function* fast(): AsyncGenerator<string> {
    yield 'fast-done'
  }
  async function* slow(): AsyncGenerator<string> {
    for (const v of ['slow-0', 'slow-1', 'slow-2']) {
      await sleep(200)
      yield v
    }
  }
  return { fast: fast(), slow: slow() }
}

// ── Mid-stream error cases ───────────────────────────────────────────

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

const onAbortOneOfManyStreamingValues = async () => {
  async function* aborting(): AsyncGenerator<string> {
    yield 'abort-0'
    await sleep(80)
    throw Abort({ reason: 'stream-abort', code: 101 })
  }

  async function* other(): AsyncGenerator<string> {
    let i = 0
    while (true) {
      await sleep(20)
      yield `other-${i++}`
    }
  }

  const encoder = new TextEncoder()
  let streamIndex = 0
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      await sleep(20)
      controller.enqueue(encoder.encode(`stream-${streamIndex++}`))
    },
  })

  const promise = new Promise<string>((resolve) => setTimeout(() => resolve('promise-done'), 200))

  return { aborting: aborting(), other: other(), stream, promise }
}

const onChannelAbortAbortsSiblingStreamingValues = async () => {
  let markFirstStarted!: () => void
  let markSecondStarted!: () => void
  let markStreamStarted!: () => void
  const firstStarted = new Promise<void>((resolve) => {
    markFirstStarted = resolve
  })
  const secondStarted = new Promise<void>((resolve) => {
    markSecondStarted = resolve
  })
  const streamStarted = new Promise<void>((resolve) => {
    markStreamStarted = resolve
  })
  const ch = channel<(msg: string) => void, never>()
  ch.listen(async () => {
    await Promise.all([firstStarted, secondStarted, streamStarted])
    throw Abort({ reason: 'channel-listener-abort', code: 7 })
  })

  async function* first(): AsyncGenerator<string> {
    let i = 0
    while (true) {
      await sleep(30)
      yield `first-${i}`
      if (i === 0) markFirstStarted()
      i++
    }
  }

  async function* second(): AsyncGenerator<string> {
    let i = 0
    while (true) {
      await sleep(45)
      yield `second-${i}`
      if (i === 0) markSecondStarted()
      i++
    }
  }

  const encoder = new TextEncoder()
  let i = 0
  let firstChunkSent = false
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      await sleep(35)
      if (firstChunkSent) markStreamStarted()
      controller.enqueue(encoder.encode(`chunk-${i}`))
      firstChunkSent = true
      i++
    },
  })

  return { first: first(), second: second(), stream, channel: ch.client }
}

// ── Upload progress via streaming ────────────────────────────────────

async function* onUploadWithProgress(
  file: File,
): AsyncGenerator<{ bytesRead: number; totalSize: number; timestamp: number; duration?: number }> {
  const totalSize = file.size
  let bytesRead = 0
  const t0 = Date.now()
  yield { bytesRead, totalSize, timestamp: t0 }

  await sleep(3000)
  const reader = file.stream().getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    bytesRead += value.byteLength
  }
  const t1 = Date.now()
  yield { bytesRead, totalSize, timestamp: t1, duration: t1 - t0 }
}
