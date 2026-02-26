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

// ── Error cases (e2e tests) ─────────────────────────────────────────

const onReturnTwoGenerators = async () => {
  async function* gen1(): AsyncGenerator<number> {
    yield 1
  }
  async function* gen2(): AsyncGenerator<number> {
    yield 2
  }
  return { first: gen1(), second: gen2() }
}

const onReturnStreamAndGenerator = async () => {
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
