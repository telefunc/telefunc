export {
  onReturnReadableStream,
  onReturnAsyncGenerator,
  onReturnGeneratorWithMeta,
  onReturnEmptyGenerator,
  onReturnPlainValue,
  onReturnDelayedStream,
  onReturnDelayedGenerator,
  onReturnDelayedGeneratorWithMeta,
  onReturnStreamWithMeta,
  onReturnTwoGenerators,
  onReturnStreamAndGenerator,
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/** Return a ReadableStream<Uint8Array> directly */
const onReturnReadableStream = async (): Promise<ReadableStream<Uint8Array>> => {
  const encoder = new TextEncoder()
  const chunks = ['hello', ' ', 'stream']
  let i = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i >= chunks.length) {
        controller.close()
        return
      }
      controller.enqueue(encoder.encode(chunks[i]!))
      i++
    },
  })
}

/** Return an AsyncGenerator<number> */
async function* onReturnAsyncGenerator(): AsyncGenerator<number> {
  for (const n of [1, 2, 3, 4, 5]) {
    yield n
  }
}

/** Return a single generator alongside plain metadata in an object */
const onReturnGeneratorWithMeta = async () => {
  async function* messages(): AsyncGenerator<string> {
    yield 'hello'
    yield 'world'
  }
  return {
    stream: messages(),
    timestamp: 1234567890,
    tags: ['a', 'b'],
  }
}

/** Return a generator that yields nothing */
async function* onReturnEmptyGenerator(): AsyncGenerator<number> {
  // yields nothing
}

/** Return a plain value (no streaming) — should still work normally */
const onReturnPlainValue = async (x: number) => {
  return { doubled: x * 2 }
}

/** Return a ReadableStream that emits chunks with delays */
const onReturnDelayedStream = async (): Promise<ReadableStream<Uint8Array>> => {
  const encoder = new TextEncoder()
  const chunks = ['chunk1', 'chunk2', 'chunk3']
  let i = 0
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (i >= chunks.length) {
        controller.close()
        return
      }
      await sleep(200)
      controller.enqueue(encoder.encode(chunks[i]!))
      i++
    },
  })
}

/** Return an AsyncGenerator that yields values with delays */
async function* onReturnDelayedGenerator(): AsyncGenerator<string> {
  const words = ['alpha', 'beta', 'gamma', 'delta']
  for (const word of words) {
    await sleep(200)
    yield word
  }
}

/** Return delayed generator alongside metadata */
const onReturnDelayedGeneratorWithMeta = async () => {
  async function* countdown(): AsyncGenerator<number> {
    for (let i = 3; i >= 0; i--) {
      await sleep(200)
      yield i
    }
  }
  return {
    stream: countdown(),
    label: 'countdown',
  }
}

/** Return a ReadableStream inside an object alongside plain metadata */
const onReturnStreamWithMeta = async () => {
  const encoder = new TextEncoder()
  const chunks = ['foo', 'bar', 'baz']
  let i = 0
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i >= chunks.length) {
        controller.close()
        return
      }
      controller.enqueue(encoder.encode(chunks[i]!))
      i++
    },
  })
  return {
    stream,
    count: 3,
  }
}

/** Return two generators — should fail with assertUsage */
const onReturnTwoGenerators = async () => {
  async function* gen1(): AsyncGenerator<number> {
    yield 1
  }
  async function* gen2(): AsyncGenerator<number> {
    yield 2
  }
  return {
    first: gen1(),
    second: gen2(),
  }
}

/** Return a stream and a generator — should fail with assertUsage */
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
  return {
    stream,
    gen: gen(),
  }
}
