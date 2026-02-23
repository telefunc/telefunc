export { serializeTelefunctionResult }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamReplacer } from '../../../shared/wire-protocol/replacer-response.js'

type StreamingValue =
  | { type: 'stream'; value: ReadableStream<Uint8Array> }
  | { type: 'generator'; value: AsyncGenerator<unknown> }

type SerializeResult = { type: 'text'; body: string } | { type: 'streaming'; body: ReadableStream<Uint8Array> }
const textEncoder = new TextEncoder()

function serializeTelefunctionResult(runContext: {
  telefunctionReturn: unknown
  telefunctionName: string
  telefuncFilePath: string
  telefunctionAborted: boolean
}): SerializeResult {
  const bodyValue: Record<string, unknown> = {
    ret: runContext.telefunctionReturn,
  }
  if (runContext.telefunctionAborted) {
    bodyValue.abort = true
  }

  const streamingValues: StreamingValue[] = []
  const replacer = createStreamReplacer({
    onStream: (stream) => {
      streamingValues.push({ type: 'stream', value: stream })
    },
    onGenerator: (gen) => {
      streamingValues.push({ type: 'generator', value: gen })
    },
  })

  let httpResponseBody: string
  try {
    httpResponseBody = stringify(bodyValue, { forbidReactElements: true, replacer })
  } catch (err: unknown) {
    assert(hasProp(err, 'message', 'string'))
    assertUsage(
      false,
      [
        `Cannot serialize value returned by telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}).`,
        'Make sure that telefunctions always return a serializable value.',
        `Serialization error: ${lowercaseFirstLetter(err.message)}`,
      ].join(' '),
    )
  }

  if (streamingValues.length === 0) {
    return { type: 'text', body: httpResponseBody }
  }

  assertUsage(
    streamingValues.length <= 1,
    `Telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}) returns multiple streaming values. Only one ReadableStream or AsyncGenerator per return value is supported.`,
  )
  // Build binary streaming response: [u32 metadata len][metadata JSON][chunk frames...][zero terminator]
  return { type: 'streaming', body: buildStreamingResponseBody(httpResponseBody, streamingValues[0]!) }
}

// ===== Streaming response framing =====

/** Build a ReadableStream that frames metadata + a single streaming value. */
function buildStreamingResponseBody(
  metadataSerialized: string,
  streamingValue: StreamingValue,
): ReadableStream<Uint8Array> {
  const gen = generateResponseBody(metadataSerialized, streamingValue)
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await gen.next()
        if (done) controller.close()
        else controller.enqueue(value)
      } catch (err) {
        controller.error(err)
      }
    },
    async cancel() {
      await gen.return(undefined)
    },
  })
}

async function* generateResponseBody(
  metadataSerialized: string,
  streamingValue: StreamingValue,
): AsyncGenerator<Uint8Array> {
  // Metadata header
  const metadataBytes = textEncoder.encode(metadataSerialized)
  yield encodeU32(metadataBytes.length)
  yield metadataBytes

  // Chunks as length-prefixed frames terminated by a zero-length frame
  if (streamingValue.type === 'stream') {
    const reader = streamingValue.value.getReader()
    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        assertUsage(
          value instanceof Uint8Array,
          'ReadableStream returned by a telefunction must yield Uint8Array chunks.',
        )
        yield encodeU32(value.byteLength)
        yield value
      }
    } finally {
      await reader.cancel()
    }
  } else {
    for await (const value of streamingValue.value) {
      const serialized = textEncoder.encode(stringify(value))
      yield encodeU32(serialized.byteLength)
      yield serialized
    }
  }

  // Zero-length terminator
  yield encodeU32(0)
}

function encodeU32(n: number): Uint8Array {
  const buf = new Uint8Array(4)
  new DataView(buf.buffer).setUint32(0, n, false)
  return buf
}
