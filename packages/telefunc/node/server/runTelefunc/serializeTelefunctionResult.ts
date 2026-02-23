export { serializeTelefunctionResult }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamReplacer } from '../../../shared/wire-protocol/replacer-response.js'

type StreamSegment =
  | { type: 'stream'; value: ReadableStream<Uint8Array> }
  | { type: 'generator'; value: AsyncGenerator<unknown> }

type SerializeResult = { type: 'text'; body: string } | { type: 'streaming'; body: ReadableStream<Uint8Array> }

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

  const segments: StreamSegment[] = []
  const replacer = createStreamReplacer({
    onStream: (stream) => {
      segments.push({ type: 'stream', value: stream })
    },
    onGenerator: (gen) => {
      segments.push({ type: 'generator', value: gen })
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

  assertUsage(
    segments.length <= 1,
    `Telefunction ${runContext.telefunctionName}() (${runContext.telefuncFilePath}) returns multiple streaming values. Only one ReadableStream or AsyncGenerator per return value is supported.`,
  )

  if (segments.length === 0) {
    return { type: 'text', body: httpResponseBody }
  }

  // Build binary streaming response: [u32 metadata len][metadata JSON][chunk frames...][zero terminator]
  return { type: 'streaming', body: buildStreamingResponseBody(httpResponseBody, segments[0]!) }
}

// ===== Streaming response framing =====

/** Build a ReadableStream that frames metadata + a single streaming segment. */
function buildStreamingResponseBody(metadataSerialized: string, segment: StreamSegment): ReadableStream<Uint8Array> {
  const gen = generateResponseBody(metadataSerialized, segment)
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

async function* generateResponseBody(metadataSerialized: string, segment: StreamSegment): AsyncGenerator<Uint8Array> {
  // Metadata header
  const metadataBytes = new TextEncoder().encode(metadataSerialized)
  const lengthPrefix = new Uint8Array(4)
  new DataView(lengthPrefix.buffer).setUint32(0, metadataBytes.length, false)
  yield lengthPrefix
  yield metadataBytes

  // Single segment as length-prefixed chunks terminated by a zero-length frame
  if (segment.type === 'stream') {
    const reader = segment.value.getReader()
    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        assertUsage(
          value instanceof Uint8Array,
          'ReadableStream returned by a telefunction must yield Uint8Array chunks.',
        )
        const chunkLen = new Uint8Array(4)
        new DataView(chunkLen.buffer).setUint32(0, value.byteLength, false)
        yield chunkLen
        yield value
      }
    } finally {
      reader.releaseLock()
    }
  } else {
    // Generator: serialize each yielded value
    const encoder = new TextEncoder()
    for await (const value of segment.value) {
      const serialized = encoder.encode(stringify(value))
      const chunkLen = new Uint8Array(4)
      new DataView(chunkLen.buffer).setUint32(0, serialized.byteLength, false)
      yield chunkLen
      yield serialized
    }
  }
  // Zero-length terminator
  yield new Uint8Array(4)
}
