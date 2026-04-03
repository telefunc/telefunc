export { serializeTelefunctionResult }
export type { TelefuncId }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import type { ServerReplacerContext } from '../../../wire-protocol/types.js'
import { buildInlineResponseBody } from '../../../wire-protocol/server/response/StreamingResponseBody.js'
import { pumpProducerToChannel } from '../../../wire-protocol/server/response/ChannelResponseBody.js'
import { STREAM_TRANSPORT, type StreamTransport } from '../../../wire-protocol/constants.js'
import { textEncoder } from '../../../wire-protocol/frame.js'
import { uint8ArrayToBase64url } from '../../../wire-protocol/base64url.js'
import type { StreamingValueServer } from '../../../wire-protocol/types.js'
import { restoreRequestContext, type RequestContext } from '../requestContext.js'
import type { Telefunc } from '../getContext.js'

type TelefuncId = {
  telefunctionName: string
  telefuncFilePath: string
}

type SerializeResult =
  | { type: 'text'; body: string }
  | {
      type: 'streaming'
      body: ReadableStream<Uint8Array>
      streamTransport: StreamTransport
    }

function serializeTelefunctionResult(runContext: {
  telefunctionReturn: unknown
  telefunctionName: string
  telefuncFilePath: string
  telefunctionAborted: boolean
  providedContext: Telefunc.Context | null
  requestContext: RequestContext
  abortSignal: AbortSignal
  streamTransport: StreamTransport
}): SerializeResult {
  const { requestContext } = runContext

  const bodyValue = runContext.telefunctionAborted
    ? { ret: runContext.telefunctionReturn, abort: true }
    : { ret: runContext.telefunctionReturn }

  const useChannelPump = runContext.streamTransport === STREAM_TRANSPORT.CHANNEL
  const streamingValues: StreamingValueServer[] = []
  let nextStreamingIndex = 0
  let pendingCount = 0
  const context: ServerReplacerContext = {
    useChannelPump,
    registerChannel(channel) {
      channel._setResponseAbort(requestContext.responseAbort.abort)
      requestContext.responseAbort.onAbort((abortError) => channel.abort(abortError.abortValue))
      pendingCount++
      channel.onClose(() => {
        if (--pendingCount === 0) requestContext.markComplete()
      })
    },
    registerStreamingValue(createProducer) {
      assert(!useChannelPump, 'registerStreamingValue called in channel transport mode')
      const index = nextStreamingIndex++
      streamingValues.push({ createProducer, index })
      return index
    },
    pumpToChannel(createProducer) {
      assert(useChannelPump, 'pumpToChannel called in inline transport mode')
      pendingCount++
      return pumpProducerToChannel(createProducer, runContext, () => {
        if (--pendingCount === 0) requestContext.markComplete()
      })
    },
  }
  const replacer = createStreamingReplacer(context)

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

  // markComplete fires when all pending channels and pumps are done,
  // or immediately if there are none.
  if (useChannelPump || streamingValues.length === 0) {
    if (pendingCount === 0) requestContext.markComplete()
    return { type: 'text', body: httpResponseBody }
  }

  const telefuncId: TelefuncId = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
  }

  const encodeFrame =
    runContext.streamTransport === STREAM_TRANSPORT.SSE_INLINE
      ? (frame: Uint8Array<ArrayBuffer>) => textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`)
      : (frame: Uint8Array<ArrayBuffer>) => frame

  return {
    type: 'streaming',
    body: buildInlineResponseBody({
      metadataSerialized: httpResponseBody,
      streamingValues,
      telefuncId,
      requestContext,
      encodeFrame,
    }),
    streamTransport: runContext.streamTransport,
  }
}
