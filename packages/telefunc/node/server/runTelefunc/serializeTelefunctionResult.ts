export { serializeTelefunctionResult }
export type { TelefuncId }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import {
  buildStreamingResponseBody,
  buildSSEResponseBody,
} from '../../../wire-protocol/server/response/StreamingResponseBody.js'
import { buildChannelResponseBody } from '../../../wire-protocol/server/response/ChannelResponseBody.js'
import { ServerChannel } from '../../../wire-protocol/server/channel.js'
import { injectFrameChannel } from '../../../wire-protocol/frame-channel.js'
import { STREAM_TRANSPORT, type ChannelTransport, type StreamTransport } from '../../../wire-protocol/constants.js'
import type { RequestContext } from '../requestContext.js'
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
  channelTransport: ChannelTransport
}): SerializeResult {
  const { requestContext } = runContext

  const bodyValue: TelefuncResponseBody = runContext.telefunctionAborted
    ? { ret: runContext.telefunctionReturn, abort: true }
    : { ret: runContext.telefunctionReturn }

  const { replacer, streamingValues } = createStreamingReplacer(runContext.channelTransport, (channel) => {
    channel._setResponseAbort(requestContext.responseAbort.abort)
    requestContext.responseAbort.onAbort((abortError) => channel.abort(abortError.abortValue))
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
    requestContext.markComplete()
    return { type: 'text', body: httpResponseBody }
  }

  const telefuncId: TelefuncId = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
  }

  if (runContext.streamTransport === STREAM_TRANSPORT.CHANNEL) {
    const serverChannel = new ServerChannel<never, never>({ channelTransport: runContext.channelTransport })
    serverChannel._registerChannel()
    serverChannel.onClose(() => requestContext.markComplete())
    httpResponseBody = injectFrameChannel(httpResponseBody, {
      channelId: serverChannel.id,
      channelTransport: runContext.channelTransport,
    })
    buildChannelResponseBody(streamingValues, telefuncId, serverChannel, runContext)
    return { type: 'text', body: httpResponseBody }
  }

  let buildFn: typeof buildStreamingResponseBody | typeof buildSSEResponseBody
  switch (runContext.streamTransport) {
    case STREAM_TRANSPORT.BINARY_INLINE:
      buildFn = buildStreamingResponseBody
      break
    case STREAM_TRANSPORT.SSE_INLINE:
      buildFn = buildSSEResponseBody
      break
    default:
      assert(false)
  }

  return {
    type: 'streaming',
    body: buildFn(
      httpResponseBody,
      streamingValues,
      telefuncId,
      requestContext.markComplete,
      runContext.abortSignal,
      requestContext.responseAbort,
    ),
    streamTransport: runContext.streamTransport,
  }
}

// ===== Response body (JSON path + streaming metadata) =====
/** Wire format of the JSON response body / streaming metadata. */
type TelefuncResponseBody = TelefuncResponseBodySuccess | TelefuncResponseBodyAbort
/** Successful telefunction return value. */
type TelefuncResponseBodySuccess = {
  ret: unknown
}
/** Aborted telefunction return value. */
type TelefuncResponseBodyAbort = {
  ret: unknown
  abort: true
}
