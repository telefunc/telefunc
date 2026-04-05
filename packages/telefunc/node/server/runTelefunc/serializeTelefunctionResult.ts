export { serializeTelefunctionResult }
export type { TelefuncId }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import { ServerChannel } from '../../../wire-protocol/server/channel.js'
import { buildInlineResponseBody } from '../../../wire-protocol/server/response/StreamingResponseBody.js'
import { pumpProducerToChannel } from '../../../wire-protocol/server/response/ChannelResponseBody.js'
import { STREAM_TRANSPORT, type StreamTransport } from '../../../wire-protocol/constants.js'
import { textEncoder } from '../../../wire-protocol/frame.js'
import { uint8ArrayToBase64url } from '../../../wire-protocol/base64url.js'
import type { StreamingValueServer } from '../../../wire-protocol/types.js'
import { type RequestContext } from '../requestContext.js'
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
  function registerChannel(channel: ServerChannel<any, any>) {
    channel._registerChannel()
    channel._setResponseAbort(requestContext.responseAbort.abort)
    channel.onClose(requestContext.trackPending())
  }
  const replacer = createStreamingReplacer(
    {
      createChannel<TOut, TIn>(opts?: { ack?: boolean }) {
        const channel = new ServerChannel<TOut, TIn>(opts)
        registerChannel(channel)
        return channel
      },
      registerChannel,
      sendStream(createProducer) {
        if (useChannelPump) {
          const channelId = pumpProducerToChannel(createProducer, runContext, requestContext.trackPending())
          return {
            metadata: { channelId },
            // Pump self-manages lifecycle: close in finally, abort via responseAbort.errorPromise race.
            close() {},
            abort() {},
          }
        }
        const index = nextStreamingIndex++
        streamingValues.push({ createProducer, index })
        return {
          metadata: { __index: index },
          // Inline streaming lifecycle is managed by the HTTP body stream.
          close() {},
          abort() {},
        }
      },
    },
    function onReplaced({ abort }) {
      requestContext.responseAbort.onAbort(abort)
    },
  )

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

  if (useChannelPump || streamingValues.length === 0) {
    requestContext.markComplete()
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

  const onStreamComplete = requestContext.trackPending()
  requestContext.markComplete()

  return {
    type: 'streaming',
    body: buildInlineResponseBody({
      metadataSerialized: httpResponseBody,
      streamingValues,
      telefuncId,
      abortSignal: requestContext.abortSignal,
      responseAbort: requestContext.responseAbort,
      onComplete: onStreamComplete,
      encodeFrame,
    }),
    streamTransport: runContext.streamTransport,
  }
}
