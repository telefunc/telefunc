export { serializeTelefunctionResult }

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
import { createChannel, ServerChannel } from '../channel.js'
import { injectFrameChannel } from '../../../wire-protocol/frame-channel.js'
import { TRANSPORT, type Transport } from '../../../wire-protocol/constants.js'
import type { TelefuncIdentifier, TelefuncResponseBody } from '../../../shared/constants.js'
import type { RequestContext } from '../requestContext.js'
import type { Telefunc } from '../getContext.js'

type SerializeResult =
  | { type: 'text'; body: string }
  | { type: 'streaming'; body: ReadableStream<Uint8Array>; transport: typeof TRANSPORT.STREAM | typeof TRANSPORT.SSE }

function serializeTelefunctionResult(runContext: {
  telefunctionReturn: unknown
  telefunctionName: string
  telefuncFilePath: string
  telefunctionAborted: boolean
  providedContext: Telefunc.Context | null
  requestContext: RequestContext
  abortSignal: AbortSignal
  transport: Transport
}): SerializeResult {
  const bodyValue: TelefuncResponseBody = runContext.telefunctionAborted
    ? { ret: runContext.telefunctionReturn, abort: true }
    : { ret: runContext.telefunctionReturn }

  const { replacer, streamingValues } = createStreamingReplacer()

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

  const { requestContext } = runContext

  if (streamingValues.length === 0) {
    requestContext.markComplete()
    return { type: 'text', body: httpResponseBody }
  }

  const telefuncId: TelefuncIdentifier = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
  }

  // WS transport: create a frame channel, inject it into the response body,
  // and start pumping data frames over the WebSocket in the background.
  if (runContext.transport === TRANSPORT.WS) {
    const channel = createChannel<never, { p: 0 | 1 }>()
    const serverChannel = channel as unknown as ServerChannel<never, { p: 0 | 1 }>
    serverChannel.onAbort(() => requestContext.markAborted())
    serverChannel.onClose(() => requestContext.markComplete())
    httpResponseBody = injectFrameChannel(httpResponseBody, serverChannel.id)
    buildChannelResponseBody(streamingValues, telefuncId, serverChannel, runContext)
    return { type: 'text', body: httpResponseBody }
  }

  const buildFn = runContext.transport === TRANSPORT.SSE ? buildSSEResponseBody : buildStreamingResponseBody

  return {
    type: 'streaming',
    body: buildFn(httpResponseBody, streamingValues, telefuncId, requestContext.markComplete, runContext.abortSignal),
    transport: runContext.transport === TRANSPORT.SSE ? TRANSPORT.SSE : TRANSPORT.STREAM,
  }
}
