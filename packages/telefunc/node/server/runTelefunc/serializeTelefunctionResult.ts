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
import { ServerChannel } from '../../../wire-protocol/server/channel.js'
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

  const { replacer, streamingValues, returnedChannels } = createStreamingReplacer()

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

  for (const channel of returnedChannels) {
    channel._setResponseAbort(requestContext.responseAbort.abort)
    requestContext.responseAbort.onAbort((abortError) => channel.abort(abortError.abortValue))
  }

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
    const serverChannel = new ServerChannel<never, never>()
    serverChannel._registerChannel()
    serverChannel.onClose(() => requestContext.markComplete())
    httpResponseBody = injectFrameChannel(httpResponseBody, { channelId: serverChannel.id })
    buildChannelResponseBody(streamingValues, telefuncId, serverChannel, runContext)
    return { type: 'text', body: httpResponseBody }
  }

  const buildFn = runContext.transport === TRANSPORT.SSE ? buildSSEResponseBody : buildStreamingResponseBody

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
    transport: runContext.transport === TRANSPORT.SSE ? TRANSPORT.SSE : TRANSPORT.STREAM,
  }
}
