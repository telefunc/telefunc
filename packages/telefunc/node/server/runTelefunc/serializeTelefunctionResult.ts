export { serializeTelefunctionResult }
export type { TelefuncId }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import { ServerChannel } from '../../../wire-protocol/server/channel.js'
import { getChannelMux } from '../../../wire-protocol/server/substrate.js'
import {
  buildShieldValidators,
  type ShieldValidators,
  type ValueShields,
  type ShieldLogConfig,
  type ShieldValidatorCtx,
} from '../shield.js'
import { isObjectOrFunction } from '../../../utils/isObjectOrFunction.js'
import { buildInlineResponseBody } from '../../../wire-protocol/server/response/StreamingResponseBody.js'
import { pumpProducerToChannel } from '../../../wire-protocol/server/response/ChannelResponseBody.js'
import { STREAM_TRANSPORT, type StreamTransport } from '../../../wire-protocol/constants.js'
import { textEncoder } from '../../../wire-protocol/frame.js'
import { uint8ArrayToBase64url } from '../../../wire-protocol/base64url.js'
import type { StreamingProducer, StreamingValueServer } from '../../../wire-protocol/types.js'
import { type RequestContext } from '../context/requestContext.js'
import type { Context } from '../context/context.js'
import type { ReplacerType, TypeContract, ServerReplacerContext } from '../../../wire-protocol/types.js'

/** Look up the shields for a revived value and build its auto-logging validator map.
 *  Empty map when the value isn't tracked in `valueShields` or has no registered shields. */
function makeValidators(
  value: unknown,
  valueShields: ValueShields | undefined,
  ctx: ShieldValidatorCtx,
): ShieldValidators {
  if (!valueShields || !isObjectOrFunction(value)) return new Map()
  const shields = valueShields.get(value)
  if (!shields) return new Map()
  return buildShieldValidators(shields, ctx)
}

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
  context: Context
  requestContext: RequestContext
  abortSignal: AbortSignal
  streamTransport: StreamTransport
  serverConfig: {
    extensionResponseTypes: ReplacerType<TypeContract, ServerReplacerContext>[]
    log: { shieldErrors: ShieldLogConfig }
  }
  valueShields?: ValueShields
}): SerializeResult {
  const { requestContext } = runContext
  const { extensionResponseTypes } = runContext.serverConfig

  const bodyValue = runContext.telefunctionAborted
    ? { ret: runContext.telefunctionReturn, abort: true }
    : { ret: runContext.telefunctionReturn }

  const useChannelPump = runContext.streamTransport === STREAM_TRANSPORT.CHANNEL
  const streamingValues: StreamingValueServer[] = []
  let nextStreamingIndex = 0
  const { valueShields } = runContext
  const shieldCtx = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
    shieldErrors: runContext.serverConfig.log.shieldErrors,
  }
  const mux = getChannelMux()
  function registerChannel(channel: ServerChannel<any, any>) {
    mux.registerChannel(channel as ServerChannel<unknown, unknown>)
    channel._setResponseAbort(requestContext.responseAbort.abort)
    channel.onClose(requestContext.trackPending())
    channel._validators = makeValidators(channel, valueShields, shieldCtx)
  }
  function sendStream(createProducer: () => StreamingProducer) {
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
  }
  function createChannel<ClientToServer, ServerToClient>(opts?: { ack?: boolean }) {
    const channel = new ServerChannel<ClientToServer, ServerToClient>(opts)
    registerChannel(channel)
    return channel
  }
  const replacer = createStreamingReplacer(
    function getContext(value: unknown) {
      return {
        createChannel,
        registerChannel,
        sendStream,
        validators: makeValidators(value, valueShields, shieldCtx),
      }
    },
    function onReplaced({ abort }) {
      requestContext.responseAbort.onAbort(abort)
    },
    extensionResponseTypes,
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
