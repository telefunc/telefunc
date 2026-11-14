export { serializeTelefunctionResult }
export type { TelefuncId }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/values/server/response/registry.js'
import { ServerChannel } from '../../../wire-protocol/channel/server/ServerChannel.js'
import { getChannelMux } from '../../../wire-protocol/channel/server/ChannelMux.js'
import {
  buildShieldValidators,
  type ShieldValidators,
  type ValueShields,
  type ShieldLogConfig,
  type ShieldValidatorCtx,
} from '../shield.js'
import { isObjectOrFunction } from '../../../utils/isObjectOrFunction.js'
import { buildInlineResponseBody } from '../../../wire-protocol/values/server/response/StreamingResponseBody.js'
import { pumpProducerToChannel } from '../../../wire-protocol/values/server/response/ChannelResponseBody.js'
import { STREAM_TRANSPORT, type StreamTransport } from '../../../wire-protocol/constants.js'
import { textEncoder } from '../../../wire-protocol/wire/envelope.js'
import { uint8ArrayToBase64url } from '../../../wire-protocol/wire/base64url.js'
import type { StreamingProducer, StreamingValueServer } from '../../../wire-protocol/values/types.js'
import { type RequestContext } from '../context/requestContext.js'
import type { AbortError } from '../Abort.js'
import type { Context } from '../context/context.js'
import type { Readable } from 'node:stream'
import { getServerExtensionTypes } from '../serverConfig.js'

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
      body: Readable | ReadableStream<Uint8Array<ArrayBuffer>>
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
  useNodeStream: boolean
  serverConfig: {
    log: { shieldErrors: ShieldLogConfig }
  }
  valueShields?: ValueShields
}): SerializeResult {
  const { requestContext } = runContext
  const { responseTypes } = getServerExtensionTypes()

  const bodyValue = runContext.telefunctionAborted
    ? { ret: runContext.telefunctionReturn, abort: true }
    : { ret: runContext.telefunctionReturn }

  const useChannelPump = runContext.streamTransport === STREAM_TRANSPORT.CHANNEL
  const streamingValues: StreamingValueServer[] = []
  let nextStreamingIndex = 0
  /** What the response carries is registered once the response is sure to go; ending the response before that, as a
   *  failed serialization does, ends what only it would carry; a channel an earlier response carried stays open. */
  const registrations: (() => void)[] = []
  let goes = false
  const { valueShields } = runContext
  const shieldCtx = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
    shieldErrors: runContext.serverConfig.log.shieldErrors,
  }
  const mux = getChannelMux()
  function registerChannel(channel: ServerChannel<any, any>) {
    registrations.push(() => {
      channel._setResponseAbort(requestContext.responseAbort.abort)
      channel._validators = makeValidators(channel, valueShields, shieldCtx)
      mux.registerChannel(channel as ServerChannel<unknown, unknown>)
      channel.onClose(requestContext.trackPending())
    })
  }
  function sendStream(createProducer: () => StreamingProducer) {
    // Once the response goes, the pump or the HTTP body ends the stream.
    const stream = {
      close() {},
      abort(abortError: AbortError) {
        if (!goes) createProducer().cancel(abortError)
      },
    }
    if (useChannelPump) {
      const channel = new ServerChannel<never, never>()
      registrations.push(() =>
        pumpProducerToChannel(channel, createProducer, runContext, requestContext.trackPending()),
      )
      return { metadata: { channelId: channel.id }, ...stream }
    }
    const index = nextStreamingIndex++
    streamingValues.push({ createProducer, index })
    return { metadata: { __index: index }, ...stream }
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
    function onReplaced({ value, abort }) {
      // Registered already (it has a replay buffer), so its page may hold it: it ends with this response only once that goes.
      const carriedBefore = ServerChannel.isServerChannel(value) && value._replayBuffer !== null
      if (carriedBefore) registrations.push(() => requestContext.responseAbort.onAbort(abort))
      else requestContext.responseAbort.onAbort(abort)
    },
    responseTypes,
  )

  let httpResponseBody: string
  try {
    httpResponseBody = stringify(bodyValue, { forbidReactElements: true, replacer })
  } catch (err: unknown) {
    requestContext.responseAbort.abort()
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

  goes = true
  for (const register of registrations) register()

  if (useChannelPump || streamingValues.length === 0) return { type: 'text', body: httpResponseBody }

  const telefuncId: TelefuncId = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
  }

  const encodeFrame =
    runContext.streamTransport === STREAM_TRANSPORT.SSE_INLINE
      ? (frame: Uint8Array<ArrayBuffer>) => textEncoder.encode(`data: ${uint8ArrayToBase64url(frame)}\n\n`)
      : (frame: Uint8Array<ArrayBuffer>) => frame

  const onStreamComplete = requestContext.trackPending()

  return {
    type: 'streaming',
    body: buildInlineResponseBody({
      metadataSerialized: httpResponseBody,
      streamingValues,
      telefuncId,
      context: runContext.context,
      abortSignal: requestContext.abortSignal,
      responseAbort: requestContext.responseAbort,
      onComplete: onStreamComplete,
      encodeFrame,
      useNodeStream: runContext.useNodeStream,
    }),
    streamTransport: runContext.streamTransport,
  }
}
