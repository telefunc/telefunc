export { serializeTelefunctionResult }
export type { TelefuncId }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/values/server/response/registry.js'
import { ServerChannel } from '../../../wire-protocol/channel/server/ServerChannel.js'
import { ServerBroadcastEnd } from '../../../wire-protocol/channel/server/ServerBroadcastEnd.js'
import { getSessionToken } from '../../../wire-protocol/wire/requestKind.js'
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
import type {
  ServerReplacerContext,
  StreamingProducer,
  StreamingValueServer,
} from '../../../wire-protocol/values/types.js'
import { type RequestContext } from '../context/requestContext.js'
import { carryChannel } from './carryChannel.js'
import type { AbortError } from '../Abort.js'
import type { Context } from '../context/context.js'
import type { Readable } from 'node:stream'
import { getAdapterMaxFrameBytes, getServerExtensionTypes } from '../serverConfig.js'

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
  const producers: StreamingValueServer[] = []
  let nextStreamingIndex = 0
  /** What the response carries is registered once the response is sure to go; ending the response before that, as a
   *  failed serialization or a page that left does, ends what only it would carry; a channel an earlier response carried
   *  stays open. */
  const registrations: (() => void)[] = []
  let goes = false
  const { valueShields } = runContext
  const shieldCtx = {
    telefunctionName: runContext.telefunctionName,
    telefuncFilePath: runContext.telefuncFilePath,
    shieldErrors: runContext.serverConfig.log.shieldErrors,
  }
  function registerChannel(channel: ServerChannel<any, any>) {
    registrations.push(() => {
      channel._validators = makeValidators(channel, valueShields, shieldCtx)
      carryChannel(channel, requestContext)
    })
  }
  function sendStream(createProducer: () => StreamingProducer) {
    // Taken as it's serialized: a stream that can't be read fails the serialization, which ends what the response carries.
    const producer = createProducer()
    const stream = {
      close() {},
      abort(abortError: AbortError) {
        if (!goes) producer.cancel(abortError)
      },
    }
    if (useChannelPump) {
      const channel = new ServerChannel<never, never>()
      registrations.push(() => pumpProducerToChannel(channel, producer, runContext, requestContext.trackPending()))
      return { metadata: { channelId: channel.id }, ...stream }
    }
    const index = nextStreamingIndex++
    producers.push({ producer, index })
    return { metadata: { __index: index }, ...stream }
  }
  function createChannel<ClientToServer, ServerToClient>(opts?: { ack?: boolean }) {
    const channel = new ServerChannel<ClientToServer, ServerToClient>(opts)
    registerChannel(channel)
    return channel
  }
  function createBroadcastChannel() {
    const end = new ServerBroadcastEnd()
    registerChannel(end)
    return end
  }
  const responseStates = new Map<symbol, unknown>()
  function responseState<T>(key: symbol, init: () => T): T {
    if (!responseStates.has(key)) responseStates.set(key, init())
    return responseStates.get(key) as T
  }
  // Read once a replacer asks, so a response that returns no value of a page parses no URL.
  let page: string | undefined
  const replacer = createStreamingReplacer(
    function getContext(value: unknown): ServerReplacerContext {
      return {
        createChannel,
        registerChannel,
        createBroadcastChannel,
        sendStream,
        responseState,
        get page() {
          return (page ??= getSessionToken(requestContext.request) ?? crypto.randomUUID())
        },
        validators: makeValidators(value, valueShields, shieldCtx),
        maxFrameBytes: getAdapterMaxFrameBytes(),
      }
    },
    function onReplaced({ value, abort }) {
      // Registered already, so its page may hold it: it ends with this response only once that goes.
      const carriedBefore = ServerChannel.isServerChannel(value) && value._wasRegistered
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

  // Its page left while the telefunction ran: nobody gets this response.
  if (requestContext.abortSignal.aborted) {
    requestContext.responseAbort.abort()
    return { type: 'text', body: httpResponseBody }
  }

  goes = true
  for (const register of registrations) register()

  if (useChannelPump || producers.length === 0) return { type: 'text', body: httpResponseBody }

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
      producers,
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
