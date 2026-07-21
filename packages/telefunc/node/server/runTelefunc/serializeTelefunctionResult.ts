export { serializeTelefunctionResult }
export type { TelefuncId }

import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { hasProp } from '../../../utils/hasProp.js'
import { lowercaseFirstLetter } from '../../../utils/lowercaseFirstLetter.js'
import { createStreamingReplacer } from '../../../wire-protocol/server/response/registry.js'
import { ServerChannel } from '../../../wire-protocol/server/channel.js'
import { getChannelMux } from '../../../wire-protocol/server/mux.js'
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
import { getServerConfig } from '../serverConfig.js'
import { type RequestContext } from '../context/requestContext.js'
import type { Context } from '../context/context.js'
import type { ReplacerType, TypeContract, ServerReplacerContext } from '../../../wire-protocol/types.js'
import type { Readable } from 'node:stream'

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
    extensionResponseTypes?: ReplacerType<TypeContract, ServerReplacerContext>[]
    log: { shieldErrors: ShieldLogConfig }
  }
  valueShields?: ValueShields
}): SerializeResult {
  const { requestContext } = runContext
  // Consult the LIVE config here, not just the request-start snapshot on `runContext`.
  //
  // A request resolves its config BEFORE it loads the user's `.telefunc.js` files, and an extension may
  // register while those files evaluate — `config.extensions.push(…)` at module level is a supported way to
  // install one. Reading the snapshot made every such extension's `responseTypes` invisible to the very
  // request that loaded it: the value it exists to encode serialized as an ordinary object instead, with no
  // error anywhere, so the client silently received something that was never replaced.
  //
  // Built-in types never had this problem — they are a static list — which is why it stayed hidden until an
  // externally-registered response type was tried. Serialization happens well after the files are loaded, so
  // the live config is what a request must be judged against. (See the standing TO-DO at the snapshot site
  // in runTelefunc.ts, which already suspected the copy was unnecessary.)
  //
  // A UNION rather than a replacement, deliberately. In production the snapshot is always a prefix of the
  // live list, so the union IS the live list. Where they differ is a caller that hands in types directly
  // without registering them globally — which is how `ref-identity.spec.ts` drives the pipeline. Replacing
  // the snapshot silently took that seam away and broke it; unioning can only ever ADD types, so no existing
  // behaviour is lost and late registrations become visible.
  const extensionResponseTypes = [
    ...new Set([
      ...(runContext.serverConfig.extensionResponseTypes ?? []),
      ...getServerConfig().extensionResponseTypes,
    ]),
  ]

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
