export { parseResponse }
export { BaseStreamReader } from './BaseStreamReader.js'

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../../../../utils/assert.js'
import { isObject } from '../../../../utils/isObject.js'
import { isObjectOrFunction } from '../../../../utils/isObjectOrFunction.js'
import { createStreamingReviver } from './registry.js'
import type { ClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import { setAbortController } from '../../../../client/abort.js'
import { setCloseHandlers, addExtraCloseHandlers, type CloseHandler } from '../../../../client/close.js'
import { makeAbortError, throwAbortError, throwBugError } from '../../../../client/remoteTelefunctionCall/errors.js'
import { BaseStreamReader } from './BaseStreamReader.js'
import { PendingValue, resolvePendingValues } from './PendingValue.js'
import { StreamReader } from './StreamReader.js'
import { SseStreamReader } from './SseStreamReader.js'
import { FrameDemuxer } from './FrameDemuxer.js'
import { ClientChannel, type ConnectionOptions } from '../../../channel/client/ClientChannel.js'
import { ClientBroadcast } from '../../../channel/client/ClientBroadcast.js'
import { wrapProxy } from '../../../../utils/wrapProxy.js'
import { GcRegistry } from '../../../../utils/GcRegistry.js'
import { ChannelStreamSource } from '../../ChannelStreamSource.js'
import { STREAMING_ERROR_TYPE } from '../../../constants.js'
import { getGlobalObject } from '../../../../utils/getGlobalObject.js'

const globalObject = getGlobalObject('wire-protocol/values/client/response/parse.ts', {
  gcRegistry: new GcRegistry(),
})

// ===== Types =====

type CallContext = {
  telefunctionName: string
  telefuncFilePath: string
  abortController: AbortController
  requestCloseHandlers: CloseHandler[]
  extensionResponseTypes: ReviverType<TypeContract, ClientReviverContext>[]
}

// ===== Public entry point =====

/** Single entry point for all response transports.
 *
 *  Detects the transport from response headers / body and delegates to
 *  the right reviver — the caller never needs to know which transport
 *  was used.
 *
 *  - `application/octet-stream` → HTTP binary stream
 *  - `text/event-stream`        → SSE stream
 *  - text                       → plain or channel-pump streaming */
async function parseResponse(
  response: Response,
  callContext: CallContext,
  connection: ConnectionOptions,
): Promise<unknown> {
  const contentType = response.headers.get('content-type') ?? ''
  const isStreaming = contentType.includes('application/octet-stream') || contentType.includes('text/event-stream')

  // HTTP streaming (binary / SSE)
  if (isStreaming) {
    assert(response.body)
    const reader = response.body.getReader() as ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>
    const isSSE = contentType.includes('text/event-stream')
    const streamReader = isSSE
      ? //
        new SseStreamReader(reader, callContext)
      : new StreamReader(reader, callContext)
    const metaLen = await streamReader.readU32()
    const metaBytes = await streamReader.readExact(metaLen)
    const metaText = new TextDecoder().decode(metaBytes)
    return reviveResponse(metaText, callContext, connection, streamReader)
  }

  // Text response: plain or channel-pump streaming (each streaming value owns its own channel).
  const body = await response.text()
  return reviveResponse(body, callContext, connection, null)
}

// ===== Response revival =====

async function reviveResponse(
  body: string,
  callContext: CallContext,
  connection: ConnectionOptions,
  bodyStreamReader: BaseStreamReader | null,
): Promise<unknown> {
  const { extensionResponseTypes } = callContext
  const closeHandlers = new WeakMap<object, () => void>()

  const throwStreamError = (errorPayload: Record<string, unknown>): never => {
    if (errorPayload.type === STREAMING_ERROR_TYPE.ABORT && 'abortValue' in errorPayload) {
      throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, errorPayload.abortValue)
    }
    throwBugError()
  }

  const pendingValues: PendingValue<unknown>[] = []
  let demuxer: FrameDemuxer | undefined
  const context: ClientReviverContext = {
    createChannel(opts) {
      return new ClientChannel({
        channelId: opts.channelId,
        ack: opts.ack,
        maxFrameBytes: opts.maxFrameBytes,
        connection,
      })
    },
    createBroadcast(opts) {
      return new ClientBroadcast({
        channelId: opts.channelId,
        key: opts.key,
        lossy: opts.lossy,
        maxFrameBytes: opts.maxFrameBytes,
        connection,
      })
    },
    receiveStream(metadata) {
      if ('channelId' in metadata) {
        const channel = new ClientChannel({ channelId: metadata.channelId, connection })
        return ChannelStreamSource.create(channel, { throwError: throwStreamError })
      }
      assert(bodyStreamReader, 'Unexpected receiveStream call in non-streaming response')
      demuxer ??= new FrameDemuxer(bodyStreamReader, callContext.telefunctionName)
      return demuxer.create(metadata.__index)
    },
  }

  const allCloseHandlers = [...callContext.requestCloseHandlers]

  const reviver = createStreamingReviver(
    context,
    function handOut(revived) {
      const { value, abort, close, onEnd } = revived
      allCloseHandlers.push(close)
      callContext.abortController.signal.addEventListener(
        'abort',
        () => {
          abort(makeAbortError(undefined, callContext))
        },
        { once: true },
      )

      if (value instanceof PendingValue) {
        // Suppress unhandled-rejection noise if `parse()` throws before the pending values are awaited.
        value.promise.catch(() => {})
        pendingValues.push(value)
        return value
      }

      assert(isObjectOrFunction(value))
      const wrapper = wrapProxy(value)
      const forget = globalObject.gcRegistry.register(wrapper, close)
      // Forgotten at its value's end, as the value may keep what its wrapper returned, which keeps the wrapper.
      onEnd?.(forget)
      closeHandlers.set(wrapper, close)
      // This is what the user gets
      return wrapper
    },
    extensionResponseTypes,
  )

  let parsed: unknown
  try {
    parsed = parse(body, { reviver })
    if (pendingValues.length > 0) {
      parsed = await resolvePendingValues(parsed, pendingValues, (value) => closeHandlers.has(value))
    }
    demuxer?.valuesHandedOut()
  } catch (err) {
    for (const close of allCloseHandlers) close()
    throw err
  }
  if (!isObject(parsed)) return parsed

  const { ret } = parsed
  if (isObjectOrFunction(ret)) {
    setAbortController(ret, callContext.abortController)
    setCloseHandlers(ret, closeHandlers)
    addExtraCloseHandlers(ret, callContext.requestCloseHandlers)
  }

  return { ret }
}
