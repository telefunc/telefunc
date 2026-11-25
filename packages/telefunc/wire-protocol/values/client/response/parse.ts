export { parseResponse }

import { parse } from '@brillout/json-serializer/parse'
import { assert } from '../../../../utils/assert.js'
import { isObject } from '../../../../utils/isObject.js'
import { isObjectOrFunction } from '../../../../utils/isObjectOrFunction.js'
import type { ClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import { setAbortController } from '../../../../client/abort.js'
import { setCloseHandlers, addExtraCloseHandlers, type CloseHandler } from '../../../../client/close.js'
import { makeAbortError, throwAbortError, throwBugError } from '../../../../client/remoteTelefunctionCall/errors.js'
import type { BaseStreamReader } from './BaseStreamReader.js'
import type { FrameDemuxer } from './FrameDemuxer.js'
import { isPendingValue, resolvePendingValues, type PendingValue } from './PendingValue.js'
import type { ConnectionOptions } from '../../../channel/client/ClientChannel.js'
import { wrapProxy } from '../../../../utils/wrapProxy.js'
import { GcRegistry } from '../../../../utils/GcRegistry.js'
import { STREAMING_ERROR_TYPE } from '../../../constants.js'
import { getGlobalObject } from '../../../../utils/getGlobalObject.js'
import { createStreamingReviver, featuresIn } from './registry.js'
import { loadFeatures, loadedFeatures, loadedChannels } from '../loadFeatures.js'

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

  try {
    // HTTP streaming (binary / SSE): its body carries streams, which the stream readers read.
    if (isStreaming) {
      const { body } = response
      assert(body)
      return await parseStreamedResponse(body, contentType.includes('text/event-stream'), callContext, connection)
    }

    // Text response: plain or channel-pump streaming (each streaming value owns its own channel).
    const body = await response.text()
    await loadFeaturesIn(body, true, callContext)
    return await reviveResponse(body, callContext, connection, null)
  } catch (err) {
    for (const close of callContext.requestCloseHandlers) void closeOnFailure(close)
    throw err
  }
}

/** Until its values are handed out, the call owns the body: an abort or a failed load cancels it, so that the server
 *  stops sending it. */
async function parseStreamedResponse(
  body: ReadableStream<Uint8Array<ArrayBuffer>>,
  isSse: boolean,
  callContext: CallContext,
  connection: ConnectionOptions,
): Promise<unknown> {
  let streamReader: BaseStreamReader | undefined
  try {
    const { streams } = await loadFeatures(['streams'], callContext)
    assert(streams)
    streamReader = streams.createBodyReader(body.getReader(), isSse, callContext)
    const metaLen = await streamReader.readU32()
    const metaBytes = await streamReader.readExact(metaLen)
    if (callContext.abortController.signal.aborted) {
      throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, undefined)
    }
    const metaText = new TextDecoder().decode(metaBytes)
    await loadFeaturesIn(metaText, false, callContext)
    return await reviveResponse(metaText, callContext, connection, streamReader)
  } catch (err) {
    if (streamReader) streamReader.cancel()
    // An errored body rejects its cancel; that error already reached the call.
    else body.cancel().catch(() => {})
    throw err
  }
}

/** Loads the code the values in a response's serialized `text` need, before any of them revives. */
async function loadFeaturesIn(text: string, streamsRideChannels: boolean, callContext: CallContext): Promise<void> {
  const loaded = loadedFeatures()
  if (loaded.streams && loaded.channels && loaded.broadcasts) return
  const needed = [...featuresIn(text, streamsRideChannels)].filter((feature) => !loaded[feature])
  if (needed.length > 0) await loadFeatures(needed, callContext)
}

// ===== Response revival =====

async function reviveResponse(
  body: string,
  callContext: CallContext,
  connection: ConnectionOptions,
  bodyStreamReader: BaseStreamReader | null,
): Promise<unknown> {
  const closeHandlers = new WeakMap<object, () => void>()

  const throwStreamError = (errorPayload: Record<string, unknown>): never => {
    if (errorPayload.type === STREAMING_ERROR_TYPE.ABORT && 'abortValue' in errorPayload) {
      throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, errorPayload.abortValue)
    }
    throwBugError()
  }

  const pendingValues: PendingValue<unknown>[] = []
  const responseCloseHandlers: CloseHandler[] = []
  let demuxer: FrameDemuxer | undefined
  const context: ClientReviverContext = {
    createChannel(opts) {
      const { ClientChannel } = loadedChannels()
      return new ClientChannel({
        channelId: opts.channelId,
        ack: opts.ack,
        maxFrameBytes: opts.maxFrameBytes,
        connection,
      })
    },
    createBroadcast(opts) {
      const { broadcasts } = loadedFeatures()
      assert(broadcasts)
      return new broadcasts.ClientBroadcast({
        channelId: opts.channelId,
        key: opts.key,
        lossy: opts.lossy,
        maxFrameBytes: opts.maxFrameBytes,
        connection,
      })
    },
    receiveStream(metadata) {
      if ('channelId' in metadata) {
        const { ClientChannel, ChannelStreamSource } = loadedChannels()
        const channel = new ClientChannel({ channelId: metadata.channelId, connection })
        return ChannelStreamSource.create(channel, { throwError: throwStreamError })
      }
      assert(bodyStreamReader, 'Unexpected receiveStream call in non-streaming response')
      const { streams } = loadedFeatures()
      assert(streams)
      demuxer ??= new streams.FrameDemuxer(bodyStreamReader, callContext.telefunctionName)
      return demuxer.create(metadata.__index)
    },
  }
  const reviver = createStreamingReviver(context, handOut, callContext.extensionResponseTypes, loadedFeatures())

  function handOut(revived: ReturnType<ReviverType['revive']>) {
    const { value, abort, close, onEnd } = revived
    responseCloseHandlers.push(close)
    callContext.abortController.signal.addEventListener(
      'abort',
      () => {
        abort(makeAbortError(undefined, callContext))
      },
      { once: true },
    )

    if (isPendingValue(value)) {
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
  }

  let parsed: unknown
  try {
    parsed = parse(body, { reviver })
    if (pendingValues.length > 0) {
      parsed = await resolvePendingValues(parsed, pendingValues, (value) => closeHandlers.has(value))
    }
    demuxer?.valuesHandedOut()
  } catch (err) {
    for (const close of responseCloseHandlers) void closeOnFailure(close)
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

async function closeOnFailure(close: CloseHandler): Promise<void> {
  try {
    await close()
  } catch {
    // The caller gets the response failure.
  }
}
