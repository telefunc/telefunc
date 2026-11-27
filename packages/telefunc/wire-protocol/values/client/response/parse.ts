export { parseResponse }

import { assert } from '../../../../utils/assert.js'
import { isObject } from '../../../../utils/isObject.js'
import { isObjectOrFunction } from '../../../../utils/isObjectOrFunction.js'
import type { ClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import { setAbortController } from '../../../../client/abort.js'
import { setCloseHandlers, addExtraCloseHandlers, type CloseHandler } from '../../../../client/close.js'
import {
  makeAbortError,
  throwAbortError,
  throwBugError,
  throwIfAborted,
} from '../../../../client/remoteTelefunctionCall/errors.js'
import type { BaseStreamReader } from './BaseStreamReader.js'
import type { FrameDemuxer } from './FrameDemuxer.js'
import { isPendingValue, resolvePendingValues, type PendingValue } from './PendingValue.js'
import type { ConnectionOptions } from '../../../channel/client/ClientChannel.js'
import { wrapProxy } from '../../../../utils/wrapProxy.js'
import { GcRegistry } from '../../../../utils/GcRegistry.js'
import { STREAMING_ERROR_TYPE } from '../../../constants.js'
import { getGlobalObject } from '../../../../utils/getGlobalObject.js'
import { ResponseValues } from './registry.js'
import { loadFeatures, loadedFeatures, loadedFeature } from '../loadFeatures.js'

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
    const body = await readText(response, callContext)
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
    const streams = loadedFeatures().streams ?? (await loadFeatures(['streams'], callContext)).streams
    assert(streams)
    // The reader cancels the body on the call's abort from here on.
    throwIfAborted(callContext)
    streamReader = streams.createBodyReader(body.getReader(), isSse, callContext)
    const metaLen = await streamReader.readU32()
    const metaBytes = await streamReader.readExact(metaLen)
    // An abort during the reads let go of the body: what they returned is no metadata.
    throwIfAborted(callContext)
    const metaText = new TextDecoder().decode(metaBytes)
    return await reviveResponse(metaText, callContext, connection, streamReader)
  } catch (err) {
    if (streamReader) streamReader.cancel()
    // An errored body rejects its cancel: ignoring that keeps the call's original failure, the body's or the import's.
    else body.cancel().catch(() => {})
    throw err
  }
}

/** A text body, read as `response.text()` reads it, but that the call's abort cancels: an app's `config.fetch` may
 *  return a body the call's signal doesn't end. */
async function readText(response: Response, callContext: CallContext): Promise<string> {
  const { signal } = callContext.abortController
  const { body } = response
  // Fetch polyfills (`whatwg-fetch`, React Native) give no body stream.
  if (!body) {
    const text = await response.text().catch((err: unknown) => {
      if (!signal.aborted) throw err
      return ''
    })
    throwIfAborted(callContext)
    return text
  }
  const reader = body.getReader()
  const cancel = () => {
    reader.cancel().catch(() => {})
  }
  signal.addEventListener('abort', cancel, { once: true })
  if (signal.aborted) cancel()
  const decoder = new TextDecoder()
  let text = ''
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (signal.aborted) break
      if (done) return text + decoder.decode()
      text += decoder.decode(value, { stream: true })
    }
  } catch (err) {
    // A body that the abort errored, as a fetch's own does, ends the call as aborted.
    if (!signal.aborted) throw err
  } finally {
    signal.removeEventListener('abort', cancel)
    reader.releaseLock()
  }
  // The read ends early only on the call's abort.
  throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, undefined)
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
      const { ClientChannel } = loadedFeature('channels')
      return new ClientChannel({
        channelId: opts.channelId,
        ack: opts.ack,
        maxFrameBytes: opts.maxFrameBytes,
        connection,
      })
    },
    createBroadcast(opts) {
      const { ClientBroadcast } = loadedFeature('broadcasts')
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
        const { ClientChannel, ChannelStreamSource } = loadedFeature('channels')
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
    const values = new ResponseValues(body, callContext.extensionResponseTypes, !bodyStreamReader)
    const needed = values.missingFeatures(loadedFeatures())
    const loaded = needed?.length ? await loadFeatures(needed, callContext) : loadedFeatures()
    throwIfAborted(callContext)
    parsed = values.revive(context, handOut, loaded)
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
