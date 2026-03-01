export { makeHttpRequest }

import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import { objectAssign } from '../../utils/objectAssign.js'
import { callOnAbortListeners } from './onAbort.js'
import { decodeU32, concat } from '../../wire-protocol/frame.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../wire-protocol/constants.js'
import type { StreamingErrorFramePayload } from '../../wire-protocol/constants.js'
import { createStreamingReviver } from '../../wire-protocol/streaming-types/registry.client.js'
import {
  STATUS_CODE_SUCCESS,
  STATUS_CODE_THROW_ABORT,
  STATUS_CODE_MALFORMED_REQUEST,
  STATUS_CODE_INTERNAL_SERVER_ERROR,
  STATUS_CODE_SHIELD_VALIDATION_ERROR,
  STATUS_BODY_MALFORMED_REQUEST,
  STATUS_BODY_INTERNAL_SERVER_ERROR,
  STATUS_BODY_SHIELD_VALIDATION_ERROR,
} from '../../shared/constants.js'
import type { TelefuncResponseBody } from '../../shared/constants.js'

const method = 'POST'

async function makeHttpRequest(callContext: {
  telefuncUrl: string
  httpRequestBody: string | Blob
  telefunctionName: string
  telefuncFilePath: string
  headers: Record<string, string> | null
  fetch: typeof globalThis.fetch | null
  abortController: AbortController
}): Promise<unknown> {
  const isBinaryFrame = typeof callContext.httpRequestBody !== 'string'
  const contentType = isBinaryFrame ? { 'Content-Type': 'application/octet-stream' } : { 'Content-Type': 'text/plain' }
  let response: Response
  try {
    const fetch = callContext.fetch ?? window.fetch
    response = await fetch(callContext.telefuncUrl, {
      method,
      body: callContext.httpRequestBody,
      credentials: 'same-origin',
      headers: {
        ...contentType,
        ...callContext.headers,
      },
      signal: callContext.abortController.signal,
    })
  } catch (err) {
    if (callContext.abortController.signal.aborted) {
      throwCancelError()
    }
    const telefunctionCallError = new Error('No Server Connection')
    objectAssign(telefunctionCallError, { isConnectionError: true as const })
    throw telefunctionCallError
  }

  const statusCode = response.status

  if (statusCode === STATUS_CODE_SUCCESS) {
    const responseContentType = response.headers.get('content-type') || ''
    const isStreaming = responseContentType.includes('application/octet-stream')
    const { ret } = isStreaming
      ? await parseStreamingResponseBody(response, callContext)
      : await parseResponseBody(response, callContext)
    return ret
  } else if (statusCode === STATUS_CODE_THROW_ABORT) {
    const { ret } = await parseResponseBody(response, callContext)
    throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, ret)
  } else if (statusCode === STATUS_CODE_INTERNAL_SERVER_ERROR) {
    const errMsg = await getErrMsg(STATUS_BODY_INTERNAL_SERVER_ERROR, response, callContext)
    throwBugError(errMsg)
  } else if (statusCode === STATUS_CODE_SHIELD_VALIDATION_ERROR) {
    const errMsg = await getErrMsg(
      STATUS_BODY_SHIELD_VALIDATION_ERROR,
      response,
      callContext,
      ' (if enabled: https://telefunc.com/log)',
    )
    throw new Error(errMsg)
  } else if (statusCode === STATUS_CODE_MALFORMED_REQUEST) {
    const responseBody = await response.text()
    assertUsage(responseBody === STATUS_BODY_MALFORMED_REQUEST, wrongInstallation({ method, callContext }))
    /* With Next.js 12: when renaming a `.telefunc.js` file the client makes a request with the new `.telefunc.js` name while the server is still serving the old `.telefunc.js` name. Seems like a race condition: trying again seems to fix the error.
    // This should never happen as the Telefunc Client shouldn't make invalid requests
    assert(false)
    */
    assertUsage(false, 'Try again. You may need to reload the page. (The client and server are/was out-of-sync.)')
  } else {
    assertUsage(
      statusCode !== 404,
      wrongInstallation({
        reason: 'a 404 HTTP response',
        method,
        isNotInstalled: true,
        callContext,
      }),
    )
    assertUsage(
      false,
      wrongInstallation({
        reason: `a status code \`${statusCode}\` which Telefunc never returns`,
        method,
        callContext,
      }),
    )
  }
}

async function parseResponseBody(response: Response, callContext: { telefuncUrl: string }): Promise<{ ret: unknown }> {
  const responseBody = await response.text()
  const responseBodyParsed: unknown = parse(responseBody)
  assertUsage(isObject(responseBodyParsed) && 'ret' in responseBodyParsed, wrongInstallation({ method, callContext }))
  assert(response.status !== STATUS_CODE_THROW_ABORT || 'abort' in responseBodyParsed)
  const { ret } = responseBodyParsed as TelefuncResponseBody
  return { ret }
}

// ===== Streaming response parsing =====

async function parseStreamingResponseBody(
  response: Response,
  callContext: { telefunctionName: string; telefuncFilePath: string; abortController: AbortController },
): Promise<{ ret: unknown }> {
  assert(response.body)
  const reader = response.body.getReader()
  const streamReader = new StreamReader(reader, callContext)

  const cancelUpstream = () => {
    console.log('[client:stream] cancelUpstream called — cancelling reader')
    streamReader.cancelled = true
    reader.cancel()
  }

  const demuxer = new FrameDemuxer(streamReader, cancelUpstream)

  // Read metadata header
  const metaLenBuf = await streamReader.readExact(4)
  const metaLen = decodeU32(metaLenBuf)
  const metaBytes = await streamReader.readExact(metaLen)
  const metaText = new TextDecoder().decode(metaBytes)

  const getChunkReader = (index: number) => {
    demuxer.registerConsumer()
    return () => demuxer.readNextChunkForIndex(index)
  }

  const getCancelForIndex = (index: number) => {
    return demuxer.getCancelForIndex(index)
  }

  const { reviver } = createStreamingReviver(getChunkReader, getCancelForIndex)

  const parsed = parse(metaText, { reviver }) as TelefuncResponseBody
  assert(isObject(parsed) && 'ret' in parsed)

  return { ret: parsed.ret }
}

// ===== Frame demultiplexer =====

/** Demultiplexes indexed frames from a single HTTP stream to multiple consumers.
 *
 *  Best-effort backpressure: stops reading when an idle consumer's buffer hits
 *  MAX_BUFFER_PER_INDEX, resumes when drained. Active consumers (registered as
 *  waiters) receive frames via direct dispatch — zero buffering, zero delay.
 *
 *  Note: an index's buffer may briefly exceed MAX_BUFFER_PER_INDEX. This happens when
 *  another consumer's drain restarts the read loop, and the next frame on the wire
 *  is for the already-full index. Each restart adds at most 1 frame of overshoot.
 *  This is the unavoidable cost of multiplexing over a single stream — we can't
 *  peek at the next frame's index without reading it.
 *
 *  Cancellation follows .tee() semantics: cancelling one consumer marks its index
 *  as cancelled and drops future frames for it. Other consumers continue normally.
 *  The upstream reader is only cancelled when ALL consumers are cancelled. */
class FrameDemuxer {
  private static readonly MAX_BUFFER_BYTES_PER_INDEX = 1024 * 1024 // 1 MB
  private streamReader: StreamReader
  private pendingFrames = new Map<number, Uint8Array[]>()
  private pendingBytes = new Map<number, number>()
  private indexWaiters = new Map<number, { resolve: (v: Uint8Array | null) => void; reject: (e: unknown) => void }>()
  private reading = false
  private ended = false
  private streamError: unknown = null
  private cancelledIndices = new Set<number>()
  private doneIndices = new Set<number>()
  private totalConsumers = 0
  private cancelUpstream: (() => void) | null = null

  constructor(streamReader: StreamReader, cancelUpstream: () => void) {
    this.streamReader = streamReader
    this.cancelUpstream = cancelUpstream
  }

  registerConsumer() {
    this.totalConsumers++
  }

  /** Returns a cancel function for the given index. Follows .tee() semantics:
   *  marks the index as cancelled, drops its buffered/future frames, and resolves
   *  any pending waiter with null. Upstream is cancelled only when all consumers
   *  are cancelled. */
  getCancelForIndex(index: number): () => void {
    return () => {
      if (this.cancelledIndices.has(index)) return
      console.log(
        `[client:demux] cancelForIndex(${index}) called, cancelledIndices=${this.cancelledIndices.size + 1}/${this.totalConsumers}`,
      )
      this.cancelledIndices.add(index)
      // Drop buffered frames for this index
      this.pendingFrames.delete(index)
      // Resolve any pending waiter with null (stream ended for this consumer)
      const waiter = this.indexWaiters.get(index)
      if (waiter) {
        this.indexWaiters.delete(index)
        waiter.resolve(null)
      }
      // Cancel upstream when all consumers are cancelled
      if (this.cancelledIndices.size >= this.totalConsumers) {
        console.log('[client:demux] all consumers cancelled, cancelling upstream')
        this.cancelUpstream?.()
        this.cancelUpstream = null
      }
    }
  }

  async readNextChunkForIndex(index: number): Promise<Uint8Array | null> {
    if (this.cancelledIndices.has(index)) {
      console.log(`[client:demux] readNextChunkForIndex(${index}) — index cancelled, returning null`)
      return null
    }
    if (this.streamError) throw this.streamError

    const pending = this.pendingFrames.get(index)
    if (pending && pending.length > 0) {
      const frame = pending.shift()!
      this.pendingBytes.set(index, (this.pendingBytes.get(index) ?? 0) - frame.byteLength)
      console.log(
        `[client:demux] readNextChunkForIndex(${index}) — returning buffered frame (${frame.byteLength} bytes, ${pending.length} remaining, ${this.pendingBytes.get(index)} buffered bytes)`,
      )
      this.ensureReading()
      return frame
    }
    if (this.doneIndices.has(index)) {
      console.log(`[client:demux] readNextChunkForIndex(${index}) — index done, returning null`)
      return null
    }
    if (this.ended) {
      console.log(`[client:demux] readNextChunkForIndex(${index}) — stream ended, returning null`)
      return null
    }

    console.log(`[client:demux] readNextChunkForIndex(${index}) — registering waiter`)
    let resolve: (v: Uint8Array | null) => void
    let reject: (e: unknown) => void
    const promise = new Promise<Uint8Array | null>((res, rej) => {
      resolve = res
      reject = rej
    })
    this.indexWaiters.set(index, { resolve: resolve!, reject: reject! })
    this.ensureReading()
    return promise
  }

  private async ensureReading() {
    if (this.reading) return
    this.reading = true
    console.log(`[client:demux] ensureReading started, waiters=[${[...this.indexWaiters.keys()].join(',')}]`)
    try {
      while (this.indexWaiters.size > 0) {
        console.log(`[client:demux] reading next frame... (waiters=[${[...this.indexWaiters.keys()].join(',')}])`)
        const frame = await this.streamReader.readNextFrame()
        if (frame === null) {
          console.log('[client:demux] readNextFrame returned null (terminator/end), resolving all waiters with null')
          this.ended = true
          for (const [index, w] of this.indexWaiters) {
            console.log(`[client:demux] resolving waiter index=${index} with null`)
            w.resolve(null)
          }
          this.indexWaiters.clear()
          return
        }

        console.log(`[client:demux] received frame index=${frame.index} payloadLen=${frame.payload.length}`)
        // Drop frames for cancelled indices
        if (this.cancelledIndices.has(frame.index)) {
          console.log(`[client:demux] dropping frame for cancelled index=${frame.index}`)
          continue
        }

        // Empty payload = per-index "done" signal
        if (frame.payload.length === 0) {
          console.log(`[client:demux] empty frame = done signal for index=${frame.index}`)
          this.doneIndices.add(frame.index)
          const waiter = this.indexWaiters.get(frame.index)
          if (waiter) {
            this.indexWaiters.delete(frame.index)
            console.log(`[client:demux] resolving waiter for done index=${frame.index} with null`)
            waiter.resolve(null)
          }
          continue
        }

        // Direct dispatch — no buffering, no delay
        const waiter = this.indexWaiters.get(frame.index)
        if (waiter) {
          this.indexWaiters.delete(frame.index)
          console.log(`[client:demux] direct dispatch index=${frame.index} (${frame.payload.length} bytes)`)
          waiter.resolve(frame.payload)
          continue
        }

        // No consumer waiting — buffer it
        const pending = this.pendingFrames.get(frame.index)
        if (pending) pending.push(frame.payload)
        else this.pendingFrames.set(frame.index, [frame.payload])
        const newBytes = (this.pendingBytes.get(frame.index) ?? 0) + frame.payload.byteLength
        this.pendingBytes.set(frame.index, newBytes)
        console.log(`[client:demux] buffered frame index=${frame.index} (${newBytes} bytes buffered)`)

        // Per-index backpressure: stop reading when this index's buffer exceeds 1 MB.
        // The loop restarts when the consumer drains via readNextChunkForIndex().
        if (newBytes >= FrameDemuxer.MAX_BUFFER_BYTES_PER_INDEX) {
          console.log(
            `[client:demux] backpressure break for index=${frame.index} (${newBytes} bytes, waiters=[${[...this.indexWaiters.keys()].join(',')}])`,
          )
          break
        }
      }
      console.log(`[client:demux] ensureReading loop exited (waiters=${this.indexWaiters.size})`)
    } catch (err) {
      console.log('[client:demux] ensureReading caught error:', err)
      this.streamError ??= err
      for (const [, w] of this.indexWaiters) w.reject(err)
      this.indexWaiters.clear()
    } finally {
      this.reading = false
      console.log('[client:demux] ensureReading done')
    }
  }
}

// ===== Client StreamReader =====

const EMPTY = new Uint8Array(0)

/** Buffered reader for the HTTP response body stream.
 *
 *  readExact: read N bytes (low-level byte I/O with buffering).
 *  readNextFrame: read one indexed frame (wire protocol + error handling). */
class StreamReader {
  private reader: ReadableStreamDefaultReader<Uint8Array>
  private callContext: { telefunctionName: string; telefuncFilePath: string; abortController: AbortController }
  private buffer: Uint8Array = EMPTY
  cancelled = false

  constructor(
    reader: ReadableStreamDefaultReader<Uint8Array>,
    callContext: { telefunctionName: string; telefuncFilePath: string; abortController: AbortController },
  ) {
    this.reader = reader
    this.callContext = callContext

    // When the fetch is aborted, cancel the reader first to prevent the browser
    // from generating a spurious unhandled "BodyStreamBuffer was aborted" rejection.
    callContext.abortController.signal.addEventListener(
      'abort',
      () => {
        console.log('[client:reader] abortController signal fired, cancelling reader')
        reader.cancel()
      },
      { once: true },
    )
  }

  async readExact(n: number): Promise<Uint8Array> {
    while (this.buffer.length < n) {
      let done: boolean
      let value: Uint8Array | undefined
      let readError: unknown
      try {
        ;({ done, value } = await this.reader.read())
      } catch (err) {
        console.log('[client:reader] readExact read() threw:', err)
        readError = err
        done = true
      }
      if (done) {
        if (this.callContext.abortController.signal.aborted) {
          console.log('[client:reader] readExact — aborted, throwing cancel')
          throwCancelError()
        }
        if (this.cancelled) {
          console.log('[client:reader] readExact — cancelled, returning EMPTY')
          return EMPTY
        }
        console.log(
          '[client:reader] readExact — stream ended unexpectedly (wanted',
          n,
          'bytes, have',
          this.buffer.length,
          ')',
        )
        throw readError ?? new Error('Connection lost — the server closed the stream before all data was received.')
      }
      this.buffer = this.buffer.length === 0 ? value! : concat(this.buffer, value!)
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = n < this.buffer.length ? this.buffer.subarray(n) : EMPTY
    return result
  }

  /** Read the next indexed frame from the wire.
   *  Returns { index, payload } or null on terminator. Throws on error frames. */
  async readNextFrame(): Promise<{ index: number; payload: Uint8Array } | null> {
    const lenBuf = await this.readExact(4)
    if (this.cancelled) {
      console.log('[client:reader] readNextFrame — cancelled after reading len')
      return null
    }
    const len = decodeU32(lenBuf)
    if (len === 0) {
      console.log('[client:reader] readNextFrame — got terminator (len=0)')
      return null
    }
    if (len === STREAMING_ERROR_FRAME_MARKER) {
      console.log('[client:reader] readNextFrame — got ERROR frame marker')
      // Error frame: [ERROR_MARKER][u32 payload_len][payload_bytes]
      const errorLenBuf = await this.readExact(4)
      const errorLen = decodeU32(errorLenBuf)
      const errorBytes = await this.readExact(errorLen)
      const errorPayload = parse(new TextDecoder().decode(errorBytes)) as StreamingErrorFramePayload
      if (errorPayload.type === STREAMING_ERROR_TYPE.ABORT) {
        throwAbortError(this.callContext.telefunctionName, this.callContext.telefuncFilePath, errorPayload.abortValue)
      }
      throwBugError()
    }
    const frameData = await this.readExact(len)
    if (this.cancelled) return null
    return { index: frameData[0]!, payload: frameData.subarray(1) }
  }
}

// ===== Helpers =====

function wrongInstallation({
  reason = 'an HTTP response body that Telefunc never generates',
  callContext,
  method,
  isNotInstalled,
}: {
  reason?: string
  isNotInstalled?: true
  method: 'GET' | 'POST'
  callContext: { telefuncUrl: string }
}) {
  let msg = [`Telefunc doesn't seem to be `]
  if (!isNotInstalled) msg.push('(properly) ')
  msg.push('installed on your server')
  msg.push(...[`: the HTTP ${method} \`${callContext.telefuncUrl}\` request returned `, reason])
  msg.push(`, see https://telefunc.com/install`)
  return msg.join('')
}

async function getErrMsg(
  errMsg: typeof STATUS_BODY_INTERNAL_SERVER_ERROR | typeof STATUS_BODY_SHIELD_VALIDATION_ERROR,
  response: Response,
  callContext: { telefuncUrl: string },
  errMsgAddendum: ' (if enabled: https://telefunc.com/log)' | '' = '',
) {
  const responseBody = await response.text()
  assertUsage(responseBody === errMsg, wrongInstallation({ method, callContext }))
  return `${errMsg} — see server logs${errMsgAddendum}` as const
}

function throwCancelError(): never {
  const cancelError = new Error('Telefunc call cancelled')
  objectAssign(cancelError, { isCancel: true as const })
  throw cancelError
}

function throwAbortError(telefunctionName: string, telefuncFilePath: string, abortValue: unknown): never {
  const telefunctionCallError = new Error(`Aborted telefunction call ${telefunctionName}() (${telefuncFilePath}).`)
  objectAssign(telefunctionCallError, { isAbort: true as const, abortValue })
  callOnAbortListeners(telefunctionCallError)
  throw telefunctionCallError
}

function throwBugError(errMsg = `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`): never {
  throw new Error(errMsg)
}
