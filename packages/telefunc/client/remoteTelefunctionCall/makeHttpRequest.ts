export { makeHttpRequest }

import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import { objectAssign } from '../../utils/objectAssign.js'
import { callOnAbortListeners } from './onAbort.js'
import { createStreamReviver } from '../../shared/wire-protocol/reviver-response.js'
import {
  STATUS_CODE_THROW_ABORT,
  STATUS_CODE_INTERNAL_SERVER_ERROR,
  STATUS_BODY_INTERNAL_SERVER_ERROR,
  STATUS_CODE_MALFORMED_REQUEST,
  STATUS_BODY_MALFORMED_REQUEST,
  STATUS_CODE_SHIELD_VALIDATION_ERROR,
  STATUS_BODY_SHIELD_VALIDATION_ERROR,
  STATUS_CODE_SUCCESS,
} from '../../shared/constants.js'
import { STREAMING_ERROR_FRAME_MARKER, STREAMING_ERROR_TYPE } from '../../shared/wire-protocol/constants.js'
import type { StreamingErrorFramePayload } from '../../shared/wire-protocol/constants.js'
import type { TelefuncResponseBody } from '../../shared/constants.js'

const method = 'POST'

async function makeHttpRequest(callContext: {
  telefuncUrl: string
  httpRequestBody: string | Blob
  telefunctionName: string
  telefuncFilePath: string
  httpHeaders: Record<string, string> | null
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
        ...callContext.httpHeaders,
      },
      signal: callContext.abortController.signal,
    })
  } catch (err) {
    if (callContext.abortController.signal.aborted) {
      const cancelError = new Error('Telefunc call cancelled')
      objectAssign(cancelError, { isCancelled: true as const })
      throw cancelError
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

// ===== Streaming response parsing =====

/** Parse a binary streaming response: [u32 metadata len][metadata JSON][chunk frames...][zero terminator] */
async function parseStreamingResponseBody(
  response: Response,
  callContext: { telefunctionName: string; telefuncFilePath: string },
): Promise<{ ret: unknown }> {
  assert(response.body)
  const reader = response.body.getReader()
  const streamReader = new StreamReader(reader, callContext)

  // Read metadata header
  const metaLenBuf = await streamReader.readExact(4)
  const metaLen = new DataView(metaLenBuf.buffer, metaLenBuf.byteOffset, 4).getUint32(0, false)
  const metaBytes = await streamReader.readExact(metaLen)
  const metaText = new TextDecoder().decode(metaBytes)

  const reviver = createStreamReviver({
    createStream: (_meta) => {
      return new ReadableStream<Uint8Array>({
        async pull(controller) {
          try {
            const chunk = await streamReader.readNextChunk()
            if (chunk === null) controller.close()
            else controller.enqueue(chunk)
          } catch (err) {
            reader.cancel()
            controller.error(err)
          }
        },
        cancel() {
          reader.cancel()
        },
      })
    },
    createGenerator: (_meta) => {
      const gen = (async function* () {
        try {
          while (true) {
            const chunk = await streamReader.readNextChunk()
            if (chunk === null) return
            yield parse(new TextDecoder().decode(chunk))
          }
        } finally {
          reader.cancel()
        }
      })()
      const origReturn = gen.return.bind(gen)
      gen.return = (...args: Parameters<(typeof gen)['return']>) => {
        streamReader.cancelled = true
        reader.cancel()
        return origReturn(...args)
      }
      return gen
    },
  })

  const parsed = parse(metaText, { reviver }) as TelefuncResponseBody
  assert(isObject(parsed) && 'ret' in parsed)

  return { ret: parsed.ret }
}

const EMPTY = new Uint8Array(0)

class StreamReader {
  private reader: ReadableStreamDefaultReader<Uint8Array>
  private callContext: { telefunctionName: string; telefuncFilePath: string }
  private buffer: Uint8Array = EMPTY
  cancelled = false

  constructor(
    reader: ReadableStreamDefaultReader<Uint8Array>,
    callContext: { telefunctionName: string; telefuncFilePath: string },
  ) {
    this.reader = reader
    this.callContext = callContext
  }

  async readExact(n: number): Promise<Uint8Array> {
    while (this.buffer.length < n) {
      const { done, value } = await this.reader.read()
      if (done) {
        if (this.cancelled) return EMPTY
        throw new Error('Connection lost — the server closed the stream before all data was received.')
      }
      this.buffer = this.buffer.length === 0 ? value : concat(this.buffer, value)
    }
    const result = this.buffer.subarray(0, n)
    this.buffer = n < this.buffer.length ? this.buffer.subarray(n) : EMPTY
    return result
  }

  async readNextChunk(): Promise<Uint8Array | null> {
    const lenBuf = await this.readExact(4)
    if (this.cancelled) return null
    const len = new DataView(lenBuf.buffer, lenBuf.byteOffset, 4).getUint32(0, false)
    if (len === 0) return null
    if (len === STREAMING_ERROR_FRAME_MARKER) {
      // Error frame: [ERROR_MARKER][u32 payload_len][payload_bytes]
      const errorLenBuf = await this.readExact(4)
      const errorLen = new DataView(errorLenBuf.buffer, errorLenBuf.byteOffset, 4).getUint32(0, false)
      const errorBytes = await this.readExact(errorLen)
      const errorPayload = parse(new TextDecoder().decode(errorBytes)) as StreamingErrorFramePayload
      if (errorPayload.type === STREAMING_ERROR_TYPE.ABORT) {
        throwAbortError(this.callContext.telefunctionName, this.callContext.telefuncFilePath, errorPayload.abortValue)
      }
      throwBugError()
    }
    return this.readExact(len)
  }
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const result = new Uint8Array(a.length + b.length)
  result.set(a, 0)
  result.set(b, a.length)
  return result
}

// ===== Shared error helpers =====

function throwAbortError(telefunctionName: string, telefuncFilePath: string, abortValue: unknown): never {
  const telefunctionCallError = new Error(`Aborted telefunction call ${telefunctionName}() (${telefuncFilePath}).`)
  objectAssign(telefunctionCallError, { isAbort: true as const, abortValue })
  callOnAbortListeners(telefunctionCallError)
  throw telefunctionCallError
}

function throwBugError(errMsg = `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`): never {
  throw new Error(errMsg)
}
