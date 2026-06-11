export { runTelefunc }
export type { HttpResponse }

import { assert, assertUsage, assertWarning } from '../../utils/assert.js'
import { isProduction } from '../../utils/isProduction.js'
import { objectAssign } from '../../utils/objectAssign.js'
import { hasProp } from '../../utils/hasProp.js'
import { Telefunc, PROVIDED_CONTEXT } from './context/getContext.js'
import { REQUEST_CONTEXT } from './context/requestContext.js'
import { loadTelefuncFiles } from './runTelefunc/loadTelefuncFiles.js'
import { parseHttpRequest } from './runTelefunc/parseHttpRequest.js'
// import { getEtag } from './runTelefunc/getEtag.js'
import { executeTelefunction } from './runTelefunc/executeTelefunction.js'
import { serializeTelefunctionResult } from './runTelefunc/serializeTelefunctionResult.js'
import { handleTelefunctionBug } from './runTelefunc/validateTelefunctionError.js'
import { handleError } from './runTelefunc/handleError.js'
import { applyShield } from './runTelefunc/applyShield.js'
import { findTelefunction } from './runTelefunc/findTelefunction.js'
import { resolveReturnShields } from './shield.js'
import { getServerConfig } from './serverConfig.js'
import { isShieldValidationError } from '../../shared/ShieldValidationError.js'
import type { Readable as StreamReadableNode, Writable as StreamWritableNode } from 'node:stream'
import { getStreamNodeModule, loadStreamNodeModuleOnce } from '../../utils/loadStreamNodeModule.js'
import {
  STATUS_CODE_THROW_ABORT,
  STATUS_CODE_SHIELD_VALIDATION_ERROR,
  STATUS_BODY_SHIELD_VALIDATION_ERROR,
  STATUS_CODE_INTERNAL_SERVER_ERROR,
  STATUS_BODY_INTERNAL_SERVER_ERROR,
  STATUS_CODE_MALFORMED_REQUEST,
  STATUS_BODY_MALFORMED_REQUEST,
  STATUS_CODE_SUCCESS,
} from '../../shared/constants.js'
import { STREAM_TRANSPORT } from '../../wire-protocol/constants.js'
import { createRequestContext } from './context/requestContext.js'

type StreamWritableWeb = WritableStream
type StreamReadableWeb = ReadableStream

/** The HTTP Response of a telefunction remote call HTTP Request */
type HttpResponse = {
  /** HTTP Response Status Code */
  statusCode: 200 | 400 | 403 | 422 | 500
  /** HTTP Response Body (only for non-streaming responses, throws for streaming — use `pipe()` or `getReadableWebStream()` instead) */
  body: string
  /** HTTP Response Headers */
  headers: [string, string][]
  /** Pipe the response body to a Node.js or Web writable stream. The returned
   *  promise resolves once the body has been fully written (for streaming
   *  responses, that's when the underlying stream closes), so callers can
   *  hold their handler open until the response is genuinely finished. */
  pipe: (writable: StreamWritableWeb | StreamWritableNode) => Promise<void>
  /** Get the response body as a Web ReadableStream */
  getReadableWebStream: () => StreamReadableWeb
  /** Get the response body as a Node.js Readable stream */
  getReadableNodeStream: () => StreamReadableNode
  /** Get the full response body as a string (awaits streaming if needed) */
  getBody: () => Promise<string>
  /** @deprecated Use `headers` instead */
  contentType: ContentType
  /** @deprecated Unused, always null */
  etag: string | null
  /** Error thrown by your telefunction */
  err?: unknown
}

type ContentType = 'text/plain' | 'application/octet-stream' | 'text/event-stream'
/** A response body is either a literal string or a concrete stream of the
 *  matching runtime (Node `Readable` when the adapter passed a Node
 *  `IncomingMessage`, Web `ReadableStream` otherwise). Producers commit to one
 *  at construction — `useNodeStream` flows down from `runTelefunc_` to both SSE
 *  and binary-inline — so consumers can pipe natively without a webstreams
 *  middleman on the hot path. */
type ResponseBody = string | StreamReadableNode | ReadableStream<Uint8Array<ArrayBuffer>>

function createHttpResponse({
  statusCode,
  contentType,
  headers,
  body: responseBody,
  err,
}: {
  statusCode: HttpResponse['statusCode']
  contentType: ContentType
  headers: [string, string][]
  body: ResponseBody
  err?: unknown
}): HttpResponse {
  headers.push(['Content-Type', contentType])

  return {
    statusCode,
    headers,
    get contentType() {
      assertWarning(false, 'httpResponse.contentType is deprecated, use httpResponse.headers instead.', {
        onlyOnce: true,
      })
      return contentType
    },
    get etag() {
      assertWarning(false, 'httpResponse.etag is deprecated and unused.', { onlyOnce: true })
      return null
    },
    get body() {
      assertUsage(
        typeof responseBody === 'string',
        "httpResponse.body can't be used for streaming responses. Use httpResponse.pipe() or httpResponse.getReadableWebStream() instead.",
      )
      return responseBody
    },
    pipe(writable: StreamWritableWeb | StreamWritableNode): Promise<void> {
      if (isStreamWritableWeb(writable)) return pipeToStreamWritableWeb(responseBody, writable)
      if (isStreamWritableNode(writable)) return pipeToStreamWritableNode(responseBody, writable)
      assertUsage(
        false,
        "The argument `writable` passed to `httpResponse.pipe(writable)` doesn't seem to be a Web WritableStream nor a Node.js Writable.",
      )
    },
    getReadableWebStream() {
      return getStreamReadableWeb(responseBody)
    },
    getReadableNodeStream() {
      return getStreamReadableNode(responseBody)
    },
    async getBody() {
      if (typeof responseBody === 'string') return responseBody
      const decoder = new TextDecoder()
      let result = ''
      // A `ReadableStream` is iterable on modern runtimes; the cast is just to
      // satisfy `lib.dom.d.ts` which doesn't declare `[Symbol.asyncIterator]`.
      for await (const chunk of responseBody as AsyncIterable<Uint8Array<ArrayBuffer>>) {
        result += decoder.decode(chunk, { stream: true })
      }
      result += decoder.decode()
      return result
    },
    err,
  }
}

function isStreamWritableWeb(writable: unknown): writable is StreamWritableWeb {
  return typeof WritableStream !== 'undefined' && writable instanceof WritableStream
}

function isStreamWritableNode(writable: unknown): writable is StreamWritableNode {
  if (isStreamWritableWeb(writable)) return false
  return hasProp(writable, 'write', 'function')
}

async function pipeToStreamWritableWeb(responseBody: ResponseBody, writable: StreamWritableWeb): Promise<void> {
  try {
    if (typeof responseBody === 'string') {
      const writer = writable.getWriter()
      writer.write(encodeForWebStream(responseBody))
      writer.close()
      return
    }
    // Body is typically a Web `ReadableStream` here (Web-adapter producer);
    // fall back to `Readable.toWeb` for the cross-runtime case where a Node
    // `Readable` body is piped to a Web `WritableStream`.
    if (responseBody instanceof ReadableStream) return void (await responseBody.pipeTo(writable))
    const { Readable } = getStreamNodeModule()
    await Readable.toWeb(responseBody).pipeTo(writable)
  } catch (err) {
    handleError(err)
  }
}

async function pipeToStreamWritableNode(responseBody: ResponseBody, writable: StreamWritableNode): Promise<void> {
  try {
    if (typeof responseBody === 'string') {
      writable.write(responseBody)
      writable.end()
      return
    }
    const { Readable, pipeline } = getStreamNodeModule()
    // Body is typically a Node `Readable` here (Node-adapter producer); fall
    // back to `Readable.fromWeb` for the cross-runtime case where a Web body
    // is piped to a Node `Writable`.
    const readable =
      responseBody instanceof ReadableStream
        ? Readable.fromWeb(responseBody as import('stream/web').ReadableStream)
        : responseBody
    await pipeline(readable, writable)
  } catch (error) {
    handleError(error)
  }
}

function getStreamReadableWeb(responseBody: ResponseBody): StreamReadableWeb {
  if (typeof responseBody === 'string') {
    return new ReadableStream({
      start(controller) {
        controller.enqueue(encodeForWebStream(responseBody))
        controller.close()
      },
    })
  }
  if (responseBody instanceof ReadableStream) return responseBody
  const { Readable } = getStreamNodeModule()
  return Readable.toWeb(responseBody) as ReadableStream<Uint8Array<ArrayBuffer>>
}

function getStreamReadableNode(responseBody: ResponseBody): StreamReadableNode {
  const { Readable } = getStreamNodeModule()
  if (typeof responseBody === 'string') return Readable.from(responseBody)
  if (responseBody instanceof ReadableStream) {
    return Readable.fromWeb(responseBody as import('stream/web').ReadableStream)
  }
  return responseBody
}

let encoder: TextEncoder
function encodeForWebStream(thing: unknown) {
  if (!encoder) {
    encoder = new TextEncoder()
  }
  if (typeof thing === 'string') {
    return encoder.encode(thing)
  }
  return thing
}

const shieldValidationError = {
  statusCode: STATUS_CODE_SHIELD_VALIDATION_ERROR,
  body: STATUS_BODY_SHIELD_VALIDATION_ERROR,
  contentType: 'text/plain' as const,
  headers: [] as [string, string][],
} as const

// HTTP Response for:
// - User's telefunction threw an error that isn't `Abort()` (i.e. the telefunction has a bug).
// - The `.telefunc.js` file exports a non-function value.
// - The Telefunc code threw an error (i.e. Telefunc has a bug).
const serverError = {
  statusCode: STATUS_CODE_INTERNAL_SERVER_ERROR,
  body: STATUS_BODY_INTERNAL_SERVER_ERROR,
  contentType: 'text/plain' as const,
  headers: [] as [string, string][],
} as const

// HTTP Response for:
// - Some non-telefunc client makes a malformed HTTP request.
// - The telefunction couldn't be found.
const malformedRequest = {
  statusCode: STATUS_CODE_MALFORMED_REQUEST,
  body: STATUS_BODY_MALFORMED_REQUEST,
  contentType: 'text/plain' as const,
  headers: [] as [string, string][],
} as const

async function runTelefunc(httpRequestResolved: Parameters<typeof runTelefunc_>[0]): Promise<HttpResponse> {
  // Eagerly load `node:stream` so all downstream code (`createPushReadable`,
  // pipe/get helpers, `getStreamReadableNode`) can read it synchronously.
  // No-op on subsequent calls and a silent no-op on Workers (load fails,
  // sync access guarded by `useNodeStream`).
  await loadStreamNodeModuleOnce()
  try {
    return await runTelefunc_(httpRequestResolved)
  } catch (err: unknown) {
    handleTelefunctionBug(err)
    return createHttpResponse({
      ...serverError,
      err,
    })
  }
}

async function runTelefunc_({
  request,
  readable,
  context,
}: {
  request: Request
  readable?: StreamReadableNode
  context?: Telefunc.Context
}): Promise<HttpResponse> {
  const requestContext = createRequestContext(request)
  const runContext = { requestContext }

  {
    // TO-DO/eventually: remove? Since `serverConfig` is global I don't think we need to set it to `runContext`, see for example https://github.com/brillout/telefunc/commit/5e3367d2d463b72e805e75ddfc68ef7f177a35c0
    const serverConfig = getServerConfig()
    objectAssign(runContext, {
      request,
      readable,
      serverConfig,
      appRootDir: serverConfig.root,
      telefuncFilesManuallyProvidedByUser: serverConfig.telefuncFiles,
    })
  }

  {
    const logMalformedRequests = !isProduction() /* || process.env.DEBUG.includes('telefunc') */
    objectAssign(runContext, { logMalformedRequests })
  }

  objectAssign(runContext, {
    context: {
      [PROVIDED_CONTEXT]: context,
      [REQUEST_CONTEXT]: requestContext,
    },
  })
  {
    const parsed = await parseHttpRequest(runContext)
    if (parsed.isMalformedRequest) {
      return createHttpResponse({ ...malformedRequest })
    }
    if (parsed.isSseRequest) {
      return createHttpResponse(parsed.sseResponse)
    }
    const { telefunctionKey, telefuncFilePath, telefunctionName, streamTransport, requestExtensions } = parsed
    objectAssign(runContext, {
      telefunctionKey,
      telefuncFilePath,
      telefunctionName,
      streamTransport,
      requestExtensions,
      reviveArgs: parsed.reviveArgs,
    })
  }

  {
    const { telefuncFilesLoaded, telefuncFilesAll } = await loadTelefuncFiles(runContext)
    assert(telefuncFilesLoaded, 'No `.telefunc.js` file found')
    objectAssign(runContext, { telefuncFilesLoaded, telefuncFilesAll })
  }

  {
    const telefunction = await findTelefunction(runContext)
    if (!telefunction) {
      return createHttpResponse({ ...malformedRequest })
    }
    objectAssign(runContext, { telefunction })
  }

  // Revive arguments now that we know the telefunction — per-value ServerReviverContext carries the
  // shield validators built from the telefunction's argumentShields metadata, so revivers have them
  // at revive time and can wire inline validation symmetric to the response-side replacers.
  objectAssign(runContext, { telefunctionArgs: runContext.reviveArgs(runContext.telefunction) })

  {
    const { isValidRequest } = applyShield(runContext)
    objectAssign(runContext, { isValidRequest })
    if (!isValidRequest) {
      objectAssign(runContext, {
        telefunctionAborted: true,
        telefunctionReturn: undefined,
      })
      return createHttpResponse({ ...shieldValidationError })
    }
  }

  {
    assert(runContext.isValidRequest)
    const { telefunctionReturn, telefunctionAborted, telefunctionHasErrored, telefunctionTopLevelError } =
      await executeTelefunction(runContext)
    objectAssign(runContext, {
      telefunctionReturn,
      telefunctionHasErrored,
      telefunctionAborted,
      telefunctionTopLevelError,
    })
  }

  // Resolve return shields into per-value bindings; serialization picks them up via the replacer context.
  if (!runContext.telefunctionHasErrored) {
    objectAssign(runContext, {
      valueShields: resolveReturnShields(runContext.telefunctionReturn, runContext.telefunction),
    })
  }

  if (runContext.telefunctionHasErrored || runContext.telefunctionAborted) {
    runContext.requestContext.markTopLevelError()
  }

  if (runContext.telefunctionHasErrored) {
    if (isShieldValidationError(runContext.telefunctionTopLevelError)) {
      return createHttpResponse({ ...shieldValidationError })
    }
    throw runContext.telefunctionTopLevelError
  }

  {
    objectAssign(runContext, {
      abortSignal: runContext.requestContext.abortSignal,
      useNodeStream: readable !== undefined,
    })
    const result = serializeTelefunctionResult(runContext)

    if (result.type === 'streaming') {
      const contentType =
        result.streamTransport === STREAM_TRANSPORT.SSE_INLINE ? 'text/event-stream' : 'application/octet-stream'
      return createHttpResponse({
        statusCode: runContext.telefunctionAborted ? STATUS_CODE_THROW_ABORT : STATUS_CODE_SUCCESS,
        contentType,
        headers: [
          ['Cache-Control', 'no-cache, no-transform'],
          ['X-Accel-Buffering', 'no'],
        ],
        body: result.body,
      })
    }

    objectAssign(runContext, { httpResponseBody: result.body })
  }

  return createHttpResponse({
    statusCode: runContext.telefunctionAborted ? STATUS_CODE_THROW_ABORT : STATUS_CODE_SUCCESS,
    contentType: 'text/plain',
    headers: [],
    body: runContext.httpResponseBody,
  })
}
