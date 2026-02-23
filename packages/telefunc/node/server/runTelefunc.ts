export { runTelefunc }
export type { HttpResponse }

import { assert, assertUsage, assertWarning } from '../../utils/assert.js'
import { isProduction } from '../../utils/isProduction.js'
import { objectAssign } from '../../utils/objectAssign.js'
import { hasProp } from '../../utils/hasProp.js'
import { Telefunc } from './getContext.js'
import { loadTelefuncFiles } from './runTelefunc/loadTelefuncFiles.js'
import { parseHttpRequest } from './runTelefunc/parseHttpRequest.js'
// import { getEtag } from './runTelefunc/getEtag.js'
import { executeTelefunction } from './runTelefunc/executeTelefunction.js'
import { serializeTelefunctionResult } from './runTelefunc/serializeTelefunctionResult.js'
import { handleError } from './runTelefunc/handleError.js'
import { callBugListeners } from './runTelefunc/onBug.js'
import { applyShield } from './runTelefunc/applyShield.js'
import { findTelefunction } from './runTelefunc/findTelefunction.js'
import { getServerConfig } from './serverConfig.js'
import type { Readable as StreamReadableNode, Writable as StreamWritableNode } from 'node:stream'
import { import_ } from '@brillout/import'
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
  /** Pipe the response body to a Node.js or Web writable stream */
  pipe: (writable: StreamWritableWeb | StreamWritableNode) => void
  /** Get the response body as a Web ReadableStream */
  getReadableWebStream: () => StreamReadableWeb
  /** Get the response body as a Node.js Readable stream */
  getReadableNodeStream: () => Promise<StreamReadableNode>
  /** Get the full response body as a string (awaits streaming if needed) */
  getBody: () => Promise<string>
  /** @deprecated Use `headers` instead */
  contentType: 'text/plain' | 'application/octet-stream'
  /** @deprecated Unused, always null */
  etag: string | null
  /** Error thrown by your telefunction */
  err?: unknown
}

type ContentType = 'text/plain' | 'application/octet-stream'
type ResponseBody = string | ReadableStream<Uint8Array>

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
    pipe(writable: StreamWritableWeb | StreamWritableNode) {
      if (isStreamWritableWeb(writable)) {
        pipeToStreamWritableWeb(responseBody, writable)
        return
      }
      if (isStreamWritableNode(writable)) {
        pipeToStreamWritableNode(responseBody, writable)
        return
      }
      assertUsage(
        false,
        "The argument `writable` passed to `httpResponse.pipe(writable)` doesn't seem to be a Web WritableStream nor a Node.js Writable.",
      )
    },
    getReadableWebStream() {
      return getStreamReadableWeb(responseBody)
    },
    async getReadableNodeStream() {
      return getStreamReadableNode(responseBody)
    },
    async getBody() {
      if (typeof responseBody === 'string') {
        return responseBody
      }
      const reader = responseBody.getReader()
      const decoder = new TextDecoder()
      let result = ''
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        result += decoder.decode(value, { stream: true })
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

function pipeToStreamWritableWeb(responseBody: ResponseBody, writable: StreamWritableWeb): void {
  if (typeof responseBody === 'string') {
    const writer = writable.getWriter()
    writer.write(encodeForWebStream(responseBody))
    writer.close()
    return
  }
  responseBody.pipeTo(writable)
}

async function pipeToStreamWritableNode(responseBody: ResponseBody, writable: StreamWritableNode): Promise<void> {
  if (typeof responseBody === 'string') {
    writable.write(responseBody)
    writable.end()
    return
  }
  // Convert ReadableStream (Web) to Node.js Readable for piping
  const { Readable } = await loadStreamNodeModule()
  Readable.fromWeb(responseBody as import('stream/web').ReadableStream).pipe(writable)
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
  return responseBody
}

async function getStreamReadableNode(responseBody: ResponseBody): Promise<StreamReadableNode> {
  const { Readable } = await loadStreamNodeModule()
  if (typeof responseBody === 'string') {
    return Readable.from(responseBody)
  }
  return Readable.fromWeb(responseBody as import('stream/web').ReadableStream)
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

// Because of Cloudflare Workers, we cannot statically import the `stream` module, instead we dynamically import it.
async function loadStreamNodeModule(): Promise<{
  Readable: typeof StreamReadableNode
  Writable: typeof StreamWritableNode
}> {
  const streamModule = (await import_('stream')).default as Awaited<typeof import('stream')>
  const { Readable, Writable } = streamModule
  return { Readable, Writable }
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
  try {
    return await runTelefunc_(httpRequestResolved)
  } catch (err: unknown) {
    callBugListeners(err)
    handleError(err)
    return createHttpResponse({
      ...serverError,
      err,
    })
  }
}

async function runTelefunc_({
  request,
  context,
}: {
  request: Request
  context?: Telefunc.Context
}): Promise<HttpResponse> {
  const runContext = {}
  {
    // TO-DO/eventually: remove? Since `serverConfig` is global I don't think we need to set it to `runContext`, see for example https://github.com/brillout/telefunc/commit/5e3367d2d463b72e805e75ddfc68ef7f177a35c0
    const serverConfig = getServerConfig()
    objectAssign(runContext, {
      request,
      serverConfig: {
        disableNamingConvention: serverConfig.disableNamingConvention,
        telefuncUrl: serverConfig.telefuncUrl,
        log: serverConfig.log,
      },
      appRootDir: serverConfig.root,
      telefuncFilesManuallyProvidedByUser: serverConfig.telefuncFiles,
    })
  }

  {
    const logMalformedRequests = !isProduction() /* || process.env.DEBUG.includes('telefunc') */
    objectAssign(runContext, { logMalformedRequests })
  }

  objectAssign(runContext, {
    providedContext: context || null,
  })
  {
    const parsed = await parseHttpRequest(runContext)
    if (parsed.isMalformedRequest) {
      return createHttpResponse({ ...malformedRequest })
    }
    const { telefunctionKey, telefunctionArgs, telefuncFilePath, telefunctionName } = parsed
    objectAssign(runContext, {
      telefunctionKey,
      telefunctionArgs,
      telefuncFilePath,
      telefunctionName,
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
    const { telefunctionReturn, telefunctionAborted, telefunctionHasErrored, telefunctionError } =
      await executeTelefunction(runContext)
    objectAssign(runContext, {
      telefunctionReturn,
      telefunctionHasErrored,
      telefunctionAborted,
      telefunctionError,
    })
  }

  if (runContext.telefunctionHasErrored) {
    throw runContext.telefunctionError
  }

  {
    const result = serializeTelefunctionResult(runContext)
    if (result.type === 'streaming') {
      return createHttpResponse({
        statusCode: runContext.telefunctionAborted ? STATUS_CODE_THROW_ABORT : STATUS_CODE_SUCCESS,
        contentType: 'application/octet-stream',
        headers: [
          ['Cache-Control', 'no-cache, no-transform'],
          ['X-Accel-Buffering', 'no'],
        ],
        body: result.body,
      })
    }
    objectAssign(runContext, { httpResponseBody: result.body })
  }

  // {
  //   const httpResponseEtag = await getEtag(runContext)
  //   objectAssign(runContext, { httpResponseEtag })
  // }

  return createHttpResponse({
    statusCode: runContext.telefunctionAborted ? STATUS_CODE_THROW_ABORT : STATUS_CODE_SUCCESS,
    contentType: 'text/plain',
    headers: [],
    body: runContext.httpResponseBody,
  })
}
