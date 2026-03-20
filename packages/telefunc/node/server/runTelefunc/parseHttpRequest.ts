export { parseHttpRequest }

import { parse, type Reviver } from '@brillout/json-serializer/parse'
import { assertUsage, getProjectError, assert } from '../../../utils/assert.js'
import { getTelefunctionKey } from '../../../utils/getTelefunctionKey.js'
import { getUrlPathname } from '../../../utils/getUrlPathname.js'
import { hasProp } from '../../../utils/hasProp.js'
import { isProduction } from '../../../utils/isProduction.js'
import { createRequestReviver } from '../../../wire-protocol/server/request/registry.js'
import { StreamReader } from '../../../wire-protocol/server/request/StreamReader.js'
import { REQUEST_KIND, getRequestKind } from '../../../wire-protocol/request-kind.js'
import type { RequestBodyReader } from '../../../wire-protocol/request-types.js'
import { STREAM_TRANSPORT, type StreamTransport } from '../../../wire-protocol/constants.js'
import { handleSseChannelRequest, type SseChannelHttpResponse } from '../../../wire-protocol/server/sse.js'

type ParseResult =
  | {
      telefuncFilePath: string
      telefunctionName: string
      telefunctionKey: string
      telefunctionArgs: unknown[]
      streamTransport: StreamTransport
      isSseRequest: false
      isMalformedRequest: false
    }
  | {
      isMalformedRequest: false
      isSseRequest: true
      sseResponse: SseChannelHttpResponse
    }
  | { isMalformedRequest: true }

async function parseHttpRequest(runContext: {
  request: Request
  logMalformedRequests: boolean
  serverConfig: {
    telefuncUrl: string
    stream: {
      transport: StreamTransport
    }
  }
}): Promise<ParseResult> {
  assertUrl(runContext)

  if (isWrongMethod(runContext)) {
    return { isMalformedRequest: true }
  }

  const { request } = runContext
  const requestKind = getRequestKind(request, runContext.serverConfig.telefuncUrl)
  switch (requestKind) {
    case REQUEST_KIND.MISMATCH:
      return { isMalformedRequest: true }
    case REQUEST_KIND.SSE: {
      const sseResponse = await handleSseChannelRequest(request)
      if (!sseResponse) return { isMalformedRequest: true }
      return { isMalformedRequest: false, isSseRequest: true, sseResponse }
    }
    case REQUEST_KIND.BINARY:
      assert(request.body)
      return parseBinaryFrameBody(request.body, runContext)
    case REQUEST_KIND.TEXT:
    case null: {
      const text = await request.text()
      const reviver = createRequestReviver(noopReader)
      return parseTelefuncPayload(text, runContext, reviver)
    }
  }
}

// ===== Main parsing =====

/** No-op reader used when there is no binary frame (plain JSON requests). */
const noopReader: RequestBodyReader = {
  registerFile: () => {},
  consumeFile: () => Promise.reject(new Error('No binary frame')),
}

/** Parse main payload, validate shape, and build a ParseResult. */
function parseTelefuncPayload(
  text: string,
  runContext: {
    logMalformedRequests: boolean
    serverConfig?: {
      stream: { transport: StreamTransport }
    }
  },
  reviver?: Reviver,
): ParseResult {
  let parsed: unknown
  try {
    parsed = parse(text, { reviver })
  } catch (err: unknown) {
    logParseError(
      [
        //
        "Telefunc request body couldn't be parsed.",
        !hasProp(err, 'message') ? null : `Parse error: ${err.message}.`,
      ]
        .filter(Boolean)
        .join(' '),
      runContext,
    )
    return { isMalformedRequest: true }
  }

  if (!hasProp(parsed, 'file', 'string') || !hasProp(parsed, 'name', 'string') || !hasProp(parsed, 'args', 'array')) {
    logParseError('Telefunc request body has unexpected content', runContext)
    return { isMalformedRequest: true }
  }

  const telefunctionKey = getTelefunctionKey(parsed.file, parsed.name)

  const streamTransport: StreamTransport =
    hasProp(parsed, 'stream', 'object') &&
    hasProp(parsed.stream, 'transport', 'string') &&
    (parsed.stream.transport === STREAM_TRANSPORT.BINARY_INLINE ||
      parsed.stream.transport === STREAM_TRANSPORT.SSE_INLINE ||
      parsed.stream.transport === STREAM_TRANSPORT.CHANNEL)
      ? parsed.stream.transport
      : (runContext.serverConfig?.stream.transport ?? STREAM_TRANSPORT.BINARY_INLINE)
  return {
    telefuncFilePath: parsed.file,
    telefunctionName: parsed.name,
    telefunctionKey,
    telefunctionArgs: parsed.args,
    streamTransport,
    isSseRequest: false,
    isMalformedRequest: false,
  }
}

// ===== Binary frame parsing =====

async function parseBinaryFrameBody(
  bodyStream: ReadableStream<Uint8Array>,
  runContext: { logMalformedRequests: boolean },
): Promise<ParseResult> {
  const reader = new StreamReader(bodyStream)
  const metaText = await reader.readMetadata()

  const reviver = createRequestReviver(reader)
  return parseTelefuncPayload(metaText, runContext, reviver)
}

// ===== Helpers =====

function isWrongMethod(runContext: { request: Request; logMalformedRequests: boolean }) {
  const { method } = runContext.request
  if (['POST', 'post'].includes(method)) {
    return false
  }
  assert(typeof method === 'string')
  logParseError(
    [
      //
      'The HTTP request method',
      'should be `POST` (or `post`) but',
      `\`method === '${method}'\`.`,
    ].join(' '),
    runContext,
  )
  return true
}

function assertUrl(runContext: { request: Request; serverConfig: { telefuncUrl: string } }) {
  const urlPathname = getUrlPathname(runContext.request.url)
  assertUsage(
    urlPathname === runContext.serverConfig.telefuncUrl,
    `telefunc({ url }): The pathname of \`url\` is \`${urlPathname}\` but it's expected to be \`${runContext.serverConfig.telefuncUrl}\`. Either make sure that \`url\` is the HTTP request URL, or set \`config.telefuncUrl\` to \`${urlPathname}\`.`,
  )
}

function logParseError(errMsg: string, runContext: { logMalformedRequests: boolean }) {
  const errMsgPrefix = 'Malformed request in development.'
  const errMsgSuffix =
    'This is unexpected since, in development, all requests are expected to originate from the Telefunc Client and should therefore be properly structured.'
  if (!isProduction()) {
    errMsg = `${errMsgPrefix} ${errMsg} ${errMsgSuffix}`
  }
  if (runContext.logMalformedRequests) {
    console.error(getProjectError(errMsg))
  }
}
