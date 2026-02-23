export { parseHttpRequest }

import { parse, type Reviver } from '@brillout/json-serializer/parse'
import { assertUsage, getProjectError, assert } from '../../../utils/assert.js'
import { getTelefunctionKey } from '../../../utils/getTelefunctionKey.js'
import { getUrlPathname } from '../../../utils/getUrlPathname.js'
import { hasProp } from '../../../utils/hasProp.js'
import { isProduction } from '../../../utils/isProduction.js'
import { createFileReviver } from '../../../shared/wire-protocol/reviver-request.js'
import { StreamReader } from '../multipart/StreamReader.js'
import { LazyBlob, LazyFile } from '../multipart/LazyFile.js'

type ParseResult =
  | {
      telefuncFilePath: string
      telefunctionName: string
      telefunctionKey: string
      telefunctionArgs: unknown[]
      isMalformedRequest: false
    }
  | { isMalformedRequest: true }

async function parseHttpRequest(runContext: {
  request: Request
  logMalformedRequests: boolean
  serverConfig: {
    telefuncUrl: string
  }
}): Promise<ParseResult> {
  assertUrl(runContext)

  if (isWrongMethod(runContext)) {
    return { isMalformedRequest: true }
  }

  const { request } = runContext
  const contentType = request.headers.get('content-type') || ''
  const isBinaryFrame = contentType.includes('application/octet-stream')
  if (!isBinaryFrame) {
    const text = await request.text()
    return parseTelefuncPayload(text, runContext)
  } else {
    assert(request.body)
    return parseBinaryFrameBody(request.body, runContext)
  }
}

// ===== Main parsing =====

/** Parse main payload, validate shape, and build a ParseResult. */
function parseTelefuncPayload(
  text: string,
  runContext: { logMalformedRequests: boolean },
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
  return {
    telefuncFilePath: parsed.file,
    telefunctionName: parsed.name,
    telefunctionKey,
    telefunctionArgs: parsed.args,
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

  const reviver = createFileReviver({
    createFile: (fileMetadata) => new LazyFile(reader, fileMetadata),
    createBlob: (blobMetadata) => new LazyBlob(reader, blobMetadata),
  })
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
