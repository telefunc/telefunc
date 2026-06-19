export { parseHttpRequest }

import { parse, type Reviver } from '@brillout/json-serializer/parse'
import { assertUsage, getProjectError, assert } from '../../../utils/assert.js'
import { getTelefunctionKey } from '../../../utils/getTelefunctionKey.js'
import { getUrlPathname } from '../../../utils/getUrlPathname.js'
import { hasProp } from '../../../utils/hasProp.js'
import { isProduction } from '../../../utils/isProduction.js'
import { createMultipartReviver } from '../../../shared/multipart/serializer-server.js'
import { FORM_DATA_MAIN_FIELD } from '../../../shared/multipart/constants.js'
import { MultipartReader } from '../multipart/MultipartReader.js'
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
  const isMultipart = contentType.includes('multipart/form-data')
  if (!isMultipart) {
    const text = await request.text()
    return parseTelefuncPayload(text, runContext)
  } else {
    assert(request.body)
    const boundary = getBoundary(contentType)
    assert(boundary, 'The multipart request is missing a boundary in the Content-Type header.')
    return parseMultipartBody(request.body, boundary, runContext)
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

// ===== Multipart parsing =====

async function parseMultipartBody(
  bodyStream: ReadableStream<Uint8Array>,
  boundary: string,
  runContext: { logMalformedRequests: boolean },
): Promise<ParseResult> {
  const reader = new MultipartReader(bodyStream, boundary)

  const metaText = await reader.readNextPartAsText(FORM_DATA_MAIN_FIELD)
  if (metaText === null) {
    logParseError(`The multipart request body is missing the ${FORM_DATA_MAIN_FIELD} field.`, runContext)
    return { isMalformedRequest: true }
  }

  const reviver = createMultipartReviver({
    createFile: (fileMetadata) => new LazyFile(reader, fileMetadata),
    createBlob: (blobMetadata) => new LazyBlob(reader, blobMetadata),
  })
  return parseTelefuncPayload(metaText, runContext, reviver)
}

// ===== Helpers =====

function getBoundary(contentType: string): string | null {
  const match = contentType.match(/boundary=(?:"([^"]+)"|([^\s;]+))/)
  return match ? match[1] || match[2] || null : null
}

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
    `serve({ url }): The pathname of \`url\` is \`${urlPathname}\` but it's expected to be \`${runContext.serverConfig.telefuncUrl}\`. Either make sure that \`url\` is the HTTP request URL, or set \`config.telefuncUrl\` to \`${urlPathname}\`.`,
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
