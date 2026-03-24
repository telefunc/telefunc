export { makeHttpRequest }

import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import { parseResponse } from '../../wire-protocol/client/response/parse.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl } from '../../wire-protocol/request-kind.js'
import { throwAbortError, throwBugError } from './errors.js'
import { ConnectionError } from '../ConnectionError.js'
import { setShardInfo } from '../../wire-protocol/client/shard-registry.js'
import type { ChannelTransports } from '../../wire-protocol/constants.js'
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

const method = 'POST'

async function makeHttpRequest(callContext: {
  telefuncUrl: string
  telefuncUrlBase: string
  httpRequestBody: string | Blob
  telefunctionName: string
  telefuncFilePath: string
  headers: Record<string, string> | null
  fetch: typeof globalThis.fetch | null
  abortController: AbortController
  channel: { transports: ChannelTransports }
}): Promise<unknown> {
  const isBinaryFrame = typeof callContext.httpRequestBody !== 'string'
  const requestKind = isBinaryFrame ? REQUEST_KIND.BINARY : REQUEST_KIND.TEXT
  const requestUrl = getMarkedRequestUrl(callContext.telefuncUrl, requestKind)
  const contentType = isBinaryFrame ? { 'Content-Type': 'application/octet-stream' } : { 'Content-Type': 'text/plain' }
  const requestKindHeader = { [REQUEST_KIND_HEADER]: requestKind }
  let response: Response
  try {
    const fetch = callContext.fetch ?? window.fetch
    response = await fetch(requestUrl, {
      method,
      body: callContext.httpRequestBody,
      credentials: 'same-origin',
      headers: {
        ...contentType,
        ...requestKindHeader,
        ...callContext.headers,
      },
      signal: callContext.abortController.signal,
    })
  } catch (err) {
    if (callContext.abortController.signal.aborted) {
      throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, undefined)
    }
    throw new ConnectionError()
  }

  const statusCode = response.status
  const shard = response.headers.get('x-telefunc-shard') ?? undefined

  if (shard) setShardInfo(callContext.telefuncUrlBase, shard)

  if (statusCode === STATUS_CODE_SUCCESS) {
    const parsed = await parseResponse(response, callContext, shard)
    assertUsage(isObject(parsed) && 'ret' in parsed, wrongInstallation({ method, callContext }))
    return parsed.ret
  } else if (statusCode === STATUS_CODE_THROW_ABORT) {
    const responseBody = await response.text()
    const parsed: unknown = parse(responseBody)
    assertUsage(isObject(parsed) && 'ret' in parsed, wrongInstallation({ method, callContext }))
    assert('abort' in parsed)
    throwAbortError(callContext.telefunctionName, callContext.telefuncFilePath, (parsed as { ret: unknown }).ret)
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
