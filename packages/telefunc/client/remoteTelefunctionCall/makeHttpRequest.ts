export { makeHttpRequest }

import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import { objectAssign } from '../../utils/objectAssign.js'
import { callOnAbortListeners } from './onAbort.js'
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

const method = 'POST'

async function makeHttpRequest(callContext: {
  telefuncUrl: string
  httpRequestBody: string | Blob
  telefunctionName: string
  telefuncFilePath: string
  httpHeaders: Record<string, string> | null
  fetch: typeof globalThis.fetch | null
}): Promise<{ telefunctionReturn: unknown }> {
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
    })
  } catch (_) {
    const telefunctionCallError = new Error('No Server Connection')
    objectAssign(telefunctionCallError, { isConnectionError: true as const })
    throw telefunctionCallError
  }

  const statusCode = response.status

  if (statusCode === STATUS_CODE_SUCCESS) {
    const { ret } = await parseResponseBody(response, callContext)
    const telefunctionReturn = ret
    return { telefunctionReturn }
  } else if (statusCode === STATUS_CODE_THROW_ABORT) {
    const { ret } = await parseResponseBody(response, callContext)
    const abortValue = ret
    const telefunctionCallError = new Error(
      `Aborted telefunction call ${callContext.telefunctionName}() (${callContext.telefuncFilePath}).`,
    )
    objectAssign(telefunctionCallError, { isAbort: true as const, abortValue })
    callOnAbortListeners(telefunctionCallError)
    throw telefunctionCallError
  } else if (statusCode === STATUS_CODE_INTERNAL_SERVER_ERROR) {
    const errMsg = await getErrMsg(STATUS_BODY_INTERNAL_SERVER_ERROR, response, callContext)
    throw new Error(errMsg)
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
  const responseBodyParsed = parse(responseBody)
  assertUsage(isObject(responseBodyParsed) && 'ret' in responseBodyParsed, wrongInstallation({ method, callContext }))
  assert(response.status !== STATUS_CODE_THROW_ABORT || 'abort' in responseBodyParsed)
  const { ret } = responseBodyParsed
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
