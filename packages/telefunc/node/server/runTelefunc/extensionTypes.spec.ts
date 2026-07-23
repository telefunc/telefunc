import { stringify } from '@brillout/json-serializer/stringify'
import { afterEach, describe, expect, it } from 'vitest'
import { STREAM_TRANSPORT } from '../../../wire-protocol/constants.js'
import type {
  ReplacerType,
  ReviverType,
  ServerReplacerContext,
  ServerReviverContext,
  TypeContract,
} from '../../../wire-protocol/types.js'
import { createRequestContext } from '../context/requestContext.js'
import { config, getServerConfig } from '../serverConfig.js'
import { parseHttpRequest } from './parseHttpRequest.js'
import { serializeTelefunctionResult } from './serializeTelefunctionResult.js'

const EXTENSION_NAME = 'late-extension-types-spec'
const REQUEST_PREFIX = '!LateRequestSpec:'
const RESPONSE_PREFIX = '!LateResponseSpec:'

const requestReviver: ReviverType<
  TypeContract<unknown, { revived: string }, { mark: string }>,
  ServerReviverContext
> = {
  prefix: REQUEST_PREFIX,
  revive(metadata) {
    return { value: { revived: metadata.mark }, close() {}, abort() {} }
  },
}

const responseReplacer: ReplacerType<
  TypeContract<{ __lateResponse: true; mark: string }, unknown, { mark: string }>,
  ServerReplacerContext
> = {
  prefix: RESPONSE_PREFIX,
  detect(value): value is { __lateResponse: true; mark: string } {
    return typeof value === 'object' && value !== null && '__lateResponse' in value
  },
  replace(value) {
    return { metadata: { mark: value.mark }, close() {}, abort() {} }
  },
}

function registerExtension() {
  config.extensions.push({
    name: EXTENSION_NAME,
    requestTypes: [requestReviver as ReviverType<TypeContract, ServerReviverContext>],
    responseTypes: [responseReplacer as ReplacerType<TypeContract, ServerReplacerContext>],
  })
}

afterEach(() => {
  const index = config.extensions.findIndex((extension) => extension.name === EXTENSION_NAME)
  if (index >= 0) config.extensions.splice(index, 1)
})

describe('extension wire types registered while a telefunc module loads', () => {
  it('transforms request values with requestTypes registered after routing', async () => {
    const marker = { __lateRequest: true, mark: 'request-mark' }
    const ordinaryPrefixedString = `${REQUEST_PREFIX}ordinary-string`
    const date = new Date('2026-07-23T00:00:00.000Z')
    const body = stringify(
      {
        file: '/spec/Late.telefunc.ts',
        name: 'onLate',
        args: [marker, ordinaryPrefixedString, date],
      },
      {
        replacer(_key, value, serializer) {
          if (value !== marker) return undefined
          return {
            replacement: REQUEST_PREFIX + serializer({ mark: marker.mark }),
            resolved: true,
          }
        },
      },
    )
    const request = new Request('http://localhost/_telefunc', { method: 'POST', body })
    const requestContext = createRequestContext(request)

    const parsed = await parseHttpRequest({
      request,
      requestContext,
      logMalformedRequests: false,
      serverConfig: getServerConfig(),
    })
    expect(parsed.isMalformedRequest).toBe(false)
    if (parsed.isMalformedRequest || parsed.isSseRequest) throw new Error('expected a telefunction request')

    registerExtension()
    const resolved = parsed.resolveRequest((() => undefined) as never)
    expect(resolved.isMalformedRequest).toBe(false)
    if (resolved.isMalformedRequest) throw new Error('expected a resolved request')

    expect(resolved.telefunctionArgs[0]).toEqual({ revived: 'request-mark' })
    expect(resolved.telefunctionArgs[1]).toBe(ordinaryPrefixedString)
    expect(resolved.telefunctionArgs[2]).toEqual(date)
  })

  it('serializes response values with responseTypes registered after the request config snapshot', () => {
    const requestStartConfig = getServerConfig()
    registerExtension()
    const requestContext = createRequestContext(new Request('http://localhost/_telefunc', { method: 'POST' }))

    const result = serializeTelefunctionResult({
      telefunctionReturn: { __lateResponse: true, mark: 'response-mark' },
      telefunctionName: 'onLate',
      telefuncFilePath: '/spec/Late.telefunc.ts',
      telefunctionAborted: false,
      context: {},
      requestContext,
      abortSignal: requestContext.abortSignal,
      streamTransport: STREAM_TRANSPORT.BINARY_INLINE,
      useNodeStream: false,
      serverConfig: requestStartConfig,
    })

    expect(result.type).toBe('text')
    if (result.type !== 'text') throw new Error('expected a text response')
    expect(result.body).toContain(RESPONSE_PREFIX)
  })
})
