import { expect, test } from 'vitest'
import { stringify } from '@brillout/json-serializer/stringify'
import { parseHttpRequest } from './parseHttpRequest.js'
import { createRequestContext } from '../context/requestContext.js'
import { getServerConfig } from '../serverConfig.js'
import { getChannelMux } from '../../../wire-protocol/server/mux.js'
import { SERIALIZER_PREFIX_FUNCTION } from '../../../wire-protocol/constants.js'

test("a callback's call whose channels closed leaves no scan timer, which would keep a Durable Object from hibernating (#469)", async () => {
  const callback = { channelId: crypto.randomUUID() }
  const body = stringify(
    { file: '/pages/Upload.telefunc.ts', name: 'onUpload', args: [callback] },
    {
      replacer: (_key, value, serializer) =>
        value === callback
          ? { replacement: SERIALIZER_PREFIX_FUNCTION + serializer({ channelId: callback.channelId }), resolved: true }
          : undefined,
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
  if (parsed.isMalformedRequest || parsed.isSseRequest) throw new Error('expected a telefunction request')
  const resolved = parsed.resolveRequest((() => undefined) as never)
  if (resolved.isMalformedRequest) throw new Error('expected a resolved request')
  const registry = getChannelMux().gcRegistry as unknown as { scanTimer: unknown }
  expect(registry.scanTimer).not.toBe(null)

  requestContext.markComplete() // the response went out
  getChannelMux()['channels'].get(callback.channelId)!._onPeerClose() // the page left
  expect(registry.scanTimer).toBe(null)
  expect(resolved.telefunctionArgs[0]).toBeTypeOf('function') // the telefunction still holds it
})
