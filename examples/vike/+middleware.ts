export { middlewareTelefunc as default }

import { enhance, type UniversalMiddleware } from '@universal-middleware/core'
import { telefunc } from 'telefunc'

const telefuncUniversalMiddleware: UniversalMiddleware = async (request, context, runtime) => {
  const httpResponse = await telefunc({
    request,
    context: {
      ...context,
      ...runtime,
    },
  })
  const { statusCode, headers } = httpResponse

  return new Response(httpResponse.getReadableWebStream(), {
    status: statusCode,
    headers,
  })
}

const middlewareTelefunc = enhance(telefuncUniversalMiddleware, {
  name: 'telefunc',
  method: 'POST',
  path: '/_telefunc',
})
