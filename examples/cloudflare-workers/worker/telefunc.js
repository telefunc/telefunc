export { handleTelefunc }

import { serve, config } from 'telefunc'

config.disableNamingConvention = true

async function handleTelefunc(request) {
  const httpResponse = await serve({ request })
  return new Response(httpResponse.body, {
    headers: httpResponse.headers,
    status: httpResponse.statusCode,
  })
}
