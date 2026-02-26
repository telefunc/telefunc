export { handleTelefunc }

import { telefunc, config } from 'telefunc'

config.disableNamingConvention = true

async function handleTelefunc(request) {
  const httpResponse = await telefunc({ request })
  return new Response(httpResponse.body, {
    headers: httpResponse.headers,
    status: httpResponse.statusCode,
  })
}
