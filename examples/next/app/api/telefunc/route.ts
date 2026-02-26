import { telefunc, config } from 'telefunc'
import { getUser } from '../../../auth/getUser'

config.telefuncUrl = '/api/telefunc'

async function handler(request: Request) {
  const context = { user: getUser() }
  const httpResponse = await telefunc({ request, context })
  return new Response(httpResponse.body, {
    status: httpResponse.statusCode,
    headers: httpResponse.headers,
  })
}
export { handler as GET, handler as POST }
