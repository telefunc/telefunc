import { telefunc } from 'telefunc'
import type { RequestHandler } from './$types'

const handler: RequestHandler = async (event) => {
  const response = await telefunc({
    request: event.request,
    context: {
      // We pass the `context` object here, see https://telefunc.com/getContext
      someContext: 'hello',
    },
  })
  return new Response(response.body, {
    headers: response.headers,
    status: response.statusCode,
  })
}
export { handler as GET, handler as POST }
