import { Telefunc } from 'telefunc/cloudflare'

// Four session Durable Objects, so that two pages can be on two of them; each telefunction knows which it runs on.
const telefunc = new Telefunc({ scale: 4, context: (request) => ({ shard: request.headers.get('x-telefunc-shard') }) })

export const TelefuncDurableObject = telefunc.TelefuncDurableObject

export default {
  async fetch(request: Request, env: Env) {
    const response = await telefunc.serve({ request, env })
    if (response) return response

    return new Response('Not found', { status: 404 })
  },
}
