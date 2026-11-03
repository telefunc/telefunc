import { Telefunc } from 'telefunc/cloudflare'

const telefunc = new Telefunc()

export const TelefuncDurableObject = telefunc.TelefuncDurableObject

export default {
  async fetch(request: Request, env: Env) {
    const response = await telefunc.serve({ request, env })
    if (response) return response

    return new Response('Not found', { status: 404 })
  },
}
