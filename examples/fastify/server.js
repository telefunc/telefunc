import Fastify from 'fastify'
import { createServer as createViteServer } from 'vite'
import { config } from 'telefunc'
import { Telefunc } from 'telefunc/node'

// This example doesn't collocate `.telefunc.js` files next to UI components
config.disableNamingConvention = true

const fastify = Fastify({ logger: true })
const telefunc = new Telefunc()

fastify.register(async (instance) => {
  // Fastify parses the body by default (even for `*`, it keeps its built-in
  // `application/json`/`text/plain` parsers), which consumes the raw request
  // stream before Telefunc gets to read it. Disable that (scoped to this route
  // only) so Telefunc can read the raw stream itself (needed for streaming to work).
  instance.addContentTypeParser(['application/json', 'text/plain', '*'], (_request, _payload, done) => done(null))

  instance.all('/_telefunc', async (request, reply) => {
    // Take over the response: Telefunc writes directly to the raw Node.js `res`,
    // Fastify shouldn't send a response of its own.
    reply.hijack()
    await telefunc.serve({ req: request.raw, res: reply.raw })
  })
})

// Vite serves the frontend (and, in development, provides `.telefunc.js` files to Telefunc)
const vite = await createViteServer({ server: { middlewareMode: true }, appType: 'spa' })

// Fastify's exact route `/_telefunc` above takes precedence over this catch-all
fastify.get('/*', (request, reply) => {
  reply.hijack()
  vite.middlewares(request.raw, reply.raw, () => {
    reply.raw.statusCode = 404
    reply.raw.end('Not Found')
  })
})

const port = process.env.PORT || 3000
const address = await fastify.listen({ port })

// Enable WebSocket transport, so that Telefunc Stream can use it
telefunc.installWebSocket(fastify.server)

console.log(`Server running at ${address}`)
