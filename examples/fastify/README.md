Example of using Telefunc with [Fastify](https://fastify.dev/), including [Telefunc Stream](https://telefunc.com/stream) (`hello.telefunc.js` is a plain RPC, `countdown.telefunc.js` streams values using an `async function*`).

Setup:
```bash
git clone git@github.com:telefunc/telefunc
cd telefunc/examples/fastify/
npm install
```

To develop:
```bash
npm run dev
```

Then open `http://localhost:3000`.

## Fastify integration

See `server.js`. The gist of it:

```js
import Fastify from 'fastify'
import { Telefunc } from 'telefunc/node'

const fastify = Fastify()
const telefunc = new Telefunc()

fastify.register(async (instance) => {
  // Fastify parses the body by default, which consumes the raw request stream
  // before Telefunc gets to read it. Disable that (scoped to this route only)
  // so Telefunc can read the raw stream itself (needed for streaming to work).
  instance.addContentTypeParser(['application/json', 'text/plain', '*'], (_request, _payload, done) => done(null))

  instance.all('/_telefunc', async (request, reply) => {
    // Fastify wraps Node's req/res: use the raw objects and hijack the reply
    // so that Fastify doesn't try to send its own response afterwards.
    reply.hijack()
    await telefunc.serve({ req: request.raw, res: reply.raw })
  })
})

const address = await fastify.listen({ port: 3000 })

// Enable WebSocket transport, so that Telefunc Stream can use it
telefunc.installWebSocket(fastify.server)
```

See <https://telefunc.com/Telefunc> and <https://telefunc.com/stream>.
