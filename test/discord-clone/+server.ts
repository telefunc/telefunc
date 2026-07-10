import type { Server as HttpServer } from 'node:http'
import vike from '@vikejs/hono'
import { Hono } from 'hono'
import { getCookie } from 'hono/cookie'
import { config } from 'telefunc'
import { Telefunc } from 'telefunc/node'
import { installAuthRoutes } from './server/api'
import { getSessionUser, SESSION_COOKIE } from './server/auth'

config.shield = true
// Presence should notice a closed tab quickly (a participant lives as long as its holder's
// connection; the default gives a dropped connection 60s to reconnect before it "leaves").
config.channel.reconnectTimeout = 10_000

const tf = new Telefunc()
const app = new Hono()

installAuthRoutes(app)

// Every telefunction sees the caller through `getContext().user` — resolved here, from the
// session cookie, never trusted from the client (see server/context.d.ts for the typing).
app.all('/_telefunc', async (c) => {
  const user = getSessionUser(getCookie(c, SESSION_COOKIE))
  const response = await tf.serve({ request: c.req.raw, context: { user } })
  return response ?? c.text('Not found', 404)
})

vike(app)

export default {
  fetch: app.fetch,
  prod: {
    port: Number(process.env.PORT) || 3000,
    onCreate(server: { node?: { server?: HttpServer } }) {
      // Upgrade to WebSocket transport in production when possible (falls back to SSE).
      const httpServer = server?.node?.server
      if (httpServer) tf.installWebSocket(httpServer)
    },
  },
}
