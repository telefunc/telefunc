import type { Server as HttpServer } from 'node:http'
import vike from '@vikejs/hono'
import { Hono } from 'hono'
import { getCookie } from 'hono/cookie'
import { config } from 'telefunc'
import { Telefunc } from 'telefunc/node'
import { installIdentityRoutes } from './server/api'
import { getSessionUser, SESSION_COOKIE } from './server/identity'

config.shield = true
// Presence should notice a dropped tab within a few seconds so the lobby roster and the in-match
// player list stay honest. Match state itself is keyed by `identity`, not by participant, so a
// reload within this window rejoins the same army regardless (see server/sim/match.ts).
config.channel.reconnectTimeout = 15_000

const tf = new Telefunc()
const app = new Hono()

installIdentityRoutes(app)

// Every telefunction sees the caller through `getContext().user`, resolved here from the signed
// session cookie — never trusted from the client (see server/context.d.ts for the typing).
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
      // Upgrade to the WebSocket transport in production when possible (falls back to SSE). The
      // match's 10 Hz binary state broadcast and per-order command sends both ride this lane.
      const httpServer = server?.node?.server
      if (httpServer) tf.installWebSocket(httpServer)
      // The e2e harness waits for this line to know the preview server is up.
      console.log(`Listening on http://localhost:${Number(process.env.PORT) || 3000}`)
    },
  },
}
