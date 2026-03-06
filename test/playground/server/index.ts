import { Hono } from 'hono'
import { apply, serve } from '@photonjs/hono'
import { telefuncWebSocket } from 'telefunc/websocket/node'
import { cleanupState, resetCleanupState } from '../cleanup-state'

function startServer() {
  const app = new Hono()

  app.get('/api/cleanup-state', (c) => c.json(cleanupState))
  app.post('/api/cleanup-state/reset', (c) => {
    resetCleanupState()
    return c.json({ ok: true })
  })

  apply(app)
  return serve(app, {
    port: 3000,
    onCreate(server) {
      const ws = telefuncWebSocket()
      // @photonjs/hono's serve() returns a srvx NodeServer, not a plain http.Server.
      // The actual Node.js http.Server is at .node.server.
      // @ts-expect-error srvx types not exposed
      ws.install(server.node.server)
    },
  })
}

export default startServer()
