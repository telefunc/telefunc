import { apply, serve } from '@photonjs/hono'
import { Hono } from 'hono'
import { config } from 'telefunc'
import { telefuncWebSocket } from 'telefunc/websocket/node'
import { cleanupState, resetCleanupState } from '../cleanup-state'
config.channel = {
  pingInterval: 1000,
}

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
      // @ts-expect-error srvx types not exposed
      ws.install(server.node.server)
    },
  })
}

export default startServer()
