import { Hono } from 'hono'
import { apply, serve } from '@photonjs/hono'
import { cleanupState, resetCleanupState } from '../cleanup-state'

function startServer() {
  const app = new Hono()

  app.get('/api/cleanup-state', (c) => c.json(cleanupState))
  app.post('/api/cleanup-state/reset', (c) => {
    resetCleanupState()
    return c.json({ ok: true })
  })

  apply(app)
  return serve(app, { port: 3000 })
}

export default startServer()
