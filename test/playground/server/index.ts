export default startServer()

import { Hono } from 'hono'
import { apply, serve } from '@photonjs/hono'
import { cleanupState, resetCleanupState } from '../pages/abort/cleanup-state'

function startServer() {
  const app = new Hono()

  // Test-only API: query/reset server-side cleanup state
  app.get('/api/cleanup-state', (c) => c.json(cleanupState))
  app.post('/api/cleanup-state/reset', (c) => {
    resetCleanupState()
    return c.json({ ok: true })
  })

  apply(app)
  return serve(app, { port: 3000 })
}
