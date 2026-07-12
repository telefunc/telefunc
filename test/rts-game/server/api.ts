export { installIdentityRoutes }

import type { Hono } from 'hono'
import { getCookie, setCookie } from 'hono/cookie'
import { createSession, getSessionUser, SESSION_COOKIE } from './identity'

// Sign-in / sign-out are plain HTTP (not telefunctions): they *set* the session cookie that every
// later telefunction reads through `getContext().user`. A telefunction can't set its own auth
// cookie, so the entry point stays here.
function installIdentityRoutes(app: Hono): void {
  app.post('/api/session', async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as { name?: string }
    try {
      const { user, cookie } = createSession(body.name ?? '')
      setCookie(c, SESSION_COOKIE, cookie, {
        httpOnly: true,
        sameSite: 'Lax',
        path: '/',
        maxAge: 60 * 60 * 24 * 7,
      })
      return c.json({ user })
    } catch {
      return c.json({ error: 'Please pick a name (letters, numbers, spaces; up to 20 chars).' }, 400)
    }
  })

  app.get('/api/session', (c) => {
    return c.json({ user: getSessionUser(getCookie(c, SESSION_COOKIE)) })
  })

  app.post('/api/logout', (c) => {
    setCookie(c, SESSION_COOKIE, '', { httpOnly: true, path: '/', maxAge: 0 })
    return c.json({ ok: true })
  })
}
