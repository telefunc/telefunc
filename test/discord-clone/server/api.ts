export { installAuthRoutes }

// Login/register/logout are plain HTTP routes (not telefunctions) because they set and clear
// the httpOnly session cookie — telefunctions don't touch response headers.

import { eq } from 'drizzle-orm'
import type { Context, Hono } from 'hono'
import { deleteCookie, getCookie, setCookie } from 'hono/cookie'
import { db } from '../database/db'
import { users } from '../database/schema'
import { createSession, deleteSession, registerUser, SESSION_COOKIE, verifyPassword } from './auth'

const NAME_RE = /^[a-z0-9_-]{2,24}$/i

function installAuthRoutes(app: Hono): void {
  app.post('/api/auth/register', async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as { name?: string; password?: string; color?: string }
    const name = (body.name ?? '').trim()
    const password = body.password ?? ''
    const color = /^#[0-9a-f]{6}$/i.test(body.color ?? '') ? body.color! : '#60a5fa'
    if (!NAME_RE.test(name)) return c.json({ error: 'Names are 2–24 letters, digits, - or _' }, 400)
    if (password.length < 8) return c.json({ error: 'Passwords need at least 8 characters' }, 400)
    if (db.select().from(users).where(eq(users.name, name)).limit(1).all().length > 0) {
      return c.json({ error: `"${name}" is taken` }, 400)
    }
    const user = registerUser(name, password, color)
    return sessionResponse(c, user)
  })

  app.post('/api/auth/login', async (c) => {
    const body = (await c.req.json().catch(() => ({}))) as { name?: string; password?: string }
    const row = db
      .select()
      .from(users)
      .where(eq(users.name, (body.name ?? '').trim()))
      .limit(1)
      .all()[0]
    if (!row || row.isBot || !verifyPassword(body.password ?? '', row.passwordHash)) {
      return c.json({ error: 'Wrong name or password' }, 401)
    }
    return sessionResponse(c, { id: row.id, name: row.name, color: row.color, isAdmin: row.isAdmin })
  })

  app.post('/api/auth/logout', (c) => {
    const token = getCookie(c, SESSION_COOKIE)
    if (token) deleteSession(token)
    deleteCookie(c, SESSION_COOKIE, { path: '/' })
    return c.json({ ok: true })
  })
}

function sessionResponse(c: Context, user: { id: string; name: string; color: string; isAdmin: boolean }) {
  const { token, expiresAt } = createSession(user.id)
  setCookie(c, SESSION_COOKIE, token, {
    path: '/',
    httpOnly: true,
    sameSite: 'Lax',
    expires: new Date(expiresAt),
  })
  return c.json({ user })
}
