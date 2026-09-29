export type { SessionUser }
export { createSession, getSessionUser, colorForName, SESSION_COOKIE }

import { createHmac, randomUUID, timingSafeEqual } from 'node:crypto'

// Lightweight identity: an RTS player picks a name and plays — no passwords, no database. The
// name mints a signed, httpOnly session cookie that carries the durable app identity (`id`),
// which every room `join()` is stamped with. Reconnecting after a reload re-reads this cookie,
// so the same `identity` re-enters the match and reclaims its army.

type SessionUser = {
  id: string
  name: string
  color: string
}

const SESSION_COOKIE = 'rts_session'
// A test app: a fixed dev secret is fine (override with RTS_SECRET). It only signs the identity
// cookie against tampering — there are no privileges to escalate to.
const SECRET = process.env.RTS_SECRET || 'telefront-dev-secret'

// Distinct, readable player colors (used for chat/lobby avatars; team color dominates in-match).
const PALETTE = [
  '#f2a541',
  '#7cc4ff',
  '#8be08a',
  '#ff8fab',
  '#c792ea',
  '#ffd166',
  '#4ecdc4',
  '#f78c6b',
  '#a3d977',
  '#e0aaff',
]

function colorForName(name: string): string {
  let hash = 0
  for (let i = 0; i < name.length; i++) hash = (hash * 31 + name.charCodeAt(i)) | 0
  return PALETTE[Math.abs(hash) % PALETTE.length]
}

function sign(payload: string): string {
  return createHmac('sha256', SECRET).update(payload).digest('base64url')
}

function createSession(rawName: string): { user: SessionUser; cookie: string } {
  const name = rawName.trim().slice(0, 20)
  if (!/^[\w \-]{1,20}$/.test(name)) throw new Error('Invalid name')
  const user: SessionUser = { id: randomUUID(), name, color: colorForName(name) }
  const payload = Buffer.from(JSON.stringify(user)).toString('base64url')
  const cookie = `${payload}.${sign(payload)}`
  return { user, cookie }
}

function getSessionUser(cookie: string | undefined): SessionUser | null {
  if (!cookie) return null
  const dot = cookie.lastIndexOf('.')
  if (dot < 0) return null
  const payload = cookie.slice(0, dot)
  const expected = sign(payload)
  const got = cookie.slice(dot + 1)
  if (expected.length !== got.length || !timingSafeEqual(Buffer.from(expected), Buffer.from(got))) return null
  try {
    const user = JSON.parse(Buffer.from(payload, 'base64url').toString()) as SessionUser
    if (typeof user?.id === 'string' && typeof user?.name === 'string') return user
  } catch {}
  return null
}
