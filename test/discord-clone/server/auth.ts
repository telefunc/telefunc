export { createSession, deleteSession, getSessionUser, hashPassword, registerUser, SESSION_COOKIE, verifyPassword }
export type { SessionUser }

import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from 'node:crypto'
import * as q from '../database/queries'

/** What `getContext().user` carries into every telefunction. */
type SessionUser = { id: string; name: string; color: string; isAdmin: boolean }

const SESSION_COOKIE = 'session'
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000 // 30 days

// --- Passwords (scrypt, no extra dependency) ---

function hashPassword(password: string): string {
  const salt = randomBytes(16).toString('hex')
  return `${salt}:${scryptSync(password, salt, 64).toString('hex')}`
}

function verifyPassword(password: string, stored: string): boolean {
  const [salt, hash] = stored.split(':')
  if (!salt || !hash) return false
  return timingSafeEqual(scryptSync(password, salt, 64), Buffer.from(hash, 'hex'))
}

// --- Users ---

function registerUser(name: string, password: string, color: string): SessionUser {
  // The first human to register owns the server.
  const isFirst = q.countHumans() === 0
  q.insertUser({
    id: randomUUID(),
    name,
    color,
    password_hash: hashPassword(password),
    is_admin: isFirst ? 1 : 0,
    is_bot: 0,
    created_at: Date.now(),
  })
  const user = q.getUserByName(name)!
  return { id: user.id, name: user.name, color: user.color, isAdmin: user.is_admin === 1 }
}

// --- Sessions ---

function createSession(userId: string): { token: string; expiresAt: number } {
  const token = randomBytes(32).toString('hex')
  const expiresAt = Date.now() + SESSION_TTL_MS
  q.insertSession(token, userId, expiresAt)
  return { token, expiresAt }
}

function deleteSession(token: string): void {
  q.deleteSession(token)
}

function getSessionUser(token: string | undefined): SessionUser | null {
  if (!token) return null
  const user = q.getSessionUser(token, Date.now())
  if (user === undefined) return null
  return { id: user.id, name: user.name, color: user.color, isAdmin: user.is_admin === 1 }
}
