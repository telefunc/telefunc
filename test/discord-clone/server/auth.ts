export { createSession, deleteSession, getSessionUser, hashPassword, registerUser, SESSION_COOKIE, verifyPassword }
export type { SessionUser }

import { randomBytes, randomUUID, scryptSync, timingSafeEqual } from 'node:crypto'
import { and, eq, gt } from 'drizzle-orm'
import { db } from '../database/db'
import { sessions, users } from '../database/schema'

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
  const isFirst = db.select({ id: users.id }).from(users).where(eq(users.isBot, false)).limit(1).all().length === 0
  const user = {
    id: randomUUID(),
    name,
    color,
    passwordHash: hashPassword(password),
    isAdmin: isFirst,
    isBot: false,
    createdAt: Date.now(),
  }
  db.insert(users).values(user).run()
  return { id: user.id, name: user.name, color: user.color, isAdmin: user.isAdmin }
}

// --- Sessions ---

function createSession(userId: string): { token: string; expiresAt: number } {
  const token = randomBytes(32).toString('hex')
  const expiresAt = Date.now() + SESSION_TTL_MS
  db.insert(sessions).values({ token, userId, expiresAt }).run()
  return { token, expiresAt }
}

function deleteSession(token: string): void {
  db.delete(sessions).where(eq(sessions.token, token)).run()
}

function getSessionUser(token: string | undefined): SessionUser | null {
  if (!token) return null
  const rows = db
    .select({ id: users.id, name: users.name, color: users.color, isAdmin: users.isAdmin })
    .from(sessions)
    .innerJoin(users, eq(sessions.userId, users.id))
    .where(and(eq(sessions.token, token), gt(sessions.expiresAt, Date.now())))
    .limit(1)
    .all()
  return rows[0] ?? null
}
