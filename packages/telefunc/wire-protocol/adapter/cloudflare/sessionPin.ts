/// <reference types="@cloudflare/workers-types" />
export { SessionPins, readSessionPin }

import { TELEFUNC_SESSION_HEADER, readForwardedTarget } from './routing.js'
import { assertWarning } from '../../../utils/assert.js'
import type { LocationBucket, SessionRoutingTarget } from './routing.js'

const SHARD_TOKEN_TTL_SECONDS = 86400
/** A session Durable Object writes its pin again once it's this old, well before KV lets it lapse. */
const SHARD_TOKEN_REPIN_MS = (SHARD_TOKEN_TTL_SECONDS * 1000) / 2
/** KV takes one write a second to a key. */
const SHARD_TOKEN_RETRY_MS = 1000
/** A token reaching a session Durable Object fits KV's 512-byte key, so two generations of this many hold under 10 MB of an isolate's 128 MB. */
const SHARD_TOKENS_GENERATION = 5_000

type StoredShardToken = {
  s: string
  b: LocationBucket
}

/** The session Durable Object and region KV pinned for `token`, if any. */
async function readSessionPin(
  kv: KVNamespace,
  token: string,
): Promise<Pick<SessionRoutingTarget, 'sessionInstanceName' | 'locationBucket'> | null> {
  const pin = await kv.get<StoredShardToken>(`session:${token}`, 'json')
  return pin && { sessionInstanceName: pin.s, locationBucket: pin.b }
}

/** The session Durable Object, not the Worker, writes each pin: a Worker's KV read caches a miss up to 60 s. */
class SessionPins {
  /** When each session token this isolate's Durable Objects served lately is due to be pinned. */
  #dueAt = new Map<string, number>()
  /** The generation `#dueAt` held before it filled, forgotten once `#dueAt` fills again. */
  #dueAtBefore = new Map<string, number>()

  /** Pins the session of `request`, as the Worker forwarded it, if its pin is due. */
  pin(request: Request, getKV: () => KVNamespace): void {
    const token = request.headers.get(TELEFUNC_SESSION_HEADER)
    if (!token) return
    const now = Date.now()
    const due = this.#dueAt.get(token) ?? this.#dueAtBefore.get(token)
    if (due !== undefined && now < due) {
      this.#recordDue(token, due)
      return
    }
    const target = readForwardedTarget(request)
    if (!target) return
    const kv = getKV()
    this.#recordDue(token, now + SHARD_TOKEN_REPIN_MS)
    const pin: StoredShardToken = { s: target.sessionInstanceName, b: target.locationBucket }
    // A Durable Object lives on while the write is pending.
    const written = kv.put(`session:${token}`, JSON.stringify(pin), { expirationTtl: SHARD_TOKEN_TTL_SECONDS })
    written.catch((err: unknown) => {
      this.#recordDue(token, Date.now() + SHARD_TOKEN_RETRY_MS)
      assertWarning(
        false,
        `A session's shard couldn't be pinned in KV, and is retried after a second: ${String(err)}`,
        { onlyOnce: false },
      )
    })
  }

  #recordDue(token: string, due: number): void {
    if (this.#dueAt.size >= SHARD_TOKENS_GENERATION) {
      this.#dueAtBefore = this.#dueAt
      this.#dueAt = new Map()
    }
    this.#dueAt.set(token, due)
  }
}
