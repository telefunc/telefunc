/// <reference types="@cloudflare/workers-types" />
export { SessionPins, readSessionPin }

import { TELEFUNC_BROADCAST_BUCKET_HEADER, TELEFUNC_SESSION_HEADER, TELEFUNC_SHARD_HEADER } from './routing.js'
import { assertWarning } from '../../../../utils/assert.js'
import type { LocationBucket, SessionRoutingTarget } from './routing.js'

const SHARD_TOKEN_TTL_SECONDS = 86400
/** A session Durable Object writes its pin again once it's this old, well before KV lets it lapse. */
const SHARD_TOKEN_REPIN_MS = (SHARD_TOKEN_TTL_SECONDS * 1000) / 2
/** KV takes one write a second to a key. */
const SHARD_TOKEN_RETRY_MS = 1000

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

/** The Worker routes a token by its KV pin. The session Durable Object writes the pin, once and again before it lapses:
 *  a Worker reads KV through a cache that keeps a miss up to 60 s, so it would write on every request of a page's first
 *  minute. */
class SessionPins {
  /** When each session token this isolate's Durable Objects serve is due to be pinned, in the order they were pinned. */
  private readonly dueAt = new Map<string, number>()

  /** Pins the session of `request`, as the Worker forwarded it, if its pin is due. */
  pin(request: Request, kv: KVNamespace): void {
    const token = request.headers.get(TELEFUNC_SESSION_HEADER)
    if (!token) return
    const now = Date.now()
    const due = this.dueAt.get(token)
    if (due !== undefined && now < due) return
    const shard = request.headers.get(TELEFUNC_SHARD_HEADER)
    const bucket = request.headers.get(TELEFUNC_BROADCAST_BUCKET_HEADER) as LocationBucket | null
    if (!shard || !bucket) return
    this.dueAt.delete(token)
    this.dueAt.set(token, now + SHARD_TOKEN_REPIN_MS)
    for (const [oldToken, oldDue] of this.dueAt) {
      if (now < oldDue) break
      this.dueAt.delete(oldToken)
    }
    const pin: StoredShardToken = { s: shard, b: bucket }
    // A Durable Object lives on while the write is pending.
    const written = kv.put(`session:${token}`, JSON.stringify(pin), { expirationTtl: SHARD_TOKEN_TTL_SECONDS })
    written.catch((err: unknown) => {
      this.dueAt.set(token, Date.now() + SHARD_TOKEN_RETRY_MS)
      assertWarning(
        false,
        `A session's shard couldn't be pinned in KV, and is retried after a second: ${String(err)}`,
        { onlyOnce: false },
      )
    })
  }
}
