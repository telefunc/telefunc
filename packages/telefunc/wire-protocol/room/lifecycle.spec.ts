import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  DefaultBroadcastAdapter,
  KV_KEEP,
  _resetBroadcastAdapterForTesting,
  getBroadcastAdapter,
  type KvMutate,
  type KvReadOptions,
  type KvWriteOptions,
} from '../server/broadcast.js'
import { roomConfigKvKey, roomIndexKvKey } from './protocol.js'
import { Room } from './server.js'

// A two-tier broadcast adapter that models the Cloudflare hybrid: partitioned room state is written to
// a strongly-consistent authority and mirrored to an eventually-consistent replica; `{ consistent }`
// reads hit the authority, default reads hit the replica; unpartitioned keys (the room index) live on
// the replica only; `{ consistent }` writes (retained) stay authority-only. `freezeReplica()` holds the
// mirror so a test can drive the authority ahead of the replica — the lag the real KV exhibits. Pub/sub
// is inherited from the in-memory default; only the KV surface is overridden.
type Entry = { value: string; expiresAt: number | null }

class HybridTestAdapter extends DefaultBroadcastAdapter {
  readonly authority = new Map<string, Entry>()
  private readonly replica = new Map<string, Entry>()
  private frozen = false
  private deferred: Array<() => void> = []

  freezeReplica(): void {
    this.frozen = true
  }
  thawReplica(): void {
    this.frozen = false
    const pending = this.deferred
    this.deferred = []
    for (const fn of pending) fn()
  }

  private mirror(fn: () => void): void {
    if (this.frozen) this.deferred.push(fn)
    else fn()
  }

  private static read(store: Map<string, Entry>, key: string): string | null {
    const entry = store.get(key)
    if (!entry) return null
    if (entry.expiresAt !== null && entry.expiresAt <= Date.now()) {
      store.delete(key)
      return null
    }
    return entry.value
  }
  private static write(store: Map<string, Entry>, key: string, value: string, ttlMs: number | undefined): void {
    store.set(key, { value, expiresAt: ttlMs === undefined ? null : Date.now() + ttlMs })
  }
  private static list(store: Map<string, Entry>, prefix: string): string[] {
    const keys: string[] = []
    for (const key of [...store.keys()]) {
      if (key.startsWith(prefix) && HybridTestAdapter.read(store, key) !== null) keys.push(key)
    }
    return keys
  }

  get(key: string, options?: KvReadOptions): string | null {
    return HybridTestAdapter.read(options?.consistent ? this.authority : this.replica, key)
  }
  keys(prefix: string, options?: KvReadOptions): string[] {
    return HybridTestAdapter.list(options?.consistent ? this.authority : this.replica, prefix)
  }
  set(key: string, value: string, options?: KvWriteOptions): void {
    if (options?.partitionKey === undefined) {
      HybridTestAdapter.write(this.replica, key, value, options?.ttlMs)
      return
    }
    HybridTestAdapter.write(this.authority, key, value, options?.ttlMs)
    if (!options?.consistent) this.mirror(() => HybridTestAdapter.write(this.replica, key, value, options?.ttlMs))
  }
  delete(key: string, options?: KvReadOptions): void {
    if ((options as KvWriteOptions | undefined)?.partitionKey === undefined) {
      this.replica.delete(key)
      return
    }
    this.authority.delete(key)
    if (!options?.consistent) this.mirror(() => this.replica.delete(key))
  }
  setIfAbsent(key: string, value: string, options?: KvWriteOptions): boolean {
    if (HybridTestAdapter.read(this.authority, key) !== null) return false
    HybridTestAdapter.write(this.authority, key, value, options?.ttlMs)
    this.mirror(() => HybridTestAdapter.write(this.replica, key, value, options?.ttlMs))
    return true
  }
  update(key: string, mutate: KvMutate, options?: KvWriteOptions): string | null {
    const current = HybridTestAdapter.read(this.authority, key)
    const next = mutate(current)
    if (next === KV_KEEP) return current
    if (next === null) {
      this.authority.delete(key)
      this.mirror(() => this.replica.delete(key))
      return null
    }
    HybridTestAdapter.write(this.authority, key, next, options?.ttlMs)
    this.mirror(() => HybridTestAdapter.write(this.replica, key, next, options?.ttlMs))
    return next
  }
}

let hybrid: HybridTestAdapter
let previous: ReturnType<typeof getBroadcastAdapter>

beforeEach(() => {
  previous = getBroadcastAdapter()
  hybrid = new HybridTestAdapter()
  _resetBroadcastAdapterForTesting(hybrid)
})
afterEach(() => _resetBroadcastAdapterForTesting(previous))

function authorityConfig(id: string): { status: string; inc: string } | null {
  const raw = hybrid.authority.get(roomConfigKvKey(id))?.value
  return raw ? (JSON.parse(raw) as { status: string; inc: string }) : null
}

describe('Room lifecycle is authority-owned (regressions that fail on 00f838b)', () => {
  it('a join is rejected against a closed room even while the replica still reads it open', async () => {
    const room = await Room.create('la:stale-join')
    // The replica lags: it keeps the pre-close (open) config while the authority moves to closed.
    hybrid.freezeReplica()
    await Room.close('la:stale-join')
    expect(hybrid.get(roomConfigKvKey('la:stale-join'))).not.toBeNull() // replica still shows the room (stale)

    // The pre-close handle tries to join. On 00f838b `_assertOpen` trusted the replica and admitted it;
    // legality now reads the authority, so the join is rejected.
    await expect(room.join({ meta: { name: 'Alice' } })).rejects.toThrow(/closed/i)
    expect(authorityConfig('la:stale-join')?.status).toBe('closed')
  })

  it('setMeta on a closed room does not resurrect it, even through a stale-open replica read', async () => {
    await Room.create('la:resurrect')
    hybrid.freezeReplica()
    await Room.close('la:resurrect')

    // `requireRoom` reads the stale-open replica and gets past the prologue, but the config CAS runs on
    // the authority: on 00f838b a null authority value was upserted (resurrection); it is now dropped.
    await expect(Room.setMeta('la:resurrect', { topic: 'zombie' })).rejects.toThrow(/closed|not found/i)
    expect(authorityConfig('la:resurrect')?.status).toBe('closed') // still a tombstone, never re-opened
  })

  it('recreation is a fresh incarnation and carries none of the previous roster', async () => {
    const first = await Room.create('la:recreate')
    await first.join({ meta: { name: 'Alice' } })
    expect((await Room.getParticipants('la:recreate')).length).toBe(1)
    const firstInc = authorityConfig('la:recreate')?.inc
    expect(firstInc).toBeTruthy() // a real incarnation id, not a counter

    await Room.close('la:recreate')
    await Room.create('la:recreate') // takes over the closed tombstone

    expect(authorityConfig('la:recreate')?.status).toBe('open')
    // A distinct incarnation id — a counter could reuse `1` after the tombstone lapsed and let a stale
    // handle false-match; a random id never collides.
    expect(authorityConfig('la:recreate')?.inc).not.toBe(firstInc)
    expect(await Room.getParticipants('la:recreate')).toEqual([]) // Alice did not survive the close
  })

  it('getOrCreate repairs a room index that a partial create left behind', async () => {
    await Room.create('la:index')
    expect((await Room.list()).map((r) => r.id)).toContain('la:index')

    // Model a create that committed the authoritative config but crashed before the separate index
    // write: drop the index entry directly. The room is still reachable by id, just unlistable.
    await getBroadcastAdapter().delete!(roomIndexKvKey('la:index'))
    expect((await Room.list()).map((r) => r.id)).not.toContain('la:index')

    await Room.getOrCreate('la:index')
    expect((await Room.list()).map((r) => r.id)).toContain('la:index') // repaired idempotently
  })
})
