/// <reference types="@cloudflare/workers-types" />
import { afterEach, describe, expect, it, vi } from 'vitest'
import { CloudflareBroadcastAuthorityState } from './broadcast.js'

// A minimal `DurableObjectState.storage`: a Map with the get/put/delete/list + alarm surface the
// room-state code uses. Strongly consistent and single-threaded, like the real thing — so this
// exercises the authority's room-state logic (atomicity, TTL, sweep) without a live Durable Object,
// the way the Redis fake exercises the Redis adapter.
function fakeState() {
  const store = new Map<string, unknown>()
  let alarm: number | null = null
  const storage = {
    async get(key: string) {
      return store.get(key)
    },
    async put(key: string, value: unknown) {
      store.set(key, value)
    },
    async delete(keyOrKeys: string | string[]) {
      for (const key of Array.isArray(keyOrKeys) ? keyOrKeys : [keyOrKeys]) store.delete(key)
    },
    async list({ prefix }: { prefix: string }) {
      const out = new Map<string, unknown>()
      for (const [key, value] of store) if (key.startsWith(prefix)) out.set(key, value)
      return out
    },
    async getAlarm() {
      return alarm
    },
    async setAlarm(time: number) {
      alarm = time
    },
    async deleteAlarm() {
      alarm = null
    },
  }
  return {
    state: { storage } as unknown as DurableObjectState,
    store,
    getAlarm: () => alarm,
    // The runtime clears a fired alarm before invoking `alarm()`; the handler re-arms if it needs to.
    clearAlarm: () => {
      alarm = null
    },
  }
}

// A minimal `KVNamespace`: the get/put/delete surface the write-through mirror uses, capturing each
// value's `expirationTtl` so a test can assert the TTL rode the mirror.
function fakeKv() {
  const kvStore = new Map<string, { value: string; expirationTtl?: number }>()
  const kv = {
    async get(key: string) {
      return kvStore.get(key)?.value ?? null
    },
    async put(key: string, value: string, options?: { expirationTtl?: number }) {
      kvStore.set(key, { value, expirationTtl: options?.expirationTtl })
    },
    async delete(key: string) {
      kvStore.delete(key)
    },
  }
  return { kv: kv as unknown as KVNamespace, kvStore }
}

function newAuthority() {
  const fake = fakeState()
  const { kv, kvStore } = fakeKv()
  return { authority: new CloudflareBroadcastAuthorityState(fake.state, kv), kvStore, ...fake }
}

describe('CloudflareBroadcastAuthorityState — room state (per-room DO storage)', () => {
  afterEach(() => vi.useRealTimers())

  it('round-trips values and lists them by prefix, stripping the storage namespace', async () => {
    const { authority } = newAuthority()

    expect(await authority.roomStateGet('telefunc:room:a:config')).toBe(null)
    await authority.roomStateSet('telefunc:room:a:config', '{"x":1}')
    expect(await authority.roomStateGet('telefunc:room:a:config')).toBe('{"x":1}')

    await authority.roomStateSet('telefunc:room:a:m:1', '{}')
    expect((await authority.roomStateKeys('telefunc:room:a:')).sort()).toEqual([
      'telefunc:room:a:config',
      'telefunc:room:a:m:1',
    ])

    await authority.roomStateDelete('telefunc:room:a:config')
    expect(await authority.roomStateGet('telefunc:room:a:config')).toBe(null)
  })

  it('setIfAbsent writes once — the first caller wins, the rest see it present', async () => {
    const { authority } = newAuthority()
    expect(await authority.roomStateSetIfAbsent('k', 'first')).toBe(true)
    expect(await authority.roomStateSetIfAbsent('k', 'second')).toBe(false)
    expect(await authority.roomStateGet('k')).toBe('first')
  })

  it('compareAndSet applies on a match, rejects on a mismatch, deletes on null', async () => {
    const { authority } = newAuthority()
    await authority.roomStateSet('k', 'v0')

    expect(await authority.roomStateCompareAndSet('k', 'WRONG', 'v1')).toBe(false)
    expect(await authority.roomStateGet('k')).toBe('v0')

    expect(await authority.roomStateCompareAndSet('k', 'v0', 'v1')).toBe(true)
    expect(await authority.roomStateGet('k')).toBe('v1')

    expect(await authority.roomStateCompareAndSet('k', 'v1', null)).toBe(true)
    expect(await authority.roomStateGet('k')).toBe(null)
  })

  it('mirrors replicated writes through to the KV read replica, and deletes drop them from it', async () => {
    const { authority, kvStore } = newAuthority()

    await authority.roomStateSet('telefunc:room:a:config', '{"x":1}')
    expect(kvStore.get('tfkv:telefunc:room:a:config')?.value).toBe('{"x":1}')

    expect(await authority.roomStateSetIfAbsent('telefunc:room:a:m:1', '{}')).toBe(true)
    expect(kvStore.get('tfkv:telefunc:room:a:m:1')?.value).toBe('{}')

    expect(await authority.roomStateCompareAndSet('telefunc:room:a:m:1', '{}', '{"seen":1}')).toBe(true)
    expect(kvStore.get('tfkv:telefunc:room:a:m:1')?.value).toBe('{"seen":1}')

    await authority.roomStateDelete('telefunc:room:a:config')
    expect(kvStore.has('tfkv:telefunc:room:a:config')).toBe(false)

    expect(await authority.roomStateCompareAndSet('telefunc:room:a:m:1', '{"seen":1}', null)).toBe(true)
    expect(kvStore.has('tfkv:telefunc:room:a:m:1')).toBe(false)
  })

  it('keeps authority-only writes off the replica — replicate: false never touches KV', async () => {
    const { authority, kvStore } = newAuthority()

    await authority.roomStateSet('telefunc:room:a:rt', 'frame', undefined, false)
    expect(await authority.roomStateGet('telefunc:room:a:rt')).toBe('frame') // held on the authority
    expect(kvStore.has('tfkv:telefunc:room:a:rt')).toBe(false) // never mirrored

    await authority.roomStateDelete('telefunc:room:a:rt', false)
    expect(await authority.roomStateGet('telefunc:room:a:rt')).toBe(null)
    expect(kvStore.size).toBe(0)
  })

  it('carries the entry TTL onto the replica mirror as a rounded, floored expirationTtl', async () => {
    const { authority, kvStore } = newAuthority()
    await authority.roomStateSet('m', 'v', 90_000) // 90s
    expect(kvStore.get('tfkv:m')?.expirationTtl).toBe(90)
    await authority.roomStateSet('m2', 'v', 500) // below the 60s floor → 60
    expect(kvStore.get('tfkv:m2')?.expirationTtl).toBe(60)
  })

  it('serializes concurrent creates through the authority chain — exactly one wins the race', async () => {
    const { authority } = newAuthority()
    const results = await Promise.all([
      authority.roomStateSetIfAbsent('k', 'A'),
      authority.roomStateSetIfAbsent('k', 'B'),
      authority.roomStateSetIfAbsent('k', 'C'),
    ])
    expect(results.filter(Boolean).length).toBe(1)
  })

  it('TTL entries expire lazily and are swept by the alarm; the alarm re-arms only while a live TTL remains', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(1_000_000)
    const { authority, getAlarm, clearAlarm, store } = newAuthority()

    await authority.roomStateSet('m1', 'a', 100_000) // expires at 1_100_000
    await authority.roomStateSet('m2', 'b', 300_000) // expires at 1_300_000
    await authority.roomStateSet('config', '{}') // no TTL — never expires
    expect(getAlarm()).not.toBe(null) // a TTL entry armed the sweep

    vi.setSystemTime(1_200_000) // m1 expired, m2 still live
    expect(await authority.roomStateGet('m1')).toBe(null) // lazily gone
    expect(await authority.roomStateGet('m2')).toBe('b')
    expect(await authority.roomStateKeys('')).toEqual(expect.not.arrayContaining(['m1']))

    clearAlarm() // the runtime clears the alarm before invoking the handler
    await authority.reapExpiredRoomState()
    expect([...store.keys()].some((key) => key.endsWith('m1'))).toBe(false) // physically swept
    expect([...store.keys()].some((key) => key.endsWith('m2'))).toBe(true) // kept — still live
    expect(getAlarm()).not.toBe(null) // m2 still has a live TTL → re-armed

    vi.setSystemTime(1_400_000) // m2 now expired too
    clearAlarm()
    await authority.reapExpiredRoomState()
    expect([...store.keys()].some((key) => key.endsWith('m2'))).toBe(false)
    expect(getAlarm()).toBe(null) // only the no-TTL config remains → the sweep lapses
  })
})
