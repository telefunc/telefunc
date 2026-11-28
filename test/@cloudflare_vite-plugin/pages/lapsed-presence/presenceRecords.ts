export { LAPSED_PRESENCE_REFUSED_FOR_MS, lapsedPresences }

/** Test-only: how long a `lapsed-presence:` key's authority refuses its subscriber's refreshes under the lease it first
 *  recorded: past the presence's 90 s (`LEASE_TTL_MS`), so that lease lapses (see +server.ts). */
const LAPSED_PRESENCE_REFUSED_FOR_MS = 100_000

/** Test-only: each `lapsed-presence:` key's first recorded presence: its lease, when it was recorded, and whether a
 *  later hold under that lease recorded it again once the refusals ended (see +server.ts). */
const lapsedPresences = new Map<string, { lease: string; recordedAt: number; rerecorded: boolean }>()
