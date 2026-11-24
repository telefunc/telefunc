export { LAPSED_PRESENCE_REFUSED_FOR_MS, rerecordedPresences }

/** Test-only: how long a `lapsed-presence:` key's authority refuses its subscriber's refreshes, from the first time it
 *  records the subscriber's presence: past the presence's 90 s (`LEASE_TTL_MS`), so it lapses (see +server.ts). */
const LAPSED_PRESENCE_REFUSED_FOR_MS = 100_000

/** Test-only: the `lapsed-presence:` keys whose subscriber's presence the key's authority recorded again, by a hold it
 *  took once it stopped refusing the subscriber's refreshes (see +server.ts). */
const rerecordedPresences = new Set<string>()
