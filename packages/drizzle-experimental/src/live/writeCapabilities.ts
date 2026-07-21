export { oldNewReturningOf, oldNewProvenOf, markOldNewProven, demoteOldNewReturning, probeOldNewReturning }

import { type SQL, sql } from 'drizzle-orm'
import { dialectOf, rowRunnerFor } from '../binding/database.js'
import { causeChain } from '../utils/causeChain.js'

type AnyDb = { dialect?: unknown; $client?: unknown }

const str = (value: unknown): string => (value == null ? '' : String(value))

// ── Returned-image capability ───────────────────────────────────────

// Whether this connection can return BOTH images of a changed row in the write statement itself
// (`RETURNING old.*, new.*`, PostgreSQL 18 and up). With it, an update that moves a primary key, and an
// update a stateless live query has to decide membership for, are exact with no extra round trip.
//
// This is a CAPABILITY probe, not a version check — a version number is a claim about a server, and the
// question here is what THIS connection accepts. Version alone is exactly what PostgreSQL-compatible forks
// break. So the statement is actually run: against a temp table, inside a transaction that is ALWAYS rolled
// back, which is what makes it safe to run against a live database.
//
// WHAT AN OPERATOR WILL SEE IN THEIR QUERY LOG, once per database, at `reactiveDrizzle()` setup:
//
//   BEGIN; CREATE TEMP TABLE telefunc_old_new_probe (x int); INSERT …; UPDATE … RETURNING old.x, new.x;
//   ROLLBACK;
//
// It is fire-and-forget, always rolled back, and has no persistent effect — no table, no row, no sequence
// (verified: nothing survives in `pg_class`). A server that rejects the syntax aborts only this transaction.
//
// `CREATE TEMP TABLE` needs the TEMP privilege, which hardened least-privilege roles revoke. That failure
// says nothing about OLD/NEW, so it is told apart from a refusal of the SYNTAX: on permission-denied ONLY,
// the answer falls back to the privilege-free `server_version_num` (18+ ⇒ supported). A version is weaker
// evidence than a statement, so a capability derived that way is marked UNPROVEN, and the first write that
// actually relies on it is guarded — see `oldNewProvenOf` and its use in writeCapture's `runWrite`.
//
// The answer is cached per db object and read SYNCHRONOUSLY by write planning. Until the probe lands the
// answer is unknown, and unknown reads as NOT supported — so capture only ever gains precision from this,
// and never waits on it or assumes it.

/** `supported`: whether to use OLD/NEW at all. `proven`: whether a statement — rather than a version
 *  number — is what established it. An unsupported answer is always proven: there is nothing to retry. */
type OldNewCapability = { supported: boolean; proven: boolean }

const oldNewSupport = new WeakMap<object, OldNewCapability>()
const probeInFlight = new WeakMap<object, Promise<boolean>>()
const PROBE_TABLE = 'telefunc_old_new_probe'
/** Thrown to roll the probe back. A sentinel, so a genuine failure is never mistaken for the rollback. */
const PROBE_ROLLBACK = Symbol('telefunc: capability probe rollback')
const UNSUPPORTED: OldNewCapability = { supported: false, proven: true }

/** Whether `RETURNING old.*, new.*` is known to work on this db. Never `true` before the probe resolves. */
function oldNewReturningOf(db: AnyDb): boolean {
  return oldNewSupport.get(db as object)?.supported === true
}

/** Whether that answer came from a statement that RAN, rather than from the server's version number. An
 *  unproven capability is believed only until the first statement that depends on it says otherwise. */
function oldNewProvenOf(db: AnyDb): boolean {
  return oldNewSupport.get(db as object)?.proven === true
}

/** The first OLD/NEW statement went through: the version was telling the truth, and there is nothing left
 *  to guard. */
function markOldNewProven(db: AnyDb): void {
  const capability = oldNewSupport.get(db as object)
  if (capability?.supported) oldNewSupport.set(db as object, { ...capability, proven: true })
}

/** Believe the statement over the version number, permanently for this db. Called when a write that relied
 *  on an UNPROVEN capability was rejected by the server. */
function demoteOldNewReturning(db: AnyDb): void {
  oldNewSupport.set(db as object, { supported: false, proven: true })
}

/** Start the capability probe for this db, at most once, and return what it settles on. Production calls
 *  this and does NOT await it: nothing waits on the probe, so a slow or failing one costs a write nothing
 *  but the extra precision it would have unlocked. The promise is returned so a caller that genuinely needs
 *  the settled answer (a test asserting the capability) can have it without a test-only seam. */
function probeOldNewReturning(db: AnyDb): Promise<boolean> {
  const key = db as object
  const started = probeInFlight.get(key)
  if (started) return started
  const probe = runOldNewProbe(db).then(
    (capability) => {
      oldNewSupport.set(key, { ...capability })
      return capability.supported
    },
    () => {
      oldNewSupport.set(key, { ...UNSUPPORTED })
      return false
    },
  )
  probeInFlight.set(key, probe)
  return probe
}

async function runOldNewProbe(db: AnyDb): Promise<OldNewCapability> {
  if (dialectOf(db) !== 'pg') return UNSUPPORTED
  const host = db as { transaction?: (cb: (tx: unknown) => Promise<unknown>) => Promise<unknown> }
  if (typeof host.transaction !== 'function') return UNSUPPORTED
  let supported = false
  let failure: unknown
  try {
    await host.transaction(async (tx) => {
      const run = (text: string) => (tx as { execute: (query: SQL) => Promise<unknown> }).execute(sql.raw(text))
      await run(`create temp table ${PROBE_TABLE} (x int)`)
      await run(`insert into ${PROBE_TABLE} values (1)`)
      await run(`update ${PROBE_TABLE} set x = 2 returning old.x, new.x`)
      supported = true
      throw PROBE_ROLLBACK // never commit — the probe must leave the database exactly as it found it
    })
  } catch (error) {
    if (error !== PROBE_ROLLBACK) failure = error
  }
  if (supported) return { supported: true, proven: true }
  // The probe could not RUN, which is a different answer from the server refusing the syntax. A role
  // without the TEMP privilege — routine in a hardened deployment — tells us nothing about OLD/NEW, and
  // treating it as "unsupported" would cost such a database the precision forever.
  if (!isPermissionDenied(failure)) return UNSUPPORTED
  const version = await serverVersionNum(db)
  return version >= 180000 ? { supported: true, proven: false } : UNSUPPORTED
}

/** PostgreSQL's `insufficient_privilege` (42501) — "permission denied to create temporary tables". Matched
 *  by SQLSTATE first, since that is the driver-independent fact; the message is only a fallback for drivers
 *  that do not surface the code.
 *
 *  Walks the CAUSE chain, because drizzle re-throws driver errors wrapped ("Failed query: …") and the
 *  SQLSTATE lives on the original. Reading only the outer error classified every refusal as a syntax
 *  refusal, which silently disabled the fallback this function exists to enable. */
function isPermissionDenied(error: unknown): boolean {
  for (const link of causeChain(error)) {
    if ((link as { code?: unknown }).code === '42501') return true
    const message = link instanceof Error ? link.message.toLowerCase() : ''
    if (message.includes('permission denied') || message.includes('must be owner')) return true
  }
  return false
}

/** `server_version_num` as an integer (180000 = PostgreSQL 18), or 0 when it cannot be read. `SHOW` needs
 *  no privileges, which is the whole point of using it as the fallback. */
async function serverVersionNum(db: AnyDb): Promise<number> {
  try {
    const rows = await rowRunnerFor(db)(sql.raw('show server_version_num'))
    const value = rows[0] ? Object.values(rows[0])[0] : undefined
    const version = Number.parseInt(str(value), 10)
    return Number.isFinite(version) ? version : 0
  } catch {
    return 0
  }
}
