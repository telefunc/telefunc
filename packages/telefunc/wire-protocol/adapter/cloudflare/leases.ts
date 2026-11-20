/// <reference types="@cloudflare/workers-types" />
export {
  LEASE_CALL_DEADLINE_MS,
  LEASE_RENEW_EVERY_MS,
  LEASE_SCHEMA,
  HolderGrants,
  dropLeaseScopes,
  grantHolder,
  liveLeases,
  nextLeaseSweep,
  registerLease,
  removeLease,
  renewLease,
  sweepLeases,
}
export type { Lease, LeaseOrder, LeaseRegistered, LeaseRemoved, LeaseRenewed }

import { createDeferred } from '../../../utils/createDeferred.js'

// A Durable Object's leases at an authority (a Room route, a Broadcast presence): one row per scope and holder DO. A
// call can reach the authority any time after it was sent, so the authority tells a late write from a current one by
// a grant it issued to the holder, never by a time. Each register and removal carries the holder's grant and its own
// sequence number: one under a grant the holder no longer has, or not after the row's latest, changes nothing. An
// expired row stays, answered as lapsed, until its holder registers again; a holder's grant and rows go once it has
// held no unexpired lease for LEASE_TTL_MS, and every write it sent before carries a grant the authority no longer has.

const LEASE_TTL_MS = 90_000
const LEASE_RENEW_EVERY_MS = LEASE_TTL_MS / 3
/** How long a registration's or removal's call is waited for, as long as Cloudflare's runtime retries one itself. */
const LEASE_CALL_DEADLINE_MS = 10_000

const LEASE_SCHEMA = `
  CREATE TABLE IF NOT EXISTS telefunc_lease_grant (id INTEGER PRIMARY KEY CHECK (id = 0), last INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS telefunc_lease_holder
    (holder TEXT PRIMARY KEY, grant_id INTEGER NOT NULL, active_at INTEGER NOT NULL);
  CREATE INDEX IF NOT EXISTS telefunc_lease_holder_active_at ON telefunc_lease_holder(active_at);
  CREATE TABLE IF NOT EXISTS telefunc_lease
    (scope TEXT NOT NULL, holder TEXT NOT NULL, lease TEXT NOT NULL, grant_id INTEGER NOT NULL, seq INTEGER NOT NULL,
     bucket TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (scope, holder));
  CREATE INDEX IF NOT EXISTS telefunc_lease_by_holder ON telefunc_lease(holder);
`

/** Where a write stands among its holder's writes: the grant its authority issued to the holder, then the holder's
 *  sequence number. */
type LeaseOrder = { grant: number; seq: number }
/** A holder DO's lease on a scope, whose deliveries go to the holder in `bucket`. */
type Lease = { scope: string; holder: string; lease: string; bucket: string }
/** `stale`: a later write of the holder on the scope came first. `regrant`: the holder's grant is not that one. */
type LeaseRegistered = 'registered' | 'stale' | 'regrant'
type LeaseRenewed = 'renewed' | 'lapsed'
/** `regrant`: the holder's grant is not that one, so nothing was removed. */
type LeaseRemoved = 'removed' | 'regrant'

type Fence = { lease: string; grant_id: number; seq: number }

/** A new grant for `holder`, which ends its earlier one. Called inside a transaction. */
function grantHolder(sql: SqlStorage, holder: string, now: number): number {
  const { last } = sql
    .exec<{ last: number }>(
      'INSERT INTO telefunc_lease_grant (id, last) VALUES (0, 1) ' +
        'ON CONFLICT (id) DO UPDATE SET last = last + 1 RETURNING last',
    )
    .one()
  sql.exec(
    'INSERT OR REPLACE INTO telefunc_lease_holder (holder, grant_id, active_at) VALUES (?, ?, ?)',
    holder,
    last,
    now,
  )
  return last
}

/** Holds `lease` on its scope for its holder, unless a later write of the holder's came first. Called inside a
 *  transaction. */
function registerLease(sql: SqlStorage, lease: Lease, order: LeaseOrder, now: number): LeaseRegistered {
  if (!isCurrentGrant(sql, lease.holder, order.grant, now)) return 'regrant'
  const fence = readFence(sql, lease.scope, lease.holder)
  if (fence !== null && !isAfter(order, fence)) return 'stale'
  sql.exec(
    'INSERT OR REPLACE INTO telefunc_lease (scope, holder, lease, grant_id, seq, bucket, expires_at) ' +
      'VALUES (?, ?, ?, ?, ?, ?, ?)',
    lease.scope,
    lease.holder,
    lease.lease,
    order.grant,
    order.seq,
    lease.bucket,
    now + LEASE_TTL_MS,
  )
  return 'registered'
}

/** Extends `lease` if it is the scope's unexpired lease of its holder; an expired one stays lapsed until its holder
 *  registers it again. Called inside a transaction. */
function renewLease(sql: SqlStorage, scope: string, holder: string, lease: string, now: number): LeaseRenewed {
  const renewed = sql.exec(
    'UPDATE telefunc_lease SET expires_at = ? WHERE scope = ? AND holder = ? AND lease = ? AND expires_at > ?',
    now + LEASE_TTL_MS,
    scope,
    holder,
    lease,
    now,
  ).rowsWritten
  if (renewed === 0) return 'lapsed'
  sql.exec('UPDATE telefunc_lease_holder SET active_at = ? WHERE holder = ?', now, holder)
  return 'renewed'
}

/** Ends `lease`, and only that lease, on its scope; a register of the holder's made before it is refused from then on,
 *  however late it comes. Called inside a transaction. */
function removeLease(
  sql: SqlStorage,
  scope: string,
  holder: string,
  lease: string,
  order: LeaseOrder,
  now: number,
): LeaseRemoved {
  if (!isCurrentGrant(sql, holder, order.grant, now)) return 'regrant'
  const fence = readFence(sql, scope, holder)
  if (fence === null) {
    // The register it removes hasn't come yet.
    sql.exec(
      'INSERT INTO telefunc_lease (scope, holder, lease, grant_id, seq, bucket, expires_at) VALUES (?, ?, ?, ?, ?, ?, 0)',
      scope,
      holder,
      lease,
      order.grant,
      order.seq,
      '',
    )
    return 'removed'
  }
  if (!isAfter(order, fence)) return 'removed'
  sql.exec(
    'UPDATE telefunc_lease SET grant_id = ?, seq = ?, expires_at = CASE WHEN lease = ? THEN 0 ELSE expires_at END ' +
      'WHERE scope = ? AND holder = ?',
    order.grant,
    order.seq,
    lease,
    scope,
    holder,
  )
  return 'removed'
}

/** The unexpired leases on `scopes`, by holder, with the bucket each holder is in. */
function liveLeases<Bucket extends string = string>(
  sql: SqlStorage,
  scopes: readonly string[],
  now: number,
): { holder: string; bucket: Bucket }[] {
  return sql
    .exec<{ holder: string; bucket: Bucket }>(
      `SELECT holder, bucket FROM telefunc_lease WHERE scope IN (${scopes.map(() => '?').join(', ')}) AND expires_at > ?`,
      ...scopes,
      now,
    )
    .toArray()
}

/** Drops each holder that has held no unexpired lease for LEASE_TTL_MS, with its grant and rows. Called inside a
 *  transaction. */
function sweepLeases(sql: SqlStorage, now: number): void {
  const idle = sql
    .exec<{ holder: string }>(
      'SELECT holder FROM telefunc_lease_holder h WHERE active_at <= ? AND NOT EXISTS ' +
        '(SELECT 1 FROM telefunc_lease l WHERE l.holder = h.holder AND l.expires_at > ?)',
      now - LEASE_TTL_MS,
      now,
    )
    .toArray()
  for (const { holder } of idle) {
    sql.exec('DELETE FROM telefunc_lease WHERE holder = ?', holder)
    sql.exec('DELETE FROM telefunc_lease_holder WHERE holder = ?', holder)
  }
}

/** When `sweepLeases()` may next drop a holder, or null while none is held. */
function nextLeaseSweep(sql: SqlStorage): number | null {
  const { active } = sql
    .exec<{ active: number | null }>('SELECT MIN(active_at) AS active FROM telefunc_lease_holder')
    .one()
  return active === null ? null : active + LEASE_TTL_MS
}

/** Deletes the rows of every scope that starts with `prefix`, whose writes its caller refuses from then on. */
function dropLeaseScopes(sql: SqlStorage, prefix: string): void {
  sql.exec('DELETE FROM telefunc_lease WHERE substr(scope, 1, length(?)) = ?', prefix, prefix)
}

/** Whether `grant` is `holder`'s; it then counts as active now. */
function isCurrentGrant(sql: SqlStorage, holder: string, grant: number, now: number): boolean {
  return (
    sql.exec('UPDATE telefunc_lease_holder SET active_at = ? WHERE holder = ? AND grant_id = ?', now, holder, grant)
      .rowsWritten > 0
  )
}

function readFence(sql: SqlStorage, scope: string, holder: string): Fence | null {
  const rows = sql
    .exec<Fence>('SELECT lease, grant_id, seq FROM telefunc_lease WHERE scope = ? AND holder = ?', scope, holder)
    .toArray()
  return rows[0] ?? null
}

function isAfter(order: LeaseOrder, fence: Fence): boolean {
  return order.grant > fence.grant_id || (order.grant === fence.grant_id && order.seq > fence.seq)
}

const REGRANTS = 2
const REMOVE_RETRY_MIN_MS = 100
const REMOVE_RETRY_MAX_MS = 5_000

/** A Durable Object's grants at the authorities it holds leases at, and the order of its writes there. */
class HolderGrants {
  private readonly grants = new Map<string, Promise<number>>()
  private seq = 0

  /** Sends `write` with this DO's next order at `target`, getting a grant from `grant` first where it has none, and
   *  again where the target answers that the grant is no longer this DO's. `write` is called at once, with the order
   *  still to come, so it can take its place among this DO's calls to `target` before the grant is answered. */
  async send<T>(
    target: string,
    grant: () => Promise<number>,
    write: (order: Promise<LeaseOrder>) => Promise<T | 'regrant'>,
  ): Promise<T> {
    for (let regrants = 0; ; regrants++) {
      const granted = this.grantAt(target, grant)
      const seq = ++this.seq
      const answer = await write(granted.then((grant) => ({ grant, seq })))
      if (answer !== 'regrant') return answer
      if (this.grants.get(target) === granted) this.grants.delete(target)
      if (regrants === REGRANTS) throw new Error(`The Durable Object at '${target}' refused each grant it issued`)
    }
  }

  /** Sends a removal through `send` until the target answers it, waiting between tries a time that doubles, and at least
   *  `pausedFor()`: what a write that failed or wasn't waited for may have stored goes, however long its target fails.
   *  Settles once its first try did; the tries after it go on without a caller. Its first failure is reported. */
  removeUntilDone(send: () => Promise<unknown>, pausedFor: () => number): Promise<void> {
    const first = createDeferred<void>()
    void (async () => {
      for (let wait = REMOVE_RETRY_MIN_MS; ; wait = Math.min(wait * 2, REMOVE_RETRY_MAX_MS)) {
        try {
          await send()
          return first.resolve()
        } catch (error) {
          if (wait === REMOVE_RETRY_MIN_MS) console.error(error)
          first.resolve()
          await new Promise((resolve) => setTimeout(resolve, Math.max(wait, pausedFor())))
        }
      }
    })()
    return first.promise
  }

  private grantAt(target: string, grant: () => Promise<number>): Promise<number> {
    let granted = this.grants.get(target)
    if (granted === undefined) {
      granted = grant()
      this.grants.set(target, granted)
      // A grant that failed is asked for again by the next write.
      granted.catch(() => {
        if (this.grants.get(target) === granted) this.grants.delete(target)
      })
    }
    return granted
  }
}
