/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastAuthorityState }

import { integerColumn, type DurableStorage, type SqlStore } from './sqlStorage.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import {
  grantHolder,
  initLeaseSchema,
  leaseRegistered,
  liveLeases,
  nextLeaseSweep,
  registerLease,
  removeLease,
  renewLease,
  sweepLeases,
  type LeaseRegistered,
  type LeaseRemoved,
  type LeaseRenewed,
} from './leases.js'
import type {
  MemberPlacement,
  BroadcastLeaseRequest,
  BroadcastHoldRequest,
  BroadcastReleaseRequest,
} from './broadcastRpc.js'

const AUTHORITY_SCHEMA = 'CREATE TABLE IF NOT EXISTS broadcast_key (key TEXT PRIMARY KEY, seq INTEGER NOT NULL)'

/** The Broadcast state of a key authority DO, in its SQLite storage: each key's `seq` and each route's member presence.
 *  The tables are created on first use, so a DO in another role never has them. */
class CloudflareBroadcastAuthorityState {
  private readonly storage: DurableStorage
  private schema = false
  /** The alarm this run of the object set, null for none; undefined until it first sets or clears it. */
  private alarmAt: number | null | undefined = undefined

  constructor(state: { storage: DurableStorage }) {
    this.storage = state.storage
  }

  nextSequence(key: string): number {
    const sql = this.sql()
    return this.storage.transactionSync(() => {
      const row = sql.exec('SELECT seq FROM broadcast_key WHERE key = ?', key).toArray()[0]
      const seq = (row === undefined ? 0 : integerColumn(row, 'seq')) + 1
      sql.exec('INSERT OR REPLACE INTO broadcast_key (key, seq) VALUES (?, ?)', key, seq)
      return seq
    })
  }

  /** A grant for a member DO's writes of presence here; a member DO that holds no presence lapses with its grant. */
  async grant(member: string): Promise<number> {
    const grant = this.transaction((sql) => grantHolder(sql, member, Date.now()))
    await this.scheduleSweep()
    return grant
  }

  /** The RPC reply waits for the write. A hold that came before is answered as it was. */
  async hold({ key, kind, member, lease, bucket, order }: BroadcastHoldRequest): Promise<LeaseRegistered> {
    const scope = broadcastRouteKey({ key, kind })
    const held = this.transaction((sql) =>
      registerLease(sql, { scope, holder: member, lease, bucket }, order, Date.now()),
    )
    await this.scheduleSweep()
    return leaseRegistered(held)
  }

  renew({ key, kind, member, lease }: BroadcastLeaseRequest): LeaseRenewed {
    const scope = broadcastRouteKey({ key, kind })
    return this.transaction((sql) => renewLease(sql, scope, member, lease, Date.now()))
  }

  async release({ key, kind, member, lease, order }: BroadcastReleaseRequest): Promise<LeaseRemoved> {
    const scope = broadcastRouteKey({ key, kind })
    const removed = this.transaction((sql) => removeLease(sql, scope, member, lease, order, Date.now()))
    await this.scheduleSweep()
    return removed
  }

  /** Drops the holders that went idle, so a key nobody holds stores no lease row once `LEASE_TTL_MS` passed. */
  async alarm(): Promise<void> {
    this.alarmAt = null
    this.transaction((sql) => sweepLeases(sql, Date.now()))
    await this.scheduleSweep()
  }

  /** The route's member DOs whose presence is unexpired, each with its bucket. */
  livePresence(routeKey: string, now: number): MemberPlacement[] {
    return liveLeases(this.sql(), [routeKey], now, (member, bucket) => ({ member, bucket }))
  }

  private transaction<T>(fn: (sql: SqlStore) => T): T {
    const sql = this.sql()
    return this.storage.transactionSync(() => fn(sql))
  }

  /** Sets the alarm to when a holder may next go idle in the turn of the write that changed it, with no `await`
   *  between them, so both are stored or neither. An earlier alarm sets the next one when it fires. */
  private scheduleSweep(): Promise<void> {
    const next = nextLeaseSweep(this.sql())
    const current = this.alarmAt
    if (next === null) {
      if (current === null) return Promise.resolve()
      this.alarmAt = null
      return this.storage.deleteAlarm()
    }
    if (current !== undefined && current !== null && current <= next) return Promise.resolve()
    this.alarmAt = next
    return this.storage.setAlarm(next)
  }

  private sql(): SqlStore {
    if (!this.schema) {
      this.storage.sql.exec(AUTHORITY_SCHEMA)
      initLeaseSchema(this.storage.sql)
      this.schema = true
    }
    return this.storage.sql
  }
}
