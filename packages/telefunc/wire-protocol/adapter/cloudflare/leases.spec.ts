import { describe, expect, it } from 'vitest'
import { hasNodeSqlite, sqliteStorage } from '../../test/cloudflareSql.js'
import {
  grantHolder,
  initLeaseSchema,
  liveLeases,
  registerLease,
  removeLease,
  renewLease,
  sweepLeases,
} from './leases.js'

/** An authority's lease rows on the SQLite stand-in, and a holder `member` granted at time 0. */
function authority() {
  const { sql } = sqliteStorage().storage
  initLeaseSchema(sql)
  const grant = grantHolder(sql, 'member', 0)
  const lease = (id: string) => ({ scope: 'room:test', holder: 'member', lease: id, bucket: 'weur' })
  return {
    sql,
    grant,
    register: (id: string, seq: number, now = 0, order = { grant, seq }) => registerLease(sql, lease(id), order, now),
    remove: (id: string, seq: number, now = 0) => removeLease(sql, 'room:test', 'member', id, { grant, seq }, now),
    live: (now = 0) => liveLeases(sql, ['room:test'], now, (holder) => holder),
  }
}

// Each write of a holder carries its grant and a sequence number in the order the holder sent them; the authority gets
// them in any order, and applies a write only if it is after the latest it applied.
describe.runIf(hasNodeSqlite)("an authority's leases", () => {
  it('refuse a register that arrives after the removal sent after it', () => {
    const { register, remove, live } = authority()
    expect(remove('l1', 2)).toBe('removed')
    expect(register('l1', 1)).toBe('stale')
    expect(live()).toEqual([])
  })

  it("keep a holder's newer lease when its older lease's register arrives after it", () => {
    const { sql, register } = authority()
    expect(register('l2', 2)).toBe('registered')
    expect(register('l1', 1)).toBe('stale')
    expect(sql.exec('SELECT lease FROM telefunc_lease').toArray()).toEqual([{ lease: 'l2' }])
  })

  it('refuse a write under a grant the authority dropped, once the holder held nothing for 90 s', () => {
    const { sql, grant, register, remove } = authority()
    expect(register('l1', 1)).toBe('registered')
    expect(remove('l1', 2, 1_000)).toBe('removed')
    sweepLeases(sql, 91_001)
    expect(register('l1', 3, 91_001, { grant, seq: 3 })).toBe('regrant')
  })

  it('answer a renewal of a lapsed lease lapsed, and keep it lapsed', () => {
    const { sql, register, live } = authority()
    expect(register('l1', 1)).toBe('registered')
    expect(renewLease(sql, 'room:test', 'member', 'l1', 90_001)).toBe('lapsed')
    expect(live(90_001)).toEqual([])
  })
})
