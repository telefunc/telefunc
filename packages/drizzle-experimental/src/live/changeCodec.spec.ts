// The wire format has one job: a row that goes in comes back out as the same VALUE, and anything that
// cannot be trusted degrades to coarse instead of to a wrong row. Both halves are asserted here — a codec
// tested only on the happy path is how a `bigserial` id becomes a string in production.

import { PGlite } from '@electric-sql/pglite'
import { eq } from 'drizzle-orm'
import * as pg from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import { describe, expect, it, vi } from 'vitest'
import { CHANGE_CODEC_VERSION, type ChangeEnvelope, decodeChangePayload, encodeChangePayload } from './changeCodec.js'
import { hydrationExecutorOf } from '../binding/hydrationExecutor.js'
import { compileQuery } from '../compile/compile.js'
import { extractQueryShape } from '../extract/queryShape.js'
import { createRegistry } from '../graph/registry.js'
import type { TableChange } from '../router/events.js'

const roundTrip = (changes: TableChange[]): TableChange[] => {
  const decoded = decodeChangePayload(
    encodeChangePayload({ version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'origin-a', seq: 1, changes }),
  )
  expect(decoded).toBeDefined()
  return (decoded as { changes: TableChange[] }).changes
}

describe('change codec — the era-cut marker', () => {
  it('survives the wire, on both envelope forms', () => {
    const base = { version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'a', seq: 2, eraCut: true as const }
    expect(decodeChangePayload(encodeChangePayload({ ...base, coarseAll: true }))).toMatchObject({ eraCut: true })
    expect(decodeChangePayload(encodeChangePayload({ ...base, changes: [] }))).toMatchObject({ eraCut: true })
  })

  it('CONTROL: an ordinary publication carries no marker at all', () => {
    const decoded = decodeChangePayload(
      encodeChangePayload({ version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'a', seq: 2, changes: [] }),
    )
    // Absent, not `false` — the receiver branches on presence, and a decoder that invented the field would
    // make every publication look like a cut.
    expect(decoded).toBeDefined()
    expect('eraCut' in (decoded as object)).toBe(false)
  })
})

describe('change codec — SQL values survive a serializing transport', () => {
  it('carries the value domain a row is actually made of', () => {
    const row = {
      big: BigInt('100000000000000000000'), // a bigserial id — plain JSON throws on this
      when: new Date('2020-01-02T03:04:05.678Z'),
      bytes: new Uint8Array([1, 2, 250]),
      buf: Buffer.from([9, 8, 7]),
      nothing: null,
      missing: undefined,
      notANumber: Number.NaN,
      infinite: Number.POSITIVE_INFINITY,
      negInfinite: Number.NEGATIVE_INFINITY,
      nested: { deep: [BigInt(1), new Date(0)] },
    }
    const [change] = roundTrip([{ table: 'users', kind: 'insert', new: row }])
    const out = change!.new as typeof row

    expect(out.big).toBe(BigInt('100000000000000000000'))
    expect(out.when).toBeInstanceOf(Date)
    expect(out.when.getTime()).toBe(row.when.getTime())
    expect(out.nothing).toBeNull()
    expect(out.notANumber).toBeNaN()
    expect(out.infinite).toBe(Number.POSITIVE_INFINITY)
    expect(out.negInfinite).toBe(Number.NEGATIVE_INFINITY)
    expect('missing' in out).toBe(true) // an explicit undefined column is not the same as an absent one
    expect(out.nested.deep[0]).toBe(BigInt(1))
    expect(out.nested.deep[1]).toBeInstanceOf(Date)

    // Byte arrays are the one type the underlying serializer drops; both spellings come back as bytes.
    expect(out.bytes).toBeInstanceOf(Uint8Array)
    expect([...out.bytes]).toEqual([1, 2, 250])
    expect(out.buf).toBeInstanceOf(Uint8Array)
    expect([...out.buf]).toEqual([9, 8, 7])
  })

  it('carries a composite retraction key and a coarse marker unchanged', () => {
    const changes = roundTrip([
      { table: 'memberships', kind: 'delete', key: { userId: BigInt(7), teamId: new Date(5) } },
      { table: 'audit', kind: 'coarse' },
    ])
    expect(changes[0]!.key).toEqual({ userId: BigInt(7), teamId: new Date(5) })
    expect(changes[1]).toEqual({ table: 'audit', kind: 'coarse' })
  })

  it('round-trips a coarse-all envelope', () => {
    const payload = encodeChangePayload({
      version: CHANGE_CODEC_VERSION,
      namespace: 'ns',
      origin: 'origin-a',
      seq: 1,
      coarseAll: true,
    })
    expect(decodeChangePayload(payload)).toEqual({
      version: CHANGE_CODEC_VERSION,
      namespace: 'ns',
      origin: 'origin-a',
      seq: 1,
      coarseAll: true,
    })
  })

  it('preserves origin — the publisher drops its own echo by it', () => {
    const decoded = decodeChangePayload(
      encodeChangePayload({ version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'db-42', seq: 1, changes: [] }),
    )
    expect(decoded!.origin).toBe('db-42')
  })
})

describe('change codec — anything untrustworthy decodes to nothing (the caller coarsens)', () => {
  const rejected = (payload: string) => expect(decodeChangePayload(payload)).toBeUndefined()

  it('rejects an unknown codec version rather than interpreting it', () => {
    const future = encodeChangePayload({
      version: CHANGE_CODEC_VERSION + 1,
      namespace: 'ns',
      origin: 'a',
      seq: 1,
      coarseAll: true,
    })
    rejected(future)
  })

  it('rejects an OLDER version too — the match is exact, not a floor', () => {
    // What makes `eraCut` safe to add by bumping the version: a receiver from before the field existed
    // cannot decode a payload carrying it, so it coarsens instead of silently ignoring the marker and
    // betting precise across a cut. That only holds if the version check is equality, not `>=`.
    rejected(
      encodeChangePayload({
        version: CHANGE_CODEC_VERSION - 1,
        namespace: 'ns',
        origin: 'a',
        seq: 1,
        coarseAll: true,
      }),
    )
  })

  it('rejects a malformed eraCut marker rather than reading it as absent', () => {
    // Coercing a truthy-but-wrong marker to "no cut" is the one misreading that costs a permanent hole.
    const base = { version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'a', seq: 1, coarseAll: true }
    rejected(JSON.stringify({ ...base, eraCut: 'yes' }))
    rejected(JSON.stringify({ ...base, eraCut: 1 }))
    rejected(JSON.stringify({ ...base, eraCut: false }))
  })

  it('rejects malformed text', () => {
    rejected('{not json at all')
    rejected('')
  })

  it('rejects a well-formed payload that is not an envelope', () => {
    const base = { version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'a', seq: 1 }
    rejected(JSON.stringify({ hello: 'world' }))
    rejected(JSON.stringify({ version: CHANGE_CODEC_VERSION })) // no origin
    rejected(JSON.stringify(base)) // neither changes nor coarseAll
    rejected(JSON.stringify(null))
  })

  it('rejects an envelope with no usable DATABASE identity — the field that stops cross-feeding', () => {
    // A blank or missing namespace would collapse every database onto one identity, which is the whole
    // failure the field exists to prevent. Fail closed rather than treat it as "some database".
    rejected(JSON.stringify({ version: CHANGE_CODEC_VERSION, origin: 'a', seq: 1, coarseAll: true }))
    rejected(JSON.stringify({ version: CHANGE_CODEC_VERSION, namespace: '', origin: 'a', seq: 1, coarseAll: true }))
    rejected(JSON.stringify({ version: CHANGE_CODEC_VERSION, namespace: 7, origin: 'a', seq: 1, coarseAll: true }))
  })

  it('rejects an envelope with no usable SEQUENCE — the field the ordering rules read', () => {
    const withSeq = (seq: unknown) =>
      JSON.stringify({ version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'a', seq, coarseAll: true })
    rejected(withSeq(undefined)) // absent
    rejected(withSeq(0)) // sequences start at 1, so 0 is not a position
    rejected(withSeq(-1))
    rejected(withSeq(1.5))
    rejected(withSeq('1'))
    expect(decodeChangePayload(withSeq(1))).toBeDefined() // …and a real one still passes
  })

  it('a valid envelope IS accepted — so the rejections above are not vacuous', () => {
    const ok: ChangeEnvelope = { version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'a', seq: 1, changes: [] }
    expect(decodeChangePayload(encodeChangePayload(ok))).toBeDefined()
  })
})

// PARSEABLE BUT INVALID is the dangerous class, not the obviously-broken text above: these all decoded as
// TRUSTED precise payloads before validation reached inside `changes`. `null` threw inside `router.ingest`
// (which on the in-memory transport also aborts dispatch to every later subscriber), and a change with no
// `table` routed to nothing at all — a silently MISSED invalidation, the failure this codec exists to stop.
describe('change codec — a change that is not a change never passes as one', () => {
  const envelope = (changes: unknown[]) =>
    JSON.stringify({ version: CHANGE_CODEC_VERSION, namespace: 'ns', origin: 'a', seq: 1, changes })
  const rejects = (changes: unknown[]) => expect(decodeChangePayload(envelope(changes))).toBeUndefined()

  it('rejects a non-object entry', () => {
    rejects([null])
    rejects(['users'])
    rejects([[{ table: 'users', kind: 'coarse' }]])
  })

  it('rejects a missing, empty or non-string table — the silent-miss shape', () => {
    rejects([{ kind: 'coarse' }])
    rejects([{ table: '', kind: 'coarse' }])
    rejects([{ table: 42, kind: 'coarse' }])
  })

  it('rejects an unknown or missing kind', () => {
    rejects([{ table: 'users', kind: 'upsert' }])
    rejects([{ table: 'users' }])
  })

  it('rejects a row field that is not a row', () => {
    rejects([{ table: 'users', kind: 'insert', new: 'not a row' }])
    rejects([{ table: 'users', kind: 'insert', new: [1, 2] }])
    rejects([{ table: 'users', kind: 'delete', key: null }])
  })

  it('rejects a row-kind carrying no row data at all (it would route, then have nothing to apply)', () => {
    rejects([{ table: 'users', kind: 'insert' }])
    rejects([{ table: 'users', kind: 'delete' }])
  })

  it('rejects a row-kind carrying the WRONG row field for its kind', () => {
    // "some row field is present" is not the requirement — the requirement is the field the kind is applied
    // FROM. An insert built from `old` routes to precisely the right graph and then invalidates nothing.
    rejects([{ table: 'users', kind: 'insert', old: { id: 1 } }])
    rejects([{ table: 'users', kind: 'insert', key: { id: 1 } }])
    rejects([{ table: 'users', kind: 'update', old: { id: 1 } }])
    rejects([{ table: 'users', kind: 'update', key: { id: 1 } }])
  })

  it('rejects a byte token whose grammar or range is wrong, rather than coercing it', () => {
    // `Uint8Array.from` is not a validator: 999 truncates to 231 and 'not-a-byte' becomes 0, so a mangled
    // token would arrive as a plausible byte string. The module promises a mangled payload coarsens.
    const withNote = (note: string) =>
      JSON.stringify({
        version: CHANGE_CODEC_VERSION,
        namespace: 'ns',
        origin: 'a',
        seq: 1,
        changes: [{ table: 't', kind: 'insert', new: { b: note } }],
      })
    expect(decodeChangePayload(withNote('!Bytes:999,not-a-byte'))).toBeUndefined()
    expect(decodeChangePayload(withNote('!Bytes:256'))).toBeUndefined()
    expect(decodeChangePayload(withNote('!Bytes:-1'))).toBeUndefined()
    expect(decodeChangePayload(withNote('!Bytes:1.5'))).toBeUndefined()
    expect(decodeChangePayload(withNote('!Bytes:1, 2'))).toBeUndefined()
    expect(decodeChangePayload(withNote('!Bytes:'))).toBeDefined() // …but the empty token is legitimate
    expect(decodeChangePayload(withNote('!Bytes:0,255'))).toBeDefined() // …as are the range's edges
  })

  it('rejects the WHOLE envelope for one bad entry — a partly-valid batch has no safe partial reading', () => {
    rejects([
      { table: 'users', kind: 'coarse' },
      { table: 'posts', kind: 'nonsense' },
    ])
  })

  it('accepts every well-formed kind — so none of the above passes for the wrong reason', () => {
    const valid = [
      { table: 'users', kind: 'coarse' },
      { table: 'users', kind: 'insert', new: { id: 1 } },
      { table: 'users', kind: 'update', new: { id: 1 }, key: { id: 1 } },
      { table: 'users', kind: 'delete', key: { id: 1 } },
      { table: 'users', kind: 'delete', old: { id: 1 } }, // an old-image retraction identifies the row too
    ]
    expect(decodeChangePayload(envelope(valid))).toBeDefined()
    for (const change of valid) expect(decodeChangePayload(envelope([change]))).toBeDefined()
  })
})

// The byte tag used to be an OBJECT shape, which a JSON/JSONB column can contain verbatim. It aliased user
// data: the object decoded into a Uint8Array and its other fields vanished. The tag now lives in the
// serializer's `!`-prefixed string namespace, where the serializer's own escaping rule keeps ordinary
// strings from ever colliding with it.
describe('change codec — the byte tag cannot be forged by ordinary row data', () => {
  it('carries an object shaped like the OLD byte tag through untouched, siblings and all', () => {
    const row = { blob: { __telefunc_live_bytes__: [65, 66], ordinary: 'json column' } }
    const [change] = roundTrip([{ table: 'docs', kind: 'insert', new: row }])
    expect((change!.new as typeof row).blob).toEqual({ __telefunc_live_bytes__: [65, 66], ordinary: 'json column' })
  })

  it('carries a STRING that looks like the byte token through untouched', () => {
    const row = { note: '!Bytes:1,2,3', alsoNote: '!!Bytes:9', plain: 'Bytes:1,2,3', bang: '!Date:nope' }
    const [change] = roundTrip([{ table: 'docs', kind: 'insert', new: row }])
    expect(change!.new).toEqual(row)
  })

  it('still carries real bytes, including an empty one, next to a look-alike string', () => {
    const row = { real: new Uint8Array([7, 8]), empty: new Uint8Array(), fake: '!Bytes:7,8' }
    const [change] = roundTrip([{ table: 'docs', kind: 'insert', new: row }])
    const out = change!.new as unknown as typeof row

    expect(out.real).toBeInstanceOf(Uint8Array)
    expect([...out.real]).toEqual([7, 8])
    expect(out.empty).toBeInstanceOf(Uint8Array)
    expect([...out.empty]).toEqual([])
    expect(out.fake).toBe('!Bytes:7,8') // the string stayed a string
  })
})

// WHY the shape rules above are the shapes they are. Rejecting `{ kind:'insert', old:{…} }` looks pedantic
// until you watch it: it routes to precisely the right graph and then invalidates NOTHING, because an insert
// is applied from its post-image. That is a silently missed invalidation manufactured INSIDE the boundary
// whose entire job is to fail closed — strictly worse than a malformed payload, which at least coarsens.
//
// So this drives the real thing: a REAL PGlite-hydrated stateful join graph, through the REAL router, with
// the REAL registry sink. The middle assertion is the control that can disagree — the same row as a
// well-formed insert MUST invalidate, or the first assertion would pass for an inert graph rather than for
// the missing post-image.
describe('change codec — the rejected shapes are shapes that would SILENTLY MISS', () => {
  const users = pg.pgTable('users', {
    id: pg.integer('id').primaryKey(),
    teamId: pg.integer('team_id'),
    name: pg.text('name'),
  })
  const teams = pg.pgTable('teams', { id: pg.integer('id').primaryKey(), region: pg.text('region') })

  /** A users⋈teams graph seeded from real PGlite and asserted LIVE — a coarse graph would invalidate on
   *  anything and make the whole comparison vacuous. */
  async function hydratedJoin() {
    const client = new PGlite()
    const db = drizzle({ client })
    await client.exec('create table users (id int primary key, team_id int, name text)')
    await client.exec('create table teams (id int primary key, region text)')
    await client.query("insert into teams (id, region) values (1, 'eu')")
    await client.query("insert into users (id, team_id, name) values (1, 1, 'a')")

    const registry = createRegistry({ maxStateRowsPerInput: 1e9 })
    const shape = extractQueryShape(db.select().from(users).innerJoin(teams, eq(users.teamId, teams.id)), {
      dialect: 'pg',
    })
    const notify = vi.fn()
    const { graph, token } = await registry.acquire({
      instanceKey: 'codec-consequence',
      tables: shape.tables,
      rlsEnabled: false,
      compilePlan: () => compileQuery(shape),
      executor: hydrationExecutorOf(db),
      notify,
    })
    token.redeem() // seam 1: an UN-redeemed token joins no sink, so notify could never fire
    notify.mockClear()
    expect(graph.state()).toBe('live') // precise, not coarse — otherwise everything below is vacuous
    return { client, registry, notify }
  }

  it('an insert built from `old` invalidates NOTHING on a real graph — and the codec rejects it', async () => {
    const { client, registry, notify } = await hydratedJoin()
    // PHYSICAL column names: a captured row comes from the driver (`.returning()`), so the graph keys its
    // join on `team_id`, not the drizzle property `teamId`. Using the property name made the first version of
    // this test's CONTROL fail to fire — which is exactly what a control is for.
    const row = { id: 2, team_id: 1, name: 'b' }
    const malformed = { table: 'users', kind: 'insert', old: row } as unknown as TableChange

    // 1. THE HARM. Routed correctly, applied from nothing.
    registry.router.ingest({ changes: [malformed] })
    expect(notify).not.toHaveBeenCalled()

    // 2. THE CONTROL. The same row, well-formed, DOES invalidate — so the silence above is the missing
    //    post-image and not an inert graph.
    registry.router.ingest({ changes: [{ table: 'users', kind: 'insert', new: row }] })
    expect(notify).toHaveBeenCalled()

    // 3. THE GATE. The shape from (1) never gets through the codec to reach a graph at all.
    const payload = JSON.stringify({
      version: CHANGE_CODEC_VERSION,
      namespace: 'ns',
      origin: 'remote',
      seq: 1,
      changes: [malformed],
    })
    expect(decodeChangePayload(payload)).toBeUndefined()

    await client.close()
    // Boots PGlite and hydrates a real join graph, so it needs more than the 5s default on a loaded machine.
  }, 30_000)
})
