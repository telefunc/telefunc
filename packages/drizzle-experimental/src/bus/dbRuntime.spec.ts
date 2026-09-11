import { afterEach, describe, expect, it, vi } from 'vitest'
import { announceCoarse, registryFor } from './dbRuntime.js'
import { changeTopicFor, setChangeTransport } from './changeRuntime.js'
import { createInMemoryChangeTransport } from './changeTransport.js'
import { decodeChangePayload } from './changeCodec.js'

// `announceCoarse` is the ONE owner of "a mutation reached tables nobody can name". Its two halves used to
// be wired in two other files that each knew only one of them — this pins that both happen, exactly once,
// from the single call.

/** A graph the router will accept, watching the given tables. */
const watcher = (tables: string[]) =>
  ({
    tables,
    apply: () => ({ invalidated: false }),
    notifyKey: () => tables.join(','),
    fault() {},
    reseed() {},
  }) as never

/** The payloads other instances actually receive on this db's topic. */
async function watchAnnouncements(db: object) {
  const transport = createInMemoryChangeTransport()
  setChangeTransport(db, transport)
  const seen: string[] = []
  await transport.subscribe(changeTopicFor(db), (payload) => seen.push(payload))
  return seen
}

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

describe('announceCoarse — both halves, from one call', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('coarsens every table THIS db watches, locally and directly', async () => {
    // Directly, because a publisher suppresses its own echo: routing the local half through the topic would
    // mean this db never hears about its own raw write.
    const db = {}
    await watchAnnouncements(db)
    registryFor(db).router.register(watcher(['users', 'notes']))
    const ingested = vi.spyOn(registryFor(db).router, 'ingest')

    announceCoarse(db)

    expect(ingested.mock.calls.map(([batch]) => batch.changes)).toEqual([
      [
        { table: 'users', kind: 'coarse' },
        { table: 'notes', kind: 'coarse' },
      ],
    ])
  })

  it('tells other instances with exactly ONE coarse-all — not a batch of this db’s own tables', async () => {
    // The remote half cannot name tables: another instance may watch ones this db has never heard of, and a
    // batch listing only these would never reach them.
    const db = {}
    const announced = await watchAnnouncements(db)
    registryFor(db).router.register(watcher(['users']))

    announceCoarse(db)
    await flush()

    expect(announced).toHaveLength(1)
    expect(decodeChangePayload(announced[0]!)).toMatchObject({ coarseAll: true })
  })

  it('still announces remotely when this db watches nothing itself', async () => {
    // The two halves are independent: a db with no live queries of its own is still a publisher, and
    // skipping the announcement because IT has nothing to coarsen would strand every other instance.
    const db = {}
    const announced = await watchAnnouncements(db)
    const ingested = vi.spyOn(registryFor(db).router, 'ingest')

    announceCoarse(db)
    await flush()

    expect(ingested).not.toHaveBeenCalled() // nothing local to invalidate
    expect(announced).toHaveLength(1) // …and the other instances are told anyway
  })

  it('does not fail its caller when the local feed throws — the statement already ran', async () => {
    const db = {}
    const announced = await watchAnnouncements(db)
    registryFor(db).router.register(watcher(['users']))
    vi.spyOn(registryFor(db).router, 'ingest').mockImplementation(() => {
      throw new Error('a graph exploded')
    })
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})

    expect(() => announceCoarse(db)).not.toThrow()
    await flush()

    expect(reported).toHaveBeenCalled() // reported, not swallowed
    expect(announced).toHaveLength(1) // and the remote half still went out
  })
})
