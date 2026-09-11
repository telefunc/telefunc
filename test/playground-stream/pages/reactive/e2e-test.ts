export { testReactive }

import { autoRetry, expect, getServerUrl, page, test } from '@brillout/test-e2e'
import { getResult, navigate } from '../../e2e-utils'

// T5 — the acceptance test for cross-instance live-query invalidation.
//
// Two server instances (dbA, dbB) share one PGlite database and the default in-process change transport.
// The browser holds TWO live queries, both served by instance B: `todos` (affected by A's writes) and
// `notes` (the CONTROL). Writes go through instance A. So an invalidation that reaches the browser
// travelled A → transport → B → client; a shared graph could not produce it.
//
// The precision half is the point: the control must NOT refetch. It is asserted on FETCH COUNTS, not row
// counts — nothing writes to `notes` in the plain-write case, so its row count is identical whether or not
// it refetched. Only a fetch counter can tell "not invalidated" from "invalidated, read the same rows".
//
// WHAT THIS FILE CAN AND CANNOT OBSERVE. A completed-fetch count is an EVENTUAL-outcome observer, not a
// delivery counter. Two transport deliveries can collapse before reaching it: the server's `Live.invalidate()`
// coalesces same-turn notifications into one frame, and on the client `invalidateQueries({cancelRefetch:true})`
// can cancel and restart an in-flight fetch so two invalidations yield ONE `success` event. So a `+1` here
// proves the query was invalidated and refetched — never that it was delivered exactly once.
//
// That is fine for the acceptance criterion, which is about WHICH queries invalidate, not how many messages
// carried it: an erroneous invalidation still produces at least one eventual success, so the negative control
// stays capable of disagreeing. Exactly-once transport ingestion and the one-atomic-tick property are proven
// where they are actually observable — `writeTransport.spec.ts`, against `router.ingest` directly, with
// mutation-gated controls (`admitBatch` reverted to the owning-table rule flips those tests).
//
// RUNNER CAVEAT (Windows-only, and only for running this lane off its supported platform). This suite's
// supported runner is Ubuntu — `test-e2e.config.mjs` pins it there because the playground scripts are
// bash-only — and none of the below applies there. On a Windows dev box @brillout/test-e2e's teardown does
// not return after a passing run (the suite prints PASS and then hangs killing the dev server), so the
// evidence is `PASS` with zero FAILs rather than a process exit code; and a failed run orphans vite's HMR
// port, because `pnpm dev`'s `fuser -k 24679/tcp` — which does reclaim the port on Linux — finds no `fuser`
// on Windows and is skipped. The orphan then masquerades as a startup failure on the NEXT run; clear it with
// `taskkill -F -T -PID <pid>` before re-running.

type State = { todosCount: number; notesCount: number; todosFetches: number; notesFetches: number }

/** Load the page and wait for both live queries to have delivered a first result. */
async function openReactive(): Promise<State> {
  await navigate(`${getServerUrl()}/reactive`)
  await page.locator('#reactive-ready').waitFor({ state: 'attached', timeout: 10_000 })
  return getResult<State>('#reactive-state')
}

function testReactive() {
  test('reactive: a write on instance A invalidates the affected live query on B — and NOT the control', async () => {
    const before = await openReactive()

    await page.click('#add-todo') // INSERT into `todos` on instance A

    await autoRetry(async () => {
      const now = await getResult<State>('#reactive-state')
      expect(now.todosCount).toBe(before.todosCount + 1) // affected query refetched, with the new row
    })

    const after = await getResult<State>('#reactive-state')
    // THE PRECISION HALF: the control query was never refetched. This is the assertion that fails if
    // invalidation over-fires across tables.
    expect(after.notesFetches).toBe(before.notesFetches)
    expect(after.todosFetches).toBe(before.todosFetches + 1) // and the affected one did refetch
  })

  test('reactive: a multi-table transaction on A invalidates BOTH tables on B', async () => {
    const before = await openReactive()

    await page.click('#add-tx') // ONE transaction inserting into `todos` AND `notes` on instance A

    await autoRetry(async () => {
      const now = await getResult<State>('#reactive-state')
      expect(now.todosCount).toBe(before.todosCount + 1)
      expect(now.notesCount).toBe(before.notesCount + 1) // both tables' live queries saw the committed rows
    })

    const after = await getResult<State>('#reactive-state')
    // Both queries refetched, each landing one observed success. Read this as EVENTUAL invalidation of both
    // tables from one transaction — NOT as proof of exactly-once delivery or of the one-atomic-tick property.
    // A `+1` here cannot distinguish one delivery from two: server-side invalidate coalescing and the client's
    // cancelRefetch can both collapse a second delivery into the same single success, so a duplicated
    // publication would still show `+1`. Exactly-once ingestion is proven in writeTransport.spec.ts against
    // router.ingest, where the count is actually observable and mutation-gated.
    expect(after.todosFetches).toBe(before.todosFetches + 1)
    expect(after.notesFetches).toBe(before.notesFetches + 1)
  })

  test('reactive: a fail-closed COARSE raw-SQL write on A still invalidates cross-instance', async () => {
    const before = await openReactive()

    await page.click('#add-coarse') // raw SQL — touched tables unknowable, so capture fails closed to coarse

    await autoRetry(async () => {
      const now = await getResult<State>('#reactive-state')
      expect(now.todosCount).toBe(before.todosCount + 1) // reached B despite carrying no row images
    })

    // NOTE: no precision assertion here, deliberately. Instance A cannot know which tables instance B
    // watches, so a raw-SQL write announces itself as a coarse-all and B coarsens EVERY table it
    // watches — `notes` included. That over-fire is the documented, sound fallback; asserting the
    // control unchanged here would assert the opposite of the intended behaviour.
  })
}
