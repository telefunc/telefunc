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
    expect(after.todosFetches).toBe(before.todosFetches + 1) // and the affected one refetched exactly once
  })

  test('reactive: a multi-table transaction on A invalidates BOTH tables as one atomic tick', async () => {
    const before = await openReactive()

    await page.click('#add-tx') // ONE transaction inserting into `todos` AND `notes` on instance A

    await autoRetry(async () => {
      const now = await getResult<State>('#reactive-state')
      expect(now.todosCount).toBe(before.todosCount + 1)
      expect(now.notesCount).toBe(before.notesCount + 1) // both topics reached from one published batch
    })

    const after = await getResult<State>('#reactive-state')
    // Atomicity: the batch spans two of this instance's subscribed topics, and the deterministic
    // cross-topic dedupe rule must apply it EXACTLY ONCE — so each query refetches once, not twice.
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
    // watches, so a raw-SQL write announces itself on the wildcard coarse channel and B coarsens EVERY
    // table it watches — `notes` included. That over-fire is the documented, sound fallback; asserting the
    // control unchanged here would assert the opposite of the intended behaviour.
  })
}
