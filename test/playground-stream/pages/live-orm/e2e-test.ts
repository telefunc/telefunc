export { testLiveOrm }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import type { Page } from 'playwright-chromium'
import { navigate } from '../../e2e-utils'

const ORMS = ['drizzle', 'prisma', 'kysely'] as const
const LISTS = ['todos', 'first-todos', 'todos-with-notes', 'notes-of-1', 'notes-of-2', 'watched-notes-of-1'] as const
type List = (typeof LISTS)[number]
type Fetches = Record<List, number>

function testLiveOrm() {
  for (const orm of ORMS) {
    const url = () => `${getServerUrl()}/live-orm/${orm}`

    test(`live-orm (${orm}): the server renders the rows`, async () => {
      const html = await (await fetch(url())).text()
      expect(html).toContain('Second todo')
      expect(html).toContain('First todo: Note of the first todo')
    })

    test(`live-orm (${orm}): a write refetches the lists showing its rows, and only those`, async () => {
      await open(page, url())
      const start = await readFetches(page)

      // A note of the first todo: the second todo's notes are rows of the same table, and their list stays.
      await write(page, 'add-note-to-1', `${orm} note of 1`)
      await expectList(page, 'notes-of-1', `${orm} note of 1`)
      await expectList(page, 'watched-notes-of-1', `${orm} note of 1`)
      await expectList(page, 'todos-with-notes', `First todo: ${orm} note of 1`)

      // A todo from another tab: it sorts after the first two todos, so their list stays.
      const context2 = await page.context().browser()!.newContext()
      const page2 = await context2.newPage()
      await open(page2, url())
      await write(page2, 'add-todo', `${orm} todo from tab 2`)
      await expectList(page, 'todos', `${orm} todo from tab 2`)
      await expectList(page, 'todos-with-notes', `${orm} todo from tab 2: -`)
      await page2.close()
      await context2.close()

      // A last write and its refetch: a refetch the writes above caused would have completed by now.
      await write(page, 'add-note-to-2', `${orm} note of 2`)
      await expectList(page, 'notes-of-2', `${orm} note of 2`)
      await waitForFetches(page, { 'todos-with-notes': start['todos-with-notes'] + 3 })
      expect(await readFetches(page)).deep.equal({
        todos: start.todos + 1,
        'first-todos': start['first-todos'],
        'todos-with-notes': start['todos-with-notes'] + 3,
        'notes-of-1': start['notes-of-1'] + 1,
        'notes-of-2': start['notes-of-2'] + 1,
        'watched-notes-of-1': start['watched-notes-of-1'] + 1,
      })
    })

    test(`live-orm (${orm}): a committed transaction refetches, a rolled-back one doesn't`, async () => {
      await open(page, url())
      await write(page, 'add-todo-and-note', `${orm} both`)
      await expectList(page, 'todos', `${orm} both`)
      await expectList(page, 'todos-with-notes', `${orm} both: ${orm} both`)

      const before = await readFetches(page)
      await write(page, 'add-todo-then-roll-back', `${orm} rolled back`)
      await page.waitForSelector('#add-todo-then-roll-back-done')
      // A committed note after it: once its list shows it, a refetch from the rollback would have shown too.
      await write(page, 'add-note-to-2', `${orm} after the rollback`)
      await expectList(page, 'notes-of-2', `${orm} after the rollback`)
      expect((await readFetches(page)).todos).toBe(before.todos)
      expect(await page.textContent('#todos-list')).not.toContain(`${orm} rolled back`)
    })

    test(`live-orm (${orm}): a write outside any telefunction refetches`, async () => {
      await open(page, url())
      await write(page, 'prepare-write-outside-request', `${orm} from a server job`)
      await page.waitForSelector('#prepare-write-outside-request-done')
      const response = await fetch(`${getServerUrl()}/api/server-action?key=live-orm:${orm}`, { method: 'POST' })
      expect(response.ok).toBe(true)
      await expectList(page, 'notes-of-1', `${orm} from a server job`)
      await expectList(page, 'watched-notes-of-1', `${orm} from a server job`)
    })
  }
}

/** Opens the page and waits until it is live: each server-rendered list fetched once more in the browser. */
async function open(p: Page, url: string) {
  await navigate(url, undefined, p)
  await waitForFetches(p, {
    todos: 2,
    'first-todos': 2,
    'todos-with-notes': 2,
    'notes-of-1': 2,
    'notes-of-2': 2,
    'watched-notes-of-1': 1,
  })
}

async function write(p: Page, button: string, text: string) {
  await p.fill('#text', text)
  await p.click(`#${button}`)
}

async function expectList(p: Page, list: List, text: string) {
  await autoRetry(async () => {
    expect(await p.textContent(`#${list}-list`)).toContain(text)
  })
}

async function readFetches(p: Page): Promise<Fetches> {
  const fetches = {} as Fetches
  for (const list of LISTS) fetches[list] = Number(await p.textContent(`#${list}-fetches`))
  return fetches
}

/** Waits until each list has completed at least the given number of fetches. */
async function waitForFetches(p: Page, atLeast: Partial<Fetches>): Promise<Fetches> {
  let fetches = {} as Fetches
  await autoRetry(async () => {
    fetches = await readFetches(p)
    for (const [list, count] of Object.entries(atLeast)) expect(fetches[list as List]).greaterThanOrEqual(count)
  })
  return fetches
}
