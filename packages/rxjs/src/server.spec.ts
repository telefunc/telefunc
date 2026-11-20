import { afterEach, expect, test, vi } from 'vitest'
import { Subject } from 'rxjs'
import { ChannelPageLeftError, NetworkError } from 'telefunc'
import './server.js'

const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
afterEach(() => {
  consoleError.mockClear()
})

/** A Subject a telefunction subscribed to without an error handler, ended with `err` as its channel ends it: what
 *  the log got, and what a later subscriber gets. */
async function endUnhandled(err: unknown) {
  const subject = new Subject<unknown>()
  subject.subscribe(() => {})
  subject.error(err)
  // rxjs reports an unhandled error on a timer.
  await new Promise((resolve) => setTimeout(resolve, 0))
  let errored: unknown
  subject.subscribe({ error: (e) => (errored = e) })
  const logged = consoleError.mock.calls.filter(([first]) => String(first).startsWith('[telefunc:rxjs]'))
  return { logged, errored }
}

test('a Subject whose page left, or whose connection was lost, is errored without a line in the log', async () => {
  for (const err of [new ChannelPageLeftError(), new NetworkError('Channel lost', true)])
    expect(await endUnhandled(err)).toEqual({ logged: [], errored: err })
})

test('a Subject errored by anything else logs it', async () => {
  const err = new Error('the telefunction failed')
  expect(await endUnhandled(err)).toEqual({ logged: [['[telefunc:rxjs] Unhandled rxjs error:', err]], errored: err })
})
