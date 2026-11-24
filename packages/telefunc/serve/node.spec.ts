import { expect, test } from 'vitest'
import { Telefunc } from './node.js'

test('`serve({ request })` leaves a request for another path to the app', async () => {
  const telefunc = new Telefunc()
  for (const path of ['/other', '//_telefunc', '/_telefunc/x'])
    expect(
      await telefunc.serve({ request: new Request(`http://localhost${path}`, { method: 'POST', body: '{}' }) }),
    ).toBe(undefined)
})
