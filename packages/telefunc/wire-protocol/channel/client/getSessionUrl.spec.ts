import { expect, test } from 'vitest'
import { getWireUrl } from './getSessionUrl.js'
import { REQUEST_KIND, getMarkedRequestUrl, getSessionToken, namesWireVersion } from '../../wire/requestKind.js'

test("a configured telefuncUrl with a fragment: the server reads the SSE wire's session and version", () => {
  const sent = new URL(getMarkedRequestUrl(getWireUrl('http://app.test/_telefunc#rpc'), REQUEST_KIND.SSE))
  // A request never sends the fragment.
  sent.hash = ''
  const received = { url: sent.href }
  expect({ session: getSessionToken(received) !== undefined, wire: namesWireVersion(received) }).toEqual({
    session: true,
    wire: true,
  })
})
