export { testRefIdentity }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { navigate, getResult } from '../../e2e-utils'

type RefIdentityResult = {
  identity: {
    roomDupe: boolean
    roomList: boolean
    chDupe: boolean
    genDupe: boolean
  } | null
  chunkIdentity: boolean | null
  received: number[]
  pongs: string[]
}

function testRefIdentity() {
  test('reference identity: duplicated refs revive as one object with one subscription', async () => {
    await navigate(`${getServerUrl()}/ref-identity`)
    await page.click('#test-ref-identity')

    await autoRetry(async () => {
      const result = await getResult<RefIdentityResult>('#ref-identity-result')

      // Every occurrence of a duplicated reference is the same client object —
      // across payload keys, inside arrays, and for streamed values themselves.
      expect(result.identity).deep.equal({ roomDupe: true, roomList: true, chDupe: true, genDupe: true })

      // The same broadcast yielded inside async-generator chunks === the root one.
      expect(result.chunkIdentity).toBe(true)

      // One wire subscription: 3 publishes arrive exactly once each, in order.
      // Duplicated subscriptions would surface as duplicated values.
      expect(result.received).deep.equal([1, 2, 3])

      // The duplicated channel is live and duplex — and delivers exactly once.
      expect(result.pongs).deep.equal(['pong'])
    })
  })
}
