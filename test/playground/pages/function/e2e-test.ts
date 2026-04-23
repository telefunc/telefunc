export { testFunction }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { waitForHydration, getResult } from '../../e2e-utils'

function testFunction() {
  test('function: simple greeter returns correct string', async () => {
    await page.goto(`${getServerUrl()}/function`)
    await waitForHydration()

    await page.click('#greeter-run')

    await autoRetry(async () => {
      const result = await getResult<string>('#greeter-result')
      expect(result).toBe('Hello, World! (from server)')
    })
  })

  test('function: stateful adder accumulates server-side state across calls', async () => {
    await page.click('#adder-run')

    await autoRetry(async () => {
      const results = await getResult<number[]>('#adder-results')
      expect(results).to.deep.equal([15, 18, 20])
    })
  })

  test('function: server-side call counter increments across multiple calls', async () => {
    await page.click('#echo-run')

    await autoRetry(async () => {
      const results = await getResult<{ echo: string; callCount: number }[]>('#echo-results')
      expect(results[0]).deep.equal({ echo: 'hello', callCount: 1 })
      expect(results[1]).deep.equal({ echo: 'world', callCount: 2 })
    })
  })

  test('function: pass function as argument — map', async () => {
    await page.click('#map-run')

    await autoRetry(async () => {
      const result = await getResult<number[]>('#map-result')
      expect(result).to.deep.equal([1, 4, 9, 16, 25])
    })
  })

  test('function: pass function as argument — reduce', async () => {
    await page.click('#reduce-run')

    await autoRetry(async () => {
      const result = await getResult<number>('#reduce-result')
      expect(result).toBe(15)
    })
  })

  test('function: shield validates returned function args', async () => {
    await page.click('#shield-return-run')

    await autoRetry(async () => {
      // Valid call should succeed
      const ok = await getResult<number>('#shield-return-ok')
      expect(ok).toBe(7)

      // Invalid call (strings instead of numbers) — the server-side listener returns a
      // marker object (`FN_SHIELD_ERROR_KEY`) as its ack payload and the client-side
      // function reviver throws with the validator's message.
      const error = await getResult<string>('#shield-return-error')
      expect(error).not.toBe('NO_ERROR')
      expect(error).toMatch(/shield validation error/i)
    })
  })

  test('function: shield validates client-callback return value', async () => {
    await page.click('#shield-callback-run')

    await autoRetry(async () => {
      // Valid callback return passes through.
      const ok = await getResult<number>('#shield-callback-ok')
      expect(ok).toBe(42)

      // When the client-provided callback returns a bad type the server's reviver throws a
      // branded ShieldValidationError; the runTelefunc boundary surfaces it as the canonical
      // 422 so the client sees "Shield Validation Error" rather than a generic bug-error.
      const error = await getResult<string>('#shield-callback-error')
      expect(error).not.toMatch(/^NO_ERROR/)
      expect(error).toMatch(/shield validation error/i)
    })
  })

  test('function: file upload with progress callback receives incremental updates', async () => {
    await page.click('#upload-run')

    await autoRetry(async () => {
      const result = await getResult<{ name: string; size: number }>('#upload-result')
      expect(result).to.deep.equal({ name: 'test-upload.txt', size: 1024 * 100 })

      const progress = await getResult<number[]>('#upload-progress')
      // Should have received at least one progress update
      expect(progress.length).greaterThan(0)
      // Last progress update should be 100%
      expect(progress[progress.length - 1]).toBe(100)
      // Progress should be monotonically increasing
      for (let i = 1; i < progress.length; i++) {
        expect(progress[i]!).greaterThanOrEqual(progress[i - 1]!)
      }
    })
  })
}
