import { expect, fetchHtml, test } from '@brillout/test-e2e'
import { testRun } from '../vike/.testRun'
process.env.TEST_SSR_CONTEXT = '1'
testRun('pnpm run dev')

test('useData reads the provided SSR context or the ambient request context', async () => {
  expect(await fetchHtml('/?scope=provider')).toContain('<p id="profile">Nora</p>')
  expect(await fetchHtml('/')).toContain('<p id="profile">Ada</p>')
})
