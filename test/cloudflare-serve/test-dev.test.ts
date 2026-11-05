import { autoRetry, expect, getServerUrl, run, test } from '@brillout/test-e2e'

run('pnpm run dev', { serverIsReadyMessage: 'Ready on' })

/** A Worker's timers die with the request that set them, so each step is a request of its own. */
const read = async (path: string) => {
  const response = await fetch(getServerUrl() + path, { signal: AbortSignal.timeout(10_000) })
  return Number(await response.text())
}

test("Broadcast: a listener's answer to an answer arrives, and publishes go on after the request ended", async () => {
  await read('/answer')
  await autoRetry(async () => {
    expect(await read('/answers')).toBe(1)
  })
  const before = await read('/publish')
  expect(await read('/publish')).toBe(before + 1)
})

test('Broadcast: publishes go on after a request crossed the count between turns', async () => {
  const crossed = await read('/publishes')
  expect(await read('/publish')).toBe(crossed + 1)
  expect(await read('/publish')).toBe(crossed + 2)
})

test('Room: messages go on arriving after a request crossed the count between turns', async () => {
  await read('/join')
  // One commit a request, as the count is crossed in one that ends at once only this way.
  const commits = 1_100
  for (let done = 0; done < commits; done += 8) {
    await Promise.all(Array.from({ length: Math.min(8, commits - done) }, () => read('/commit')))
  }
  await autoRetry(async () => {
    expect(await read('/frames')).toBe(commits)
  })
})
