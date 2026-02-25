export { onSlowAIGenerator, onSlowStreamForAbort, onSlowNormalTelefunc }

import { getContext } from 'telefunc'
import { cleanupState } from './cleanup-state'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

// Simulates a slow AI upstream: each "token" takes 10 seconds.
// Client disconnects after first token — onConnectionAbort should fire
// immediately, not after waiting 10s for the next token.
async function* onSlowAIGenerator(): AsyncGenerator<string> {
  cleanupState.slowAI = 'running'
  cleanupState.slowAIAbortedAt = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    cleanupState.slowAI = 'cleaned-up'
    cleanupState.slowAIAbortedAt = String(Date.now())
  })
  // First token arrives quickly
  yield 'token-0'
  // Subsequent tokens take 10s each — client will disconnect before these
  for (let i = 1; i < 100; i++) {
    await sleep(10_000)
    yield `token-${i}`
  }
}

// Simulates a slow ReadableStream: each chunk takes 5 seconds.
// Client cancels after first chunk — onConnectionAbort should fire quickly.
const onSlowStreamForAbort = async (): Promise<ReadableStream<Uint8Array>> => {
  cleanupState.slowStream = 'running'
  cleanupState.slowStreamAbortedAt = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    cleanupState.slowStream = 'cleaned-up'
    cleanupState.slowStreamAbortedAt = String(Date.now())
  })
  const encoder = new TextEncoder()
  let i = 0
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (i >= 100) return controller.close()
      if (i > 0) await sleep(5_000)
      controller.enqueue(encoder.encode(`chunk-${i}`))
      i++
    },
  })
}

// Non-streaming telefunc with many slow steps.
// Client navigates away — onConnectionAbort fires, and the telefunc
// can check an `aborted` flag to bail out early.
async function onSlowNormalTelefunc(): Promise<{ stepsCompleted: number }> {
  cleanupState.slowNormal = 'running'
  cleanupState.slowNormalSteps = '0'
  const context = getContext()
  let aborted = false
  context.onConnectionAbort(() => {
    aborted = true
    cleanupState.slowNormal = 'cleaned-up'
  })
  let steps = 0
  for (let i = 0; i < 20; i++) {
    await sleep(500)
    steps++
    cleanupState.slowNormalSteps = String(steps)
    if (aborted) break
  }
  return { stepsCompleted: steps }
}
