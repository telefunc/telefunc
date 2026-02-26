export { onSlowAIGenerator, onSlowStreamForAbort, onSlowNormalTelefunc, onUploadAbortSingle, onUploadAbortMultiple }

import { getContext } from 'telefunc'
import { cleanupState } from './cleanup-state'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

// Simulates a slow AI upstream: each "token" takes 1 second.
// Client disconnects after first token — onConnectionAbort should fire
// immediately, not after waiting 1s for the next token.
async function* onSlowAIGenerator(): AsyncGenerator<string> {
  try {
    cleanupState.slowAI = 'running'
    cleanupState.slowAIAbortedAt = ''
    const context = getContext()
    context.onConnectionAbort(() => {
      cleanupState.slowAI = 'cleaned-up'
      cleanupState.slowAIAbortedAt = String(Date.now())
    })
    // First token arrives quickly
    yield 'token-0'
    // Subsequent tokens take 1s each — client will disconnect before these
    for (let i = 1; i < 1000; i++) {
      await sleep(1_000)
      yield `token-${i}`
    }
  } finally {
    cleanupState.slowAIFinallyRan = 'true'
  }
}

// Simulates a slow ReadableStream: each chunk takes 5 seconds.
// Client cancels after first chunk — onConnectionAbort should fire quickly.
const onSlowStreamForAbort = async (): Promise<ReadableStream<Uint8Array>> => {
  cleanupState.slowStream = 'running'
  cleanupState.slowStreamAbortedAt = ''
  cleanupState.slowStreamCancelled = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    cleanupState.slowStream = 'cleaned-up'
    cleanupState.slowStreamAbortedAt = String(Date.now())
  })
  const encoder = new TextEncoder()
  let i = 0
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (i >= 1000) return controller.close()
      if (i > 0) await sleep(1_000)
      controller.enqueue(encoder.encode(`chunk-${i}`))
      i++
    },
    cancel() {
      cleanupState.slowStreamCancelled = 'true'
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

// ── Upload abort tests ──────────────────────────────────────────────

/** Single file upload — read slowly so client has time to abort mid-upload.
 *
 *  The sleep(100) between reads is critical: without it, a 1MB file on localhost
 *  would be consumed instantly (well within the ~4-16MB TCP buffer), completing
 *  before the client's 300ms abort fires. The sleep creates a window (~1.6s total
 *  for ~16 chunks) so the abort arrives mid-read, causing StreamReader.#pullChunk()
 *  to return null and throw "Client disconnected during file upload". */
async function onUploadAbortSingle(file: File): Promise<{ bytesRead: number; error: string | null }> {
  cleanupState.uploadAbortSingle = 'running'
  cleanupState.uploadAbortSingleError = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    cleanupState.uploadAbortSingle = 'cleaned-up'
  })
  let bytesRead = 0
  try {
    const reader = file.stream().getReader()
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      bytesRead += value.byteLength
      // Slow down so client can abort mid-upload
      await sleep(100)
    }
    cleanupState.uploadAbortSingleError = ''
    return { bytesRead, error: null }
  } catch (e: any) {
    const msg = e?.message || String(e)
    cleanupState.uploadAbortSingleError = msg
    return { bytesRead, error: msg }
  }
}

/** Multiple file upload — read each file fully, sleep between files.
 *  Client aborts during the sleep after file1. File2+file3 should error.
 *
 *  Unlike the single-file test, there is no sleep between reads within a file.
 *  On localhost, 50MB is consumed in well under 3s, so file1 completes fully
 *  before the client aborts at 3s. The 5s sleep after file1 is where the abort
 *  lands. When file2's stream starts pulling, StreamReader may still have
 *  leftover bytes in its internal buffer from a chunk that spanned the file1/file2
 *  boundary — so file2.bytesRead can be > 0 even though the connection is dead.
 *  The next #pullChunk() call hits the closed connection and throws. */
async function onUploadAbortMultiple(
  file1: File,
  file2: File,
  file3: File,
): Promise<{ results: Array<{ name: string; bytesRead: number; error: string | null }> }> {
  cleanupState.uploadAbortMulti = 'running'
  cleanupState.uploadAbortMultiConnectionAbort = ''
  const context = getContext()
  context.onConnectionAbort(() => {
    cleanupState.uploadAbortMulti = 'cleaned-up'
    cleanupState.uploadAbortMultiConnectionAbort = 'fired'
  })

  const results: Array<{ name: string; bytesRead: number; error: string | null }> = []

  for (const file of [file1, file2, file3]) {
    let bytesRead = 0
    try {
      const reader = file.stream().getReader()
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        bytesRead += value.byteLength
      }
      // Simulate slow processing between files — client aborts during this pause
      await sleep(5000)
      results.push({ name: file.name, bytesRead, error: null })
    } catch (e: any) {
      results.push({ name: file.name, bytesRead, error: e?.message || String(e) })
    }
  }

  // Store results in cleanup state for e2e assertions
  cleanupState.uploadAbortMultiResults = JSON.stringify(results)

  return { results }
}
