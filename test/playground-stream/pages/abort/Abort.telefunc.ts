export { onSlowAIGenerator, onSlowStreamForAbort, onSlowNormalTelefunc, onUploadAbortSingle, onUploadAbortMultiple }

import { getContext } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { sleep } from '../../sleep'

/** Past what an abort takes to reach the server on a loaded machine; a generator's finally runs once it ends. */
const TOKEN_MS = 3_000

// Simulates a slow AI upstream: each "token" takes 3 seconds.
// Client disconnects after first token — onClose should fire
// while the generator waits for the next token, not once it comes.
async function* onSlowAIGenerator(): AsyncGenerator<string> {
  let waiting = false
  try {
    cleanupState.slowAI = 'running'
    cleanupState.slowAIClosedWaiting = ''
    const context = getContext()
    context.onClose(() => {
      cleanupState.slowAI = 'cleaned-up'
      cleanupState.slowAIClosedWaiting = String(waiting)
    })
    yield 'token-0'
    for (let i = 1; i < 1000; i++) {
      waiting = true
      await sleep(TOKEN_MS)
      waiting = false
      yield `token-${i}`
    }
  } finally {
    cleanupState.slowAIFinallyRan = 'true'
  }
}

// Simulates a slow ReadableStream: each chunk takes 3 seconds.
// Client cancels after first chunk — onClose should fire while the stream waits for the next chunk.
const onSlowStreamForAbort = async (): Promise<ReadableStream<Uint8Array>> => {
  let waiting = false
  cleanupState.slowStream = 'running'
  cleanupState.slowStreamClosedWaiting = ''
  cleanupState.slowStreamCancelled = ''
  const context = getContext()
  context.onClose(() => {
    cleanupState.slowStream = 'cleaned-up'
    cleanupState.slowStreamClosedWaiting = String(waiting)
  })
  const encoder = new TextEncoder()
  let i = 0
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (i >= 1000) return controller.close()
      if (i > 0) {
        waiting = true
        await sleep(TOKEN_MS)
        waiting = false
      }
      controller.enqueue(encoder.encode(`chunk-${i}`))
      i++
    },
    cancel() {
      cleanupState.slowStreamCancelled = 'true'
    },
  })
}

// Non-streaming telefunc with many slow steps.
// Client aborts — onClose fires, and the telefunc
// checks an `aborted` flag to bail out early.
async function onSlowNormalTelefunc(): Promise<{ stepsCompleted: number }> {
  cleanupState.slowNormal = 'running'
  cleanupState.slowNormalSteps = '0'
  const context = getContext()
  let aborted = false
  context.onClose(() => {
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

// Single file upload — read slowly so client has time to abort mid-upload.
// The sleep(100) between reads stretches consumption to ~1.6s for ~16 chunks,
// giving the client's abort time to arrive mid-read.
async function onUploadAbortSingle(file: File): Promise<{ bytesRead: number; error: string | null }> {
  cleanupState.uploadAbortSingle = 'running'
  cleanupState.uploadAbortSingleError = ''
  const context = getContext()
  context.onClose(() => {
    cleanupState.uploadAbortSingle = 'cleaned-up'
  })
  let bytesRead = 0
  try {
    const reader = file.stream().getReader()
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      bytesRead += value.byteLength
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

// Multiple file upload — read each file fully, sleep between files.
// Client aborts during the sleep after file1. File2+file3 should error.
// The 5s sleep after file1 is where the abort lands. File2/file3 may have
// leftover bytes in StreamReader's buffer from chunk boundaries.
async function onUploadAbortMultiple(
  file1: File,
  file2: File,
  file3: File,
): Promise<{ results: Array<{ name: string; bytesRead: number; error: string | null }> }> {
  cleanupState.uploadAbortMulti = 'running'
  cleanupState.uploadAbortMultiConnectionClose = ''
  const context = getContext()
  context.onClose(() => {
    cleanupState.uploadAbortMulti = 'cleaned-up'
    cleanupState.uploadAbortMultiConnectionClose = 'fired'
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
      cleanupState.uploadAbortMultiFilesRead = String(results.length + 1)
      await sleep(5000)
      results.push({ name: file.name, bytesRead, error: null })
    } catch (e: any) {
      results.push({ name: file.name, bytesRead, error: e?.message || String(e) })
    }
  }

  cleanupState.uploadAbortMultiResults = JSON.stringify(results)
  return { results }
}
