export { onMixedForClose, onCloseGen, onCloseStream, onCloseChannel, onCloseFn }

import { createChannel } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { sleep } from '../../sleep'

// ── Targeted single-type telefunctions ─────────────────────────────

async function* onCloseGen(): AsyncGenerator<string> {
  try {
    cleanupState.closeGen = 'running'
    yield 'token-0'
    for (let i = 1; i < 1000; i++) {
      await sleep(1_000)
      yield `token-${i}`
    }
  } finally {
    cleanupState.closeGenFinallyRan = 'true'
  }
}

async function onCloseStream(): Promise<ReadableStream<Uint8Array>> {
  cleanupState.closeStream = 'running'
  cleanupState.closeStreamCancelled = ''
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
      cleanupState.closeStreamCancelled = 'true'
    },
  })
}

async function onCloseChannel() {
  const channel = createChannel<never, never>()
  cleanupState.closeChannel_onCloseErr = 'pending'
  channel.onClose((err) => {
    cleanupState.closeChannel_onCloseErr = err ? ((err as any).message ?? 'error') : 'none'
  })
  return channel.client
}

async function onCloseFn(callback: () => void) {
  callback()
  cleanupState.closeFn_cbCalled = 'true'
  return () => {
    cleanupState.closeFn_retFnCalled = 'true'
  }
}

// ── Mixed ────────────────────────────────────────────────────────────

/**
 * Big mixed telefunction: takes a callback (passed fn) and returns
 * { generator, stream, channel, fn } (returned fn).
 * Tests that close(result) cleanly closes all value types in one shot.
 */
async function onMixedForClose(callback: (msg: string) => void) {
  // Call the passed-in callback — proves client→server→client function round-trip
  callback('hello-from-server')
  cleanupState.closeMixed_cbCalled = 'true'

  // Async generator — yields tokens 1 s apart; finally block tracks clean close
  const generator = (async function* (): AsyncGenerator<string> {
    try {
      cleanupState.closeMixed_gen = 'running'
      yield 'token-0'
      for (let i = 1; i < 1000; i++) {
        await sleep(1_000)
        yield `token-${i}`
      }
    } finally {
      cleanupState.closeMixed_genFinallyRan = 'true'
    }
  })()

  // ReadableStream — yields chunks 1 s apart; cancel callback tracks clean close
  cleanupState.closeMixed_stream = 'running'
  const encoder = new TextEncoder()
  let chunkI = 0
  const stream = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (chunkI >= 1000) return controller.close()
      if (chunkI > 0) await sleep(1_000)
      controller.enqueue(encoder.encode(`chunk-${chunkI}`))
      chunkI++
    },
    cancel() {
      cleanupState.closeMixed_streamCancelled = 'true'
    },
  })

  // Channel — close(result) closes it cleanly; onClose tracks no-error close
  const channel = createChannel<never, never>()
  cleanupState.closeMixed_channel_onCloseErr = 'pending'
  channel.onClose((err) => {
    cleanupState.closeMixed_channel_onCloseErr = err ? ((err as any).message ?? 'error') : 'none'
  })

  // Returned function — callable by client; close(result) closes its backing channel
  const fn = () => {
    cleanupState.closeMixed_retFnCalled = 'true'
  }

  return { generator, stream, channel: channel.client, fn }
}
