export { onKitchenSink }

import { getContext } from 'telefunc'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

// Kitchen-sink telefunction — everything at once:
//
//  Inputs
//    • file   — large File, consumed via file.stream() (never fully buffered)
//    • meta   — { label: string, blob: Blob } — blob read eagerly (small)
//
//  Outputs (all multiplexed on one HTTP response simultaneously)
//    • scalars    — fileName, fileSize, blobSize, label  (immediate)
//    • fileEcho   — ReadableStream<Uint8Array>  — one half of file.stream().tee(),
//                   raw bytes piped back to the client with zero extra buffering
//    • progress   — AsyncGenerator<{bytesRead,total,percent,chunkIndex,checksum}>
//                   consumes the other .tee() half, yielding live updates as each
//                   chunk arrives; computes a rolling CRC-like checksum on the fly
//    • midEcho    — Promise<{bytesEchoed}>
//                   resolves mid-stream once ≥50 % of file bytes have been echoed
//    • summary    — Promise<{totalBytes,checksum,chunkCount,blobPreview,label}>
//                   resolves only after the progress generator has drained the file;
//                   the Promise and generator share state through a closure
//
//  onConnectionAbort — signals the server to stop processing on client disconnect;
//                      the progress generator checks the flag between chunks

async function onKitchenSink(file: File, meta: { label: string; blob: Blob }) {
  const context = getContext()
  let aborted = false
  context.onConnectionAbort(() => {
    aborted = true
  })

  // Split the file stream into two independent readers.
  // echoStream goes straight back to the client as raw bytes.
  // processStream is consumed chunk-by-chunk in the progress generator.
  const [echoStream, processStream] = file.stream().tee()

  // summary resolves (from inside the generator) once the file is fully processed
  let summaryResolve!: (v: {
    totalBytes: number
    checksum: number
    chunkCount: number
    blobPreview: string
    label: string
  }) => void
  let summaryReject!: (e: unknown) => void
  const summary = new Promise<{
    totalBytes: number
    checksum: number
    chunkCount: number
    blobPreview: string
    label: string
  }>((res, rej) => {
    summaryResolve = res
    summaryReject = rej
  })

  // Reads processStream chunk-by-chunk, yielding a progress event per chunk.
  // Drives the summary Promise to resolution when the stream ends.
  async function* progress(): AsyncGenerator<{
    bytesRead: number
    total: number
    percent: number
    chunkIndex: number
    checksum: number
  }> {
    let bytesRead = 0
    let checksum = 0
    let chunkIndex = 0
    try {
      for await (const chunk of processStream) {
        if (aborted) break
        bytesRead += chunk.byteLength
        // Rolling additive checksum — cheap but visible in the UI
        for (const b of chunk) checksum = (checksum + b) & 0x7fffffff
        // Simulate I/O processing time so progress events are visible in the UI
        await sleep(50)
        yield { bytesRead, total: file.size, percent: Math.round((bytesRead / file.size) * 100), chunkIndex, checksum }
        chunkIndex++
      }
      // Blob comes after file in the argument list — read it only after the file
      // stream is fully consumed, which satisfies the forward-only order constraint.
      const blobText = await meta.blob.text()
      summaryResolve({
        totalBytes: bytesRead,
        checksum,
        chunkCount: chunkIndex,
        blobPreview: blobText.slice(0, 120),
        label: meta.label,
      })
    } catch (e) {
      summaryReject(e)
      throw e
    }
  }

  // midEcho resolves once ≥50 % of file bytes have been delivered through the
  // throttled echo stream, so it resolves mid-stream — not immediately.
  let midEchoResolve!: (v: { bytesEchoed: number }) => void
  const midEcho = new Promise<{ bytesEchoed: number }>((res) => {
    midEchoResolve = res
  })

  // Throttle the echo stream independently — different delay from progress (12 ms)
  // so the two streams flex at visibly different rates in the UI.
  const reader = echoStream.getReader()
  let bytesSent = 0
  let midEchoFired = false
  const throttledEcho = new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (aborted) {
        controller.close()
        return
      }
      const { done, value } = await reader.read()
      if (done) {
        controller.close()
        return
      }
      await sleep(20)
      bytesSent += value.byteLength
      if (!midEchoFired && bytesSent >= file.size / 2) {
        midEchoFired = true
        midEchoResolve({ bytesEchoed: bytesSent })
      }
      controller.enqueue(value)
    },
    cancel() {
      reader.cancel()
    },
  })

  return {
    // Scalars — arrive on the client before any streaming data
    fileName: file.name,
    fileSize: file.size,
    blobSize: meta.blob.size,
    label: meta.label,
    // ReadableStream — raw file bytes echoed back with independent throttle (5 ms/chunk)
    fileEcho: throttledEcho,
    // Promise — resolves when the echo stream crosses the 50 % mark
    midEcho,
    // AsyncGenerator — live progress while the server reads the file
    progress: progress(),
    // Promise — resolves after progress drains the stream; carries the final stats
    summary,
  }
}
