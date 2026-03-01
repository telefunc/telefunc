export { KitchenSink }

import React, { useEffect, useRef, useState } from 'react'
import { onKitchenSink } from './KitchenSink.telefunc'
import { withContext } from 'telefunc/client'

// Generate a repeating-pattern buffer of the given size
function makeBuffer(size: number, seed: string): ArrayBuffer {
  const enc = new TextEncoder()
  const pattern = enc.encode(seed)
  const buf = new Uint8Array(new ArrayBuffer(size))
  for (let i = 0; i < size; i++) buf[i] = pattern[i % pattern.length]!
  return buf.buffer as ArrayBuffer
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <tr className="border-b border-zinc-100 last:border-0">
      <td className="py-1.5 pr-4 text-xs font-medium text-zinc-500 whitespace-nowrap align-top w-32">{label}</td>
      <td className="py-1.5 text-xs font-mono text-zinc-800 break-all">{value}</td>
    </tr>
  )
}

type State = {
  // scalars — arrive immediately
  fileName?: string
  fileSize?: number
  blobSize?: number
  label?: string
  // fileEcho ReadableStream — accumulate decoded chunks
  echoChunks?: string[]
  echoDone?: boolean
  echoBytesReceived?: number
  echoChecksum?: number
  echoChecksumError?: string
  // progress AsyncGenerator — last progress event
  progressEvents?: number
  progressLast?: { bytesRead: number; total: number; percent: number; chunkIndex: number; checksum: number }
  progressDone?: boolean
  // summary Promise — resolves after progress drains the file
  summary?: { totalBytes: number; checksum: number; chunkCount: number; blobPreview: string; label: string }
  // midEcho Promise — resolves when echo crosses the 50 % mark
  midEcho?: { bytesEchoed: number; clientBytesAtResolve: number; diff: number }
  midEchoAssertError?: string
  error?: string
  abortedAt?: number
  cancelled?: boolean
}

function KitchenSink() {
  const [state, setState] = useState<State>({})
  const [running, setRunning] = useState(false)
  const [hydrated, setHydrated] = useState(false)
  const abortControllerRef = useRef<AbortController | null>(null)
  useEffect(() => setHydrated(true), [])

  const run = async () => {
    // 1 MB file with a recognisable repeating text pattern
    const file = new File([makeBuffer(1 * 1024 * 1024, 'kitchen sink')], 'demo-1mb.bin', {
      type: 'application/octet-stream',
    })
    // Small blob nested inside meta — contains readable text for the preview
    const blob = new Blob(
      ['Hello from the nested Blob! label=kitchen sink, generated at ' + new Date().toISOString()],
      {
        type: 'text/plain',
      },
    )
    const label = 'kitchen sink'

    setState({})
    abortControllerRef.current = null
    setRunning(true)

    try {
      const controller = new AbortController()
      abortControllerRef.current = controller
      const res = await withContext(onKitchenSink, { signal: controller.signal })(file, { label, blob })

      // Scalars are here as soon as the Promise resolves
      setState((s) => ({
        ...s,
        fileName: res.fileName,
        fileSize: res.fileSize,
        blobSize: res.blobSize,
        label: res.label,
        echoChunks: [],
        echoBytesReceived: 0,
        echoChecksum: 0,
        progressEvents: 0,
      }))

      const dec = new TextDecoder()

      await Promise.all([
        // AsyncGenerator — raw file bytes echoed back with independent throttle
        (async () => {
          let echoChecksum = 0
          const reader = res.fileEcho.getReader()
          try {
            while (true) {
              const { done, value } = await reader.read()
              if (done) break
              for (const b of value) echoChecksum = (echoChecksum + b) & 0x7fffffff
              setState((s) => ({
                ...s,
                echoChunks: [...(s.echoChunks ?? []), dec.decode(value, { stream: true })],
                echoBytesReceived: (s.echoBytesReceived ?? 0) + value.byteLength,
                echoChecksum,
              }))
            }
          } finally {
            reader.releaseLock()
          }
          setState((s) => ({ ...s, echoDone: true }))
        })(),

        // AsyncGenerator — live progress while server reads the file
        (async () => {
          for await (const p of res.progress) {
            setState((s) => ({ ...s, progressEvents: (s.progressEvents ?? 0) + 1, progressLast: p }))
          }
          setState((s) => ({ ...s, progressDone: true }))
        })(),

        // Promise — resolves after progress generator drains the stream
        res.summary.then((v) =>
          setState((s) => {
            const echoChecksumError =
              s.echoChecksum !== undefined && s.echoChecksum !== v.checksum
                ? `checksum mismatch: echo=0x${s.echoChecksum.toString(16)} summary=0x${v.checksum.toString(16)}`
                : undefined
            return { ...s, summary: v, echoChecksumError }
          }),
        ),

        // Promise — resolves mid-echo at 50 %
        res.midEcho.then((v) => {
          setState((s) => {
            const echoFinished = s.echoDone === true
            const clientBytes = s.echoBytesReceived ?? 0
            const diff = v.bytesEchoed - clientBytes
            const assertError = echoFinished
              ? `assertion failed: midEcho resolved after echo already finished`
              : undefined
            return { ...s, midEcho: { ...v, clientBytesAtResolve: clientBytes, diff }, midEchoAssertError: assertError }
          })
        }),
      ])
    } catch (e: any) {
      if (e.isCancel) {
        setState((s) => ({ ...s, cancelled: true }))
      } else {
        setState((s) => ({ ...s, error: e.message }))
      }
    } finally {
      setRunning(false)
      abortControllerRef.current = null
    }
  }

  const doAbort = () => {
    if (!abortControllerRef.current) return
    abortControllerRef.current.abort()
    setState((s) => ({ ...s, abortedAt: Date.now() }))
  }

  const p = state.progressLast

  return (
    <div>
      {/* Controls */}
      <div className="flex gap-2 mb-6">
        {hydrated && <span id="hydrated" />}
        <button onClick={run} disabled={running}>
          {running ? 'Running…' : 'Run (1 MB file + Blob)'}
        </button>
        <button onClick={doAbort} disabled={!running}>
          Abort
        </button>
      </div>

      {/* Results table */}
      {state.fileName !== undefined && (
        <table className="w-full text-left">
          <tbody>
            {/* Scalars */}
            <Row label="fileName" value={state.fileName} />
            <Row label="fileSize" value={`${state.fileSize?.toLocaleString()} bytes`} />
            <Row label="blobSize" value={`${state.blobSize?.toLocaleString()} bytes`} />
            <Row label="label" value={state.label} />

            {/* fileEcho ReadableStream */}
            <Row
              label={state.echoDone ? 'fileEcho ✓' : 'fileEcho …'}
              value={
                <div className="space-y-1">
                  <div className="w-64 bg-zinc-100 rounded-full h-1.5 overflow-hidden">
                    <div
                      className="h-full bg-blue-500 transition-[width] duration-100 ease-linear"
                      style={{
                        width: `${state.fileSize ? Math.round(((state.echoBytesReceived ?? 0) / state.fileSize) * 100) : 0}%`,
                      }}
                    />
                  </div>
                  <span>
                    {state.echoBytesReceived?.toLocaleString() ?? 0} / {state.fileSize?.toLocaleString()} bytes (
                    {state.fileSize ? Math.round(((state.echoBytesReceived ?? 0) / state.fileSize) * 100) : 0}
                    %){state.echoDone ? '' : '…'}
                  </span>
                </div>
              }
            />

            {/* progress AsyncGenerator */}
            {p && (
              <>
                <Row
                  label={state.progressDone ? 'progress ✓' : 'progress …'}
                  value={
                    <div className="space-y-1">
                      <div className="w-64 bg-zinc-100 rounded-full h-1.5 overflow-hidden">
                        <div
                          className="h-full bg-green-500 transition-[width] duration-100 ease-linear"
                          style={{ width: `${p.percent}%` }}
                        />
                      </div>
                      <span>
                        {p.bytesRead.toLocaleString()} / {p.total.toLocaleString()} bytes ({p.percent}%) —{' '}
                        {state.progressEvents} chunks — checksum 0x{p.checksum.toString(16).padStart(8, '0')}
                      </span>
                    </div>
                  }
                />
              </>
            )}

            {/* midEcho Promise */}
            <Row
              label={state.midEcho ? (state.midEchoAssertError ? 'midEcho ✗' : 'midEcho ✓') : 'midEcho'}
              value={
                state.midEcho ? (
                  state.midEchoAssertError ? (
                    <span className="text-red-600">{state.midEchoAssertError}</span>
                  ) : (
                    `server: ${state.midEcho.bytesEchoed.toLocaleString()} B sent — client: ${state.midEcho.clientBytesAtResolve.toLocaleString()} B received — diff: ${state.midEcho.diff.toLocaleString()} B ✓`
                  )
                ) : (
                  '(pending — resolves when echo crosses 50 %…)'
                )
              }
            />

            {/* summary Promise */}
            {state.summary ? (
              <Row
                label="summary ✓"
                value={
                  <span>
                    {state.summary.totalBytes.toLocaleString()} bytes, {state.summary.chunkCount} chunks, checksum 0x
                    {state.summary.checksum.toString(16).padStart(8, '0')}, blob preview: “{state.summary.blobPreview}”{' '}
                    {state.echoChecksumError ? (
                      <span className="text-red-600 ml-2">⚠ {state.echoChecksumError}</span>
                    ) : state.echoDone ? (
                      <span className="text-green-600 ml-2">✓ echo checksum matches</span>
                    ) : null}{' '}
                  </span>
                }
              />
            ) : (
              <Row label="summary" value="(pending — resolves after progress drains the file…)" />
            )}

            {state.abortedAt && <Row label="aborted at" value={new Date(state.abortedAt).toISOString()} />}
            {state.cancelled && (
              <Row
                label="cancelled"
                value={<span className="text-yellow-600">Call was cancelled (AbortController.abort())</span>}
              />
            )}
            {state.error && <Row label="error" value={<span className="text-red-600">{state.error}</span>} />}
          </tbody>
        </table>
      )}
    </div>
  )
}
