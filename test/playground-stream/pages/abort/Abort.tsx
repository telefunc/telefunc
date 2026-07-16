export { Abort }

import React, { useEffect, useRef, useState } from 'react'
import {
  onSlowAIGenerator,
  onSlowStreamForAbort,
  onSlowNormalTelefunc,
  onUploadAbortSingle,
  onUploadAbortMultiple,
} from './Abort.telefunc'
import { Abort as TelefuncAbort, abort, withContext } from 'telefunc/client'

function Abort() {
  const [hydrated, setHydrated] = useState(false)
  const [result, setResult] = useState<string>('')
  // The in-flight non-streaming call, stashed so the e2e test can abort it deterministically once it has
  // confirmed (by polling server cleanup-state) that the telefunc is running — see the button below.
  const slowNormalCall = useRef<ReturnType<typeof onSlowNormalTelefunc> | null>(null)
  useEffect(() => setHydrated(true), [])

  return (
    <div>
      {hydrated && <span id="hydrated" />}
      <pre id="abort-result">{result}</pre>

      <h2>Generator abort tests</h2>

      <button
        id="test-generator-abort-fn"
        onClick={async () => {
          setResult('')
          const gen = onSlowAIGenerator()
          const values: string[] = []
          const first = await gen.next()
          if (!first.done) values.push(first.value)
          const nextPromise = gen.next()
          setTimeout(() => abort(gen), 500)
          try {
            const r = await nextPromise
            setResult(
              JSON.stringify({ method: 'abort(gen)', values, nextValue: r.value, nextDone: r.done, error: null }),
            )
          } catch (e: any) {
            setResult(
              JSON.stringify({ method: 'abort(gen)', values, error: e.message, isAbort: e instanceof TelefuncAbort }),
            )
          }
        }}
      >
        Generator: abort(gen)
      </button>

      <button
        id="test-generator-return"
        onClick={async () => {
          setResult('')
          const gen = onSlowAIGenerator()
          const values: string[] = []
          const first = await gen.next()
          if (!first.done) values.push(first.value)
          const nextPromise = gen.next()
          setTimeout(() => gen.return(undefined), 500)
          try {
            const r = await nextPromise
            setResult(
              JSON.stringify({ method: 'gen.return()', values, nextValue: r.value, nextDone: r.done, error: null }),
            )
          } catch (e: any) {
            setResult(
              JSON.stringify({ method: 'gen.return()', values, error: e.message, isAbort: e instanceof TelefuncAbort }),
            )
          }
        }}
      >
        Generator: gen.return()
      </button>

      <button
        id="test-generator-withContext"
        onClick={async () => {
          setResult('')
          const controller = new AbortController()
          const gen = withContext(onSlowAIGenerator, { signal: controller.signal })()
          const values: string[] = []
          const first = await gen.next()
          if (!first.done) values.push(first.value)
          const nextPromise = gen.next()
          setTimeout(() => controller.abort(), 500)
          try {
            const r = await nextPromise
            setResult(
              JSON.stringify({
                method: 'withContext(gen, signal)',
                values,
                nextValue: r.value,
                nextDone: r.done,
                error: null,
              }),
            )
          } catch (e: any) {
            setResult(
              JSON.stringify({
                method: 'withContext(gen, signal)',
                values,
                error: e.message,
                isAbort: e instanceof TelefuncAbort,
              }),
            )
          }
        }}
      >
        Generator: withContext
      </button>

      <h2>Stream abort tests</h2>

      <button
        id="test-stream-reader-cancel"
        onClick={async () => {
          setResult('')
          const stream = await onSlowStreamForAbort()
          const reader = stream.getReader()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          const first = await reader.read()
          if (!first.done) chunks.push(decoder.decode(first.value, { stream: true }))
          setTimeout(() => reader.cancel(), 500)
          try {
            const { done, value } = await reader.read()
            if (!done && value) chunks.push(decoder.decode(value, { stream: true }))
            setResult(JSON.stringify({ method: 'reader.cancel()', chunks, readDone: done, error: null }))
          } catch (e: any) {
            setResult(JSON.stringify({ method: 'reader.cancel()', chunks, error: e.message }))
          }
        }}
      >
        Stream: reader.cancel()
      </button>

      <button
        id="test-stream-withContext"
        onClick={async () => {
          setResult('')
          const controller = new AbortController()
          const stream = await withContext(onSlowStreamForAbort, { signal: controller.signal })()
          const reader = stream.getReader()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          const first = await reader.read()
          if (!first.done) chunks.push(decoder.decode(first.value, { stream: true }))
          setTimeout(() => controller.abort(), 500)
          try {
            const { done, value } = await reader.read()
            if (!done) chunks.push(decoder.decode(value, { stream: true }))
            setResult(JSON.stringify({ method: 'withContext(stream, signal)', chunks, readDone: done, error: null }))
          } catch (e: any) {
            setResult(
              JSON.stringify({
                method: 'withContext(stream, signal)',
                chunks,
                error: e.message,
                isAbort: e instanceof TelefuncAbort,
              }),
            )
          }
        }}
      >
        Stream: withContext
      </button>

      <h2>Non-streaming abort</h2>

      <button
        id="test-slow-normal-telefunc"
        onClick={() => {
          setResult('')
          // Fire the call and stash it. The abort is triggered separately (button below), driven by the
          // e2e test AFTER it confirms the telefunc is running server-side. The old fixed
          // `setTimeout(() => abort(promise), 1500)` raced call-establishment on slow preview variants:
          // when the abort fired before/around server start it was effectively lost, the call ran to
          // normal completion, and `isAbort` came back undefined ("expected undefined to equal true").
          const promise = onSlowNormalTelefunc()
          slowNormalCall.current = promise
          promise.then(
            (res) => setResult(JSON.stringify({ result: res, error: null })),
            (e: any) => setResult(JSON.stringify({ error: e.message, isAbort: e instanceof TelefuncAbort })),
          )
        }}
      >
        Slow normal telefunc
      </button>

      <button
        id="test-slow-normal-abort"
        onClick={() => {
          if (slowNormalCall.current) abort(slowNormalCall.current)
        }}
      >
        Abort slow normal telefunc
      </button>

      <h2>Upload abort tests</h2>

      <button
        id="test-upload-abort-single"
        onClick={async () => {
          setResult('')
          // 1MB file — fits in localhost TCP buffer, but the server-side sleep(100)
          // between reads stretches consumption to ~1.6s, giving abortion time to land
          const content = 'x'.repeat(1_000_000)
          const file = new File([content], 'abort-test.txt', { type: 'text/plain' })
          const promise = onUploadAbortSingle(file)
          setTimeout(() => abort(promise), 300)
          try {
            const res = await promise
            setResult(JSON.stringify({ result: res, error: null }))
          } catch (e: any) {
            setResult(JSON.stringify({ error: e.message, isAbort: e instanceof TelefuncAbort }))
          }
        }}
      >
        Upload abort (single file)
      </button>

      <button
        id="test-upload-abort-multiple"
        onClick={async () => {
          setResult('')
          // 50MB per file — exceeds localhost TCP buffer (~4-16MB)
          const content = 'y'.repeat(50_000_000)
          const file1 = new File([content], 'file1.txt', { type: 'text/plain' })
          const file2 = new File([content], 'file2.txt', { type: 'text/plain' })
          const file3 = new File([content], 'file3.txt', { type: 'text/plain' })
          const promise = onUploadAbortMultiple(file1, file2, file3)
          // Abort after 3s — server reads file1 then sleeps 5s, abort fires during sleep
          setTimeout(() => abort(promise), 3000)
          try {
            const res = await promise
            setResult(JSON.stringify({ result: res, error: null }))
          } catch (e: any) {
            setResult(JSON.stringify({ error: e.message, isAbort: e instanceof TelefuncAbort }))
          }
        }}
      >
        Upload abort (multiple files)
      </button>
    </div>
  )
}
