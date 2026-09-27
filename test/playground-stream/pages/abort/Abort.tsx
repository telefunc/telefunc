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
  // The e2e tests abort these calls with the buttons below, once the server has them.
  const calls = useRef<Record<string, Promise<unknown>>>({})
  const startAbortable = (name: string, call: Promise<unknown>) => {
    calls.current[name] = call
    call.then(
      (res) => setResult(JSON.stringify({ result: res, error: null })),
      (e: any) => setResult(JSON.stringify({ error: e.message, isAbort: e instanceof TelefuncAbort })),
    )
  }
  const abortButton = (name: string, label: string) => (
    <button
      id={`test-${name}-abort`}
      onClick={() => {
        const call = calls.current[name]
        if (call) abort(call)
      }}
    >
      {label}
    </button>
  )
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
          startAbortable('slow-normal', onSlowNormalTelefunc())
        }}
      >
        Slow normal telefunc
      </button>

      {abortButton('slow-normal', 'Abort slow normal telefunc')}

      <h2>Upload abort tests</h2>

      <button
        id="test-upload-abort-single"
        onClick={() => {
          setResult('')
          // 1MB file — fits in localhost TCP buffer, but the server-side sleep(100)
          // between reads stretches consumption to ~1.6s, giving abortion time to land
          const content = 'x'.repeat(1_000_000)
          const file = new File([content], 'abort-test.txt', { type: 'text/plain' })
          startAbortable('upload-abort-single', onUploadAbortSingle(file))
        }}
      >
        Upload abort (single file)
      </button>

      {abortButton('upload-abort-single', 'Abort upload (single file)')}

      <button
        id="test-upload-abort-multiple"
        onClick={() => {
          setResult('')
          // 50MB per file — exceeds localhost TCP buffer (~4-16MB)
          const content = 'y'.repeat(50_000_000)
          const file1 = new File([content], 'file1.txt', { type: 'text/plain' })
          const file2 = new File([content], 'file2.txt', { type: 'text/plain' })
          const file3 = new File([content], 'file3.txt', { type: 'text/plain' })
          startAbortable('upload-abort-multiple', onUploadAbortMultiple(file1, file2, file3))
        }}
      >
        Upload abort (multiple files)
      </button>

      {abortButton('upload-abort-multiple', 'Abort upload (multiple files)')}
    </div>
  )
}
