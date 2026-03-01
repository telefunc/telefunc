export { Streaming }

import React, { useEffect, useState } from 'react'
import {
  onReturnReadableStream,
  onReturnAsyncGenerator,
  onReturnGeneratorWithMeta,
  onReturnEmptyGenerator,
  onReturnDelayedStream,
  onReturnDelayedGenerator,
  onReturnDelayedGeneratorWithMeta,
  onReturnStreamWithMeta,
  onReturnTwoGenerators,
  onReturnStreamAndGenerator,
  onReturnMultiplePromises,
  onReturnMixedEndless,
  onReturnDeadlockStream,
  onReturnAsymmetricGenerators,
  onGeneratorAbortMidStream,
  onGeneratorAbortWithValue,
  onGeneratorBugMidStream,
  onUploadWithProgress,
} from './Streaming.telefunc'

function Streaming() {
  const [result, setResult] = useState<string>('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div>
      <h2>Streaming Tests (Server → Client)</h2>
      {hydrated && <span id="hydrated" />}

      <button
        id="test-readable-stream"
        onClick={async () => {
          const stream = await onReturnReadableStream()
          const reader = stream.getReader()
          const chunks: string[] = []
          const decoder = new TextDecoder()
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            chunks.push(decoder.decode(value, { stream: true }))
          }
          chunks.push(decoder.decode())
          setResult(JSON.stringify({ content: chunks.join(''), chunkCount: chunks.length - 1 }))
        }}
      >
        ReadableStream
      </button>

      <button
        id="test-async-generator"
        onClick={async () => {
          const gen = onReturnAsyncGenerator()
          const values: number[] = []
          for await (const v of gen) {
            values.push(v)
          }
          setResult(JSON.stringify({ values, count: values.length }))
        }}
      >
        AsyncGenerator
      </button>

      <button
        id="test-generator-with-meta"
        onClick={async () => {
          const res = await onReturnGeneratorWithMeta()
          const messages: string[] = []
          for await (const v of res.stream) {
            messages.push(v)
          }
          setResult(JSON.stringify({ messages, timestamp: res.timestamp, tags: res.tags }))
        }}
      >
        Generator + meta
      </button>

      <button
        id="test-stream-with-meta"
        onClick={async () => {
          setResult('')
          const res = await onReturnStreamWithMeta()
          const reader = res.stream.getReader()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            chunks.push(decoder.decode(value, { stream: true }))
          }
          setResult(JSON.stringify({ chunks, count: res.count }))
        }}
      >
        ReadableStream + meta
      </button>

      <button
        id="test-empty-generator"
        onClick={async () => {
          const gen = onReturnEmptyGenerator()
          const values: number[] = []
          for await (const v of gen) {
            values.push(v)
          }
          setResult(JSON.stringify({ values, count: values.length }))
        }}
      >
        Empty generator
      </button>

      <pre id="streaming-result">{result}</pre>

      <h2>Async Streaming Tests (values over time)</h2>

      <button
        id="test-delayed-stream"
        onClick={async () => {
          setResult('')
          const stream = await onReturnDelayedStream()
          const reader = stream.getReader()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            const chunk = decoder.decode(value, { stream: true })
            chunks.push(chunk)
            setResult(JSON.stringify({ chunks: [...chunks], done: false }))
          }
          setResult(JSON.stringify({ chunks, done: true }))
        }}
      >
        Delayed ReadableStream
      </button>

      <button
        id="test-delayed-generator"
        onClick={async () => {
          setResult('')
          const gen = onReturnDelayedGenerator()
          const values: string[] = []
          for await (const v of gen) {
            values.push(v)
            setResult(JSON.stringify({ values: [...values], done: false }))
          }
          setResult(JSON.stringify({ values, done: true }))
        }}
      >
        Delayed AsyncGenerator
      </button>

      <button
        id="test-delayed-generator-meta"
        onClick={async () => {
          setResult('')
          const res = await onReturnDelayedGeneratorWithMeta()
          const values: number[] = []
          for await (const v of res.stream) {
            values.push(v)
            setResult(JSON.stringify({ label: res.label, values: [...values], done: false }))
          }
          setResult(JSON.stringify({ label: res.label, values, done: true }))
        }}
      >
        Delayed Generator + meta
      </button>

      <h2>Multiplexed streaming tests</h2>

      <button
        id="test-two-generators"
        onClick={async () => {
          setResult('')
          const res = await onReturnTwoGenerators()
          const first: number[] = []
          const second: number[] = []
          await Promise.all([
            (async () => {
              for await (const v of res.first) first.push(v)
            })(),
            (async () => {
              for await (const v of res.second) second.push(v)
            })(),
          ])
          setResult(JSON.stringify({ first, second }))
        }}
      >
        Two generators
      </button>

      <button
        id="test-stream-and-generator"
        onClick={async () => {
          setResult('')
          const res = await onReturnStreamAndGenerator()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          const values: number[] = []
          await Promise.all([
            (async () => {
              const reader = res.stream.getReader()
              while (true) {
                const { done, value } = await reader.read()
                if (done) break
                chunks.push(decoder.decode(value, { stream: true }))
              }
            })(),
            (async () => {
              for await (const v of res.gen) values.push(v)
            })(),
          ])
          setResult(JSON.stringify({ chunks, values }))
        }}
      >
        Stream + generator
      </button>

      <button
        id="test-multiple-promises"
        onClick={async () => {
          setResult('')
          const res = await onReturnMultiplePromises()
          const resolved: Record<string, unknown> = { label: res.label }
          const updates: string[] = []
          await Promise.all([
            res.fast.then((v) => {
              resolved.fast = v
              updates.push('fast')
              setResult(JSON.stringify({ ...resolved, updates: [...updates] }))
            }),
            res.slow.then((v) => {
              resolved.slow = v
              updates.push('slow')
              setResult(JSON.stringify({ ...resolved, updates: [...updates] }))
            }),
          ])
        }}
      >
        Multiple promises
      </button>

      <button
        id="test-stream-promise-deadlock"
        onClick={async () => {
          setResult('')
          const res = await onReturnDeadlockStream()

          let promiseResolved = false
          const promiseDone = res.promise.then((v) => {
            promiseResolved = true
            return v
          })

          // Wait without reading the stream — promise should stay pending because
          // the large stream fills the demuxer buffer (1 MB), stalling frame delivery.
          await new Promise((resolve) => setTimeout(resolve, 3000))
          setResult(JSON.stringify({ promisePending: !promiseResolved, streamDone: false }))
          // Give the e2e test time to observe the pending state before we start consuming.
          await new Promise((resolve) => setTimeout(resolve, 500))

          // Start reading the stream — this drains the demuxer buffer so the promise frame can be delivered.
          const reader = res.stream.getReader()
          let byteCount = 0
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            byteCount += value.byteLength
          }

          // Promise should now resolve quickly.
          await promiseDone
          setResult(JSON.stringify({ promisePending: false, streamDone: true, byteCount, promiseResolved: true }))
        }}
      >
        Stream + promise deadlock
      </button>

      <button
        id="test-mixed-endless-cancel"
        onClick={async () => {
          setResult('')
          const res = await onReturnMixedEndless()
          const genValues: string[] = []
          const steps: string[] = []
          let promiseResult: unknown = undefined

          const render = () =>
            setResult(JSON.stringify({ steps: [...steps], promiseResult, genValues: [...genValues] }))

          // Start promise consumption in background — resolves when its frame arrives
          // (piggybacks on gen frame reads in the demuxer)
          const promiseP = res.slow.then((v) => {
            promiseResult = v
            steps.push('promise-resolved')
            render()
            return v
          })

          // Consume gen values one by one, rendering after each chunk
          for (let i = 0; i < 3; i++) {
            const { value } = await res.gen.next()
            genValues.push(value)
            steps.push(`gen-${i}`)
            render()
          }

          // Ensure promise has resolved before cancelling
          await promiseP

          // Cancel generator — all consumers done → onConnectionAbort fires
          await res.gen.return(undefined)
          steps.push('gen-cancelled')
          render()
        }}
      >
        Mixed endless cancel
      </button>

      <button
        id="test-asymmetric-generators"
        onClick={async () => {
          setResult('')
          const res = await onReturnAsymmetricGenerators()
          const fastValues: string[] = []
          const slowValues: string[] = []
          let slowFinished = false
          let fastDoneBeforeSlowFinished = false
          await Promise.all([
            (async () => {
              for await (const v of res.fast) fastValues.push(v)
              fastDoneBeforeSlowFinished = !slowFinished
            })(),
            (async () => {
              for await (const v of res.slow) slowValues.push(v)
              slowFinished = true
            })(),
          ])
          setResult(JSON.stringify({ fastValues, slowValues, fastDoneBeforeSlowFinished }))
        }}
      >
        Asymmetric generators
      </button>

      <h2>Mid-stream error tests</h2>

      <button
        id="test-generator-abort-midstream"
        onClick={async () => {
          setResult('')
          const gen = onGeneratorAbortMidStream()
          const values: string[] = []
          try {
            for await (const v of gen) {
              values.push(v)
            }
            setResult(JSON.stringify({ error: false, values }))
          } catch (e: any) {
            setResult(JSON.stringify({ error: true, isAbort: !!e.isAbort, abortValue: e.abortValue, values }))
          }
        }}
      >
        Generator Abort mid-stream
      </button>

      <button
        id="test-generator-abort-with-value"
        onClick={async () => {
          setResult('')
          const gen = onGeneratorAbortWithValue()
          const values: string[] = []
          try {
            for await (const v of gen) {
              values.push(v)
            }
            setResult(JSON.stringify({ error: false, values }))
          } catch (e: any) {
            setResult(JSON.stringify({ error: true, isAbort: !!e.isAbort, abortValue: e.abortValue, values }))
          }
        }}
      >
        Generator Abort with value
      </button>

      <button
        id="test-generator-bug-midstream"
        onClick={async () => {
          setResult('')
          const gen = onGeneratorBugMidStream()
          const values: string[] = []
          try {
            for await (const v of gen) {
              values.push(v)
            }
            setResult(JSON.stringify({ error: false, values }))
          } catch (e: any) {
            setResult(JSON.stringify({ error: true, isBug: !e.isAbort, message: String(e), values }))
          }
        }}
      >
        Generator Bug mid-stream
      </button>

      <h2>Upload progress via streaming</h2>

      <button
        id="test-upload-progress"
        onClick={async () => {
          setResult('')
          const content = 'x'.repeat(10_000_000)
          const file = new File([content], 'progress-test-100mb.txt', { type: 'text/plain' })
          const t0 = Date.now()
          const gen = onUploadWithProgress(file)
          const updates: { bytesRead: number; totalSize: number; clientMs: number }[] = []

          for await (const update of gen) {
            updates.push({ ...update, clientMs: Date.now() - t0 })
            setResult(JSON.stringify({ updates: [...updates], done: false, totalMs: Date.now() - t0 }))
          }
          setResult(JSON.stringify({ updates, done: true, totalMs: Date.now() - t0 }))
        }}
      >
        Upload with progress
      </button>
    </div>
  )
}
