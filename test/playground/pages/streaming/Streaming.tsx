export { Streaming }

import React, { useEffect, useRef, useState } from 'react'
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
  onAbortOneOfManyStreamingValues,
  onChannelAbortAbortsSiblingStreamingValues,
  onUploadWithProgress,
} from './Streaming.telefunc'
import { Abort as TelefuncAbort, abort } from 'telefunc/client'

function Streaming() {
  const [result, setResult] = useState<string>('')
  const [hydrated, setHydrated] = useState(false)
  const deadlockRef = useRef<{
    reader: ReadableStreamDefaultReader<Uint8Array>
    promiseDone: Promise<unknown>
  } | null>(null)
  useEffect(() => setHydrated(true), [])

  return (
    <div>
      <h2>Streaming Tests (Server → Client)</h2>
      {hydrated && <span id="hydrated" />}

      <button
        id="test-readable-stream"
        onClick={async () => {
          const reader = (await onReturnReadableStream()).getReader()
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
        id="test-stream-promise-deadlock-start"
        onClick={async () => {
          setResult('')
          deadlockRef.current = null
          const res = await onReturnDeadlockStream()

          let promiseResolved = false
          const promiseDone = res.promise.then((v) => {
            promiseResolved = true
            return v
          })

          // Store reader + promise; don't consume the stream yet so the promise stays pending.
          deadlockRef.current = { reader: res.stream.getReader(), promiseDone }
          // Promise must still be pending here — the large stream fills the demuxer
          // buffer (1 MB), stalling delivery of the promise frame until stream is consumed.
          setResult(JSON.stringify({ promisePending: !promiseResolved, streamDone: false }))
        }}
      >
        Stream + promise deadlock (start)
      </button>

      <button
        id="test-stream-promise-deadlock-consume"
        onClick={async () => {
          const state = deadlockRef.current
          if (!state) return
          const { reader, promiseDone } = state

          // Drain the stream — this unblocks the demuxer so the promise frame can be delivered.
          let byteCount = 0
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            byteCount += value.byteLength
          }

          await promiseDone
          setResult(JSON.stringify({ promisePending: false, streamDone: true, byteCount, promiseResolved: true }))
        }}
      >
        Stream + promise deadlock (consume)
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

          // Cancel generator — all consumers done → onClose fires
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

      <button
        id="test-abort-multiplexed"
        onClick={async () => {
          setResult('')
          const res = await onReturnMixedEndless()
          const genValues: string[] = []
          let slowErr: { isAbort: boolean; abortValue: unknown; error: string } | null = null

          // Read a couple of gen values
          for (let i = 0; i < 2; i++) {
            const { value } = await res.gen.next()
            genValues.push(value)
          }

          // Abort the entire multiplexed result
          abort(res)

          try {
            await res.slow
          } catch (e: any) {
            slowErr = {
              isAbort: e instanceof TelefuncAbort,
              abortValue: e?.abortValue ?? null,
              error: e?.message ?? String(e),
            }
          }

          // Next read should reject with Abort
          try {
            await res.gen.next()
            setResult(JSON.stringify({ error: null, genValues }))
          } catch (e: any) {
            setResult(JSON.stringify({ error: e.message, isAbort: e instanceof TelefuncAbort, genValues, slowErr }))
          }
        }}
      >
        Abort multiplexed result
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
            setResult(
              JSON.stringify({ error: true, isAbort: e instanceof TelefuncAbort, abortValue: e.abortValue, values }),
            )
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
            setResult(
              JSON.stringify({ error: true, isAbort: e instanceof TelefuncAbort, abortValue: e.abortValue, values }),
            )
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
            setResult(JSON.stringify({ error: true, isBug: !(e instanceof TelefuncAbort), message: String(e), values }))
          }
        }}
      >
        Generator Bug mid-stream
      </button>

      <button
        id="test-abort-one-of-many-streaming-values"
        onClick={async () => {
          setResult('')
          const res = await onAbortOneOfManyStreamingValues()
          const abortingValues: string[] = []
          const otherValues: string[] = []
          const streamChunks: string[] = []
          const decoder = new TextDecoder()
          let abortingErr: { isAbort: boolean; abortValue: unknown } | null = null
          let otherErr: { isAbort: boolean; abortValue: unknown } | null = null
          let streamErr: { isAbort: boolean; abortValue: unknown } | null = null
          let promiseErr: { isAbort: boolean; abortValue: unknown } | null = null

          await Promise.all([
            (async () => {
              try {
                for await (const v of res.aborting) {
                  abortingValues.push(v)
                }
              } catch (e: any) {
                abortingErr = { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null }
              }
            })(),
            (async () => {
              try {
                for await (const v of res.other) {
                  otherValues.push(v)
                }
              } catch (e: any) {
                otherErr = { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null }
              }
            })(),
            (async () => {
              try {
                const reader = res.stream.getReader()
                while (true) {
                  const { done, value } = await reader.read()
                  if (done) break
                  streamChunks.push(decoder.decode(value, { stream: true }))
                }
              } catch (e: any) {
                streamErr = { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null }
              }
            })(),
            (async () => {
              try {
                await res.promise
              } catch (e: any) {
                promiseErr = { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null }
              }
            })(),
          ])

          setResult(
            JSON.stringify({ abortingValues, otherValues, streamChunks, abortingErr, otherErr, streamErr, promiseErr }),
          )
        }}
      >
        Abort one of many streaming values
      </button>

      <button
        id="test-channel-abort-aborts-sibling-streaming-values"
        onClick={async () => {
          setResult('')
          const res = await onChannelAbortAbortsSiblingStreamingValues()
          const firstValues: string[] = []
          const secondValues: string[] = []
          const streamChunks: string[] = []
          const decoder = new TextDecoder()
          let firstErr: { isAbort: boolean; abortValue: unknown } | null = null
          let secondErr: { isAbort: boolean; abortValue: unknown } | null = null
          let streamErr: { isAbort: boolean; abortValue: unknown } | null = null
          let channelSendErr: { isAbort: boolean; abortValue: unknown } | null = null
          let channelCloseErr: { isAbort: boolean; abortValue: unknown } | null = null

          const channelCloseP = new Promise<void>((resolve) => {
            res.channel.onClose((err: any) => {
              channelCloseErr = { isAbort: err instanceof TelefuncAbort, abortValue: err?.abortValue ?? null }
              resolve()
            })
          })

          await Promise.all([
            (async () => {
              try {
                for await (const v of res.first) {
                  firstValues.push(v)
                }
              } catch (e: any) {
                firstErr = { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null }
              }
            })(),
            (async () => {
              try {
                for await (const v of res.second) {
                  secondValues.push(v)
                }
              } catch (e: any) {
                secondErr = { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null }
              }
            })(),
            (async () => {
              try {
                const reader = res.stream.getReader()
                while (true) {
                  const { done, value } = await reader.read()
                  if (done) break
                  streamChunks.push(decoder.decode(value, { stream: true }))
                }
              } catch (e: any) {
                streamErr = { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null }
              }
            })(),
            (async () => {
              await new Promise<void>((resolve) => res.channel.onOpen(resolve))
              try {
                await res.channel.send('trigger', { ack: true })
              } catch (e: any) {
                channelSendErr = { isAbort: e instanceof TelefuncAbort, abortValue: e?.abortValue ?? null }
              }
            })(),
            channelCloseP,
          ])
          setResult(
            JSON.stringify({
              firstValues,
              secondValues,
              streamChunks,
              firstErr,
              secondErr,
              streamErr,
              channelSendErr,
              channelCloseErr,
            }),
          )
        }}
      >
        Channel abort aborts sibling streaming values
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
