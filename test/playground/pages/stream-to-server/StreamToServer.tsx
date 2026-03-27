export { StreamToServer }

import React, { useEffect, useState } from 'react'
import {
  onEcho,
  onCollect,
  onRelay,
  onPassthrough,
  onSlowConsumer,
  onBackpressure,
  onAbortMidStream,
  onLiveLoopback,
} from './StreamToServer.telefunc'
import { Abort, abort } from 'telefunc/client'

function StreamToServer() {
  const [result, setResult] = useState<string>('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="stream-result">{result}</pre>

      <h2>Basic</h2>

      <button
        id="test-echo"
        onClick={async () => {
          setResult('')
          const res = await onEcho(makeTextStream(['hello', ' ', 'world']))
          setResult(JSON.stringify(res))
        }}
      >
        Echo
      </button>

      <button
        id="test-collect"
        onClick={async () => {
          setResult('')
          const res = await onCollect(makeTextStream(['aaa', 'bb', 'cccc', 'd']))
          setResult(JSON.stringify(res))
        }}
      >
        Collect
      </button>

      <button
        id="test-relay"
        onClick={async () => {
          setResult('')
          const values: string[] = []
          for await (const v of onRelay(makeTextStream(['alpha', 'beta', 'gamma']))) values.push(v)
          setResult(JSON.stringify({ values }))
        }}
      >
        Relay (generator)
      </button>

      <button
        id="test-passthrough"
        onClick={async () => {
          setResult('')
          const sent = ['one', 'two', 'three']
          const res = await onPassthrough(makeTextStream(sent))
          const received: string[] = []
          const reader = res.stream.getReader()
          const decoder = new TextDecoder()
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            received.push(decoder.decode(value))
          }
          setResult(JSON.stringify({ sent, received, match: JSON.stringify(sent) === JSON.stringify(received) }))
        }}
      >
        Passthrough (stream back)
      </button>

      <h2>Streaming</h2>

      <button
        id="test-live-loopback"
        onClick={async () => {
          setResult('')
          const encoder = new TextEncoder()
          let i = 0
          let closed = false
          const stream = new ReadableStream<Uint8Array>({
            start(controller) {
              const interval = setInterval(() => {
                if (i >= 6) {
                  clearInterval(interval)
                  controller.close()
                  return
                }
                controller.enqueue(encoder.encode(`ping-${i++}`))
              }, 500)
            },
            cancel() {
              closed = true
            },
          })
          const log: Array<{ value: string; at: number }> = []
          const t0 = Date.now()
          for await (const v of onLiveLoopback(stream)) {
            log.push({ value: v, at: Date.now() - t0 })
            setResult(JSON.stringify({ log, done: false }))
          }
          setResult(JSON.stringify({ log, done: true }))
        }}
      >
        Live loopback (500ms/value, 3s)
      </button>

      <button
        id="test-slow-consumer"
        onClick={async () => {
          setResult('')
          const res = await onSlowConsumer(makeTextStream(['fast-0', 'fast-1', 'fast-2', 'fast-3', 'fast-4']))
          setResult(JSON.stringify(res))
        }}
      >
        Slow consumer (200ms/read)
      </button>

      <button
        id="test-backpressure"
        onClick={async () => {
          setResult('')
          const CHUNK_SIZE = 1024 * 1024 // 1 MB
          const TOTAL_CHUNKS = 50 // 50 MB total
          let pullCount = 0
          const stream = new ReadableStream<Uint8Array>({
            pull(controller) {
              if (pullCount >= TOTAL_CHUNKS) {
                controller.close()
                return
              }
              controller.enqueue(new Uint8Array(CHUNK_SIZE).fill(97))
              pullCount++
            },
          })
          const res = await onBackpressure(stream)
          setResult(JSON.stringify({ ...res, done: true }))
        }}
      >
        Backpressure (50 MB)
      </button>

      <h2>Abort</h2>

      <button
        id="test-abort-mid-stream"
        onClick={async () => {
          setResult('')
          try {
            await onAbortMidStream(makeTextStream(['a', 'b', 'c', 'd', 'e', 'f']))
            setResult(JSON.stringify({ error: false }))
          } catch (e: any) {
            setResult(JSON.stringify({ error: true, isAbort: e instanceof Abort, abortValue: e.abortValue }))
          }
        }}
      >
        Server abort after 3 chunks
      </button>

      <button
        id="test-client-abort"
        onClick={async () => {
          setResult('')
          let cancelled = false
          const stream = new ReadableStream<Uint8Array>({
            async pull(controller) {
              await new Promise((r) => setTimeout(r, 100))
              if (cancelled) return
              controller.enqueue(new TextEncoder().encode('tick'))
            },
            cancel(reason) {
              cancelled = true
              setResult((prev) => prev || JSON.stringify({ cancelled: true, reason: String(reason) }))
            },
          })
          const promise = onSlowConsumer(stream)
          setTimeout(() => abort(promise), 500)
          try {
            await promise
          } catch (e: any) {
            setResult(JSON.stringify({ aborted: true, isAbort: e instanceof Abort, abortValue: e.abortValue }))
          }
        }}
      >
        Client abort(res)
      </button>
    </div>
  )
}

function makeTextStream(chunks: string[]) {
  const encoder = new TextEncoder()
  let index = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (index >= chunks.length) {
        controller.close()
        return
      }
      controller.enqueue(encoder.encode(chunks[index]!))
      index++
    },
  })
}
