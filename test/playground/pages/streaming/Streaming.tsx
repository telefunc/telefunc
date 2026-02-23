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
          const gen = await onReturnAsyncGenerator()
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
          const gen = await onReturnEmptyGenerator()
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
          const gen = await onReturnDelayedGenerator()
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

      <h2>Error tests</h2>

      <button
        id="test-two-generators"
        onClick={async () => {
          setResult('')
          try {
            await onReturnTwoGenerators()
            setResult(JSON.stringify({ error: false }))
          } catch (e: unknown) {
            setResult(JSON.stringify({ error: true, message: String(e) }))
          }
        }}
      >
        Two generators (should error)
      </button>

      <button
        id="test-stream-and-generator"
        onClick={async () => {
          setResult('')
          try {
            await onReturnStreamAndGenerator()
            setResult(JSON.stringify({ error: false }))
          } catch (e: unknown) {
            setResult(JSON.stringify({ error: true, message: String(e) }))
          }
        }}
      >
        Stream + generator (should error)
      </button>
    </div>
  )
}
