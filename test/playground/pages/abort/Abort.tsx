export { Abort }

import React, { useEffect, useState } from 'react'
import { onSlowAIGenerator, onSlowStreamForAbort, onSlowNormalTelefunc } from './Abort.telefunc'

function Abort() {
  const [hydrated, setHydrated] = useState(false)
  const [result, setResult] = useState<string>('')
  useEffect(() => setHydrated(true), [])

  return (
    <div>
      {hydrated && <span id="hydrated" />}
      <pre id="abort-result">{result}</pre>

      <h2>Connection abort tests</h2>

      <button
        id="test-slow-ai-generator"
        onClick={async () => {
          setResult('')
          const gen = await onSlowAIGenerator()
          const values: string[] = []
          for await (const v of gen) {
            values.push(v)
            // Break after first token — simulates user navigating away
            if (values.length >= 1) break
          }
          setResult(JSON.stringify({ values, aiDisconnected: true }))
        }}
      >
        Slow AI generator
      </button>

      <button
        id="test-slow-stream"
        onClick={async () => {
          setResult('')
          const stream = await onSlowStreamForAbort()
          const reader = stream.getReader()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            chunks.push(decoder.decode(value, { stream: true }))
            // Cancel after first chunk
            if (chunks.length >= 1) {
              await reader.cancel()
              break
            }
          }
          setResult(JSON.stringify({ chunks, streamCancelled: true }))
        }}
      >
        Slow ReadableStream
      </button>

      <button
        id="test-slow-normal-telefunc"
        onClick={async () => {
          setResult('')
          // Fire and forget — we'll navigate away to disconnect
          onSlowNormalTelefunc()
          // Wait a bit for a few steps to run, then signal done
          await new Promise((r) => setTimeout(r, 1500))
          setResult(JSON.stringify({ normalStarted: true }))
        }}
      >
        Slow normal telefunc
      </button>
    </div>
  )
}
