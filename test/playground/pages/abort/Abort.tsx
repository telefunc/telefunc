export { Abort }

import React, { useEffect, useState } from 'react'
import { onSlowAIGenerator, onSlowStreamForAbort, onSlowNormalTelefunc } from './Abort.telefunc'
import { abort } from 'telefunc/client'

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
          const gen = onSlowAIGenerator()
          const values: string[] = []
          // First token arrives immediately
          const first = await gen.next()
          if (!first.done) values.push(first.value)
          // Second token takes 10s — cancel after 500ms while read is pending
          const nextPromise = gen.next()
          setTimeout(() => {
            abort(gen)
            // gen.return(undefined)
          }, 500)
          console.log(await nextPromise)
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
          // First chunk arrives immediately
          const first = await reader.read()
          if (!first.done) chunks.push(decoder.decode(first.value, { stream: true }))
          // Second chunk takes 5s — cancel after 500ms while read is pending
          const timeout = new Promise((r) => setTimeout(() => r('timeout'), 500))
          const race = await Promise.race([reader.read(), timeout])
          if (race === 'timeout') await reader.cancel()
          setResult(JSON.stringify({ chunks, streamCancelled: true }))
        }}
      >
        Slow ReadableStream
      </button>

      <button
        id="test-slow-normal-telefunc"
        onClick={async () => {
          setResult('')
          const promise = onSlowNormalTelefunc()
          // Wait a bit for a few steps to run, then abort
          await new Promise((r) => setTimeout(r, 1500))
          abort(promise)
          setResult(JSON.stringify({ normalStarted: true }))
        }}
      >
        Slow normal telefunc
      </button>
    </div>
  )
}
