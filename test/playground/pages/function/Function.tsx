export { FunctionDemo }

import React, { useEffect, useState } from 'react'
import { onGetGreeter, onGetAdder, onGetEchoWithState, onMap, onReduce, onUpload } from './Function.telefunc'

type FunctionState = {
  greeterResult: string | null
  adderResults: number[]
  echoResults: { echo: string; callCount: number }[]
  mapResult: number[] | null
  reduceResult: number | null
  uploadProgress: number[]
  uploadResult: { name: string; size: number } | null
}

function FunctionDemo() {
  const [state, setState] = useState<FunctionState>({
    greeterResult: null,
    adderResults: [],
    echoResults: [],
    mapResult: null,
    reduceResult: null,
    uploadProgress: [],
    uploadResult: null,
  })
  const [log, setLog] = useState<string[]>([])

  function addLog(msg: string) {
    setLog((prev) => [...prev, msg])
  }

  async function runGreeter() {
    addLog('calling onGetGreeter()...')
    const greet = await onGetGreeter()
    const result = await greet('World')
    addLog(`greet("World") → ${result}`)
    setState((s) => ({ ...s, greeterResult: result }))
  }

  async function runAdder() {
    addLog('calling onGetAdder(10)...')
    const add = await onGetAdder(10)
    const r1 = await add(5) // 15
    addLog(`add(5) → ${r1}`)
    const r2 = await add(3) // 18
    addLog(`add(3) → ${r2}`)
    const r3 = await add(2) // 20
    addLog(`add(2) → ${r3}`)
    setState((s) => ({ ...s, adderResults: [r1, r2, r3] }))
  }

  async function runEcho() {
    addLog('calling onGetEchoWithState()...')
    const echo = await onGetEchoWithState()
    const r1 = await echo('hello')
    addLog(`echo("hello") → ${JSON.stringify(r1)}`)
    const r2 = await echo('world')
    addLog(`echo("world") → ${JSON.stringify(r2)}`)
    setState((s) => ({ ...s, echoResults: [r1, r2] }))
  }

  async function runMap() {
    addLog('calling onMap([1,2,3,4,5], n => n * n)...')
    const result = await onMap([1, 2, 3, 4, 5], async (n) => n * n)
    addLog(`map result → ${JSON.stringify(result)}`)
    setState((s) => ({ ...s, mapResult: result }))
  }

  async function runReduce() {
    addLog('calling onReduce([1,2,3,4,5], (acc,n) => acc+n, 0)...')
    const result = await onReduce([1, 2, 3, 4, 5], async (acc, n) => acc + n, 0)
    addLog(`reduce result → ${result}`)
    setState((s) => ({ ...s, reduceResult: result }))
  }

  async function runUpload() {
    const content = 'x'.repeat(1024 * 100) // 100KB
    const file = new File([content], 'test-upload.txt', { type: 'text/plain' })
    const progress: number[] = []
    addLog(`uploading ${file.name} (${file.size} bytes)...`)
    const result = await onUpload(file, async (percent) => {
      console.log(`upload progress: ${percent}%`)
      progress.push(percent)
      setState((s) => ({ ...s, uploadProgress: [...progress] }))
    })
    console.log(`upload complete: ${result.name} (${result.size} bytes)`)
    addLog(`upload done: ${result.name} (${result.size} bytes), ${progress.length} progress updates`)
    setState((s) => ({ ...s, uploadResult: result }))
  }

  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div id={hydrated ? 'hydrated' : undefined} className="p-8 max-w-2xl space-y-8">
      <h1 className="text-xl font-semibold">Function passing demo</h1>

      {/* Greeter */}
      <section className="space-y-2">
        <h2 className="font-medium">Simple function</h2>
        <button id="greeter-run" onClick={runGreeter} className="px-3 py-1 border rounded text-sm">
          Run
        </button>
        <pre id="greeter-result" className="text-sm bg-zinc-50 p-2 rounded">
          {JSON.stringify(state.greeterResult)}
        </pre>
      </section>

      {/* Adder */}
      <section className="space-y-2">
        <h2 className="font-medium">Stateful function (server-side closure)</h2>
        <button id="adder-run" onClick={runAdder} className="px-3 py-1 border rounded text-sm">
          Run
        </button>
        <pre id="adder-results" className="text-sm bg-zinc-50 p-2 rounded">
          {JSON.stringify(state.adderResults)}
        </pre>
      </section>

      {/* Echo with call count */}
      <section className="space-y-2">
        <h2 className="font-medium">Function with server-side call counter</h2>
        <button id="echo-run" onClick={runEcho} className="px-3 py-1 border rounded text-sm">
          Run
        </button>
        <pre id="echo-results" className="text-sm bg-zinc-50 p-2 rounded">
          {JSON.stringify(state.echoResults)}
        </pre>
      </section>

      {/* Map */}
      <section className="space-y-2">
        <h2 className="font-medium">Pass function as argument — map</h2>
        <p className="text-xs text-zinc-500">
          Client passes <code>n =&gt; n * n</code>; server applies it to [1,2,3,4,5]
        </p>
        <button id="map-run" onClick={runMap} className="px-3 py-1 border rounded text-sm">
          Run
        </button>
        <pre id="map-result" className="text-sm bg-zinc-50 p-2 rounded">
          {JSON.stringify(state.mapResult)}
        </pre>
      </section>

      {/* Reduce */}
      <section className="space-y-2">
        <h2 className="font-medium">Pass function as argument — reduce</h2>
        <p className="text-xs text-zinc-500">
          Client passes <code>(acc, n) =&gt; acc + n</code>; server reduces [1,2,3,4,5]
        </p>
        <button id="reduce-run" onClick={runReduce} className="px-3 py-1 border rounded text-sm">
          Run
        </button>
        <pre id="reduce-result" className="text-sm bg-zinc-50 p-2 rounded">
          {JSON.stringify(state.reduceResult)}
        </pre>
      </section>

      {/* Upload with progress */}
      <section className="space-y-2">
        <h2 className="font-medium">File upload with progress callback</h2>
        <p className="text-xs text-zinc-500">
          Client passes <code>onProgress</code> callback; server calls it while streaming the file
        </p>
        <button id="upload-run" onClick={runUpload} className="px-3 py-1 border rounded text-sm">
          Run
        </button>
        <pre id="upload-progress" className="text-sm bg-zinc-50 p-2 rounded">
          {JSON.stringify(state.uploadProgress)}
        </pre>
        <pre id="upload-result" className="text-sm bg-zinc-50 p-2 rounded">
          {JSON.stringify(state.uploadResult)}
        </pre>
      </section>

      {/* Log */}
      <section className="space-y-1">
        <h2 className="font-medium">Log</h2>
        <div id="fn-log" className="text-xs font-mono space-y-0.5">
          {log.map((entry, i) => (
            <div key={i} className="text-zinc-600">
              {entry}
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
