export { Page }

import React from 'react'
import { KitchenSink } from './KitchenSink'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Kitchen Sink</h1>
      <p className="text-sm text-zinc-500 mb-6">
        A single telefunction that exercises everything at once: a <code>File</code> is split via <code>.tee()</code> —
        one half echoed back as a <code>ReadableStream</code>, the other consumed chunk-by-chunk as an{' '}
        <code>AsyncGenerator</code> that yields live progress. A <code>Promise</code> resolves only after the generator
        drains the file, carrying the final checksum. A nested <code>Blob</code> is read eagerly. All four streams are
        multiplexed on a single HTTP response. <code>onConnectionAbort</code> stops processing on disconnect.
      </p>
      <KitchenSink />
    </div>
  )
}
