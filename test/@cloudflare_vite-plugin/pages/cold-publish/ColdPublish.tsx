export { ColdPublish }

import React, { useEffect, useState } from 'react'
import { onColdPublish } from './ColdPublish.telefunc'

function ColdPublish() {
  const [result, setResult] = useState('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Cold publish</h1>
      <pre id="cold-publish-result">{result}</pre>
      <button type="button" id="cold-publish" onClick={async () => setResult(JSON.stringify(await onColdPublish()))}>
        Publish
      </button>
    </div>
  )
}
