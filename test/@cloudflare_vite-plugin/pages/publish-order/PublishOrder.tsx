export { PublishOrder }

import React, { useEffect, useState } from 'react'
import { onPublishLoop } from './PublishOrder.telefunc'

function PublishOrder() {
  const [result, setResult] = useState('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Publish order</h1>
      <pre id="publish-order-result">{result}</pre>
      <button type="button" id="publish-order" onClick={async () => setResult(JSON.stringify(await onPublishLoop()))}>
        Publish
      </button>
    </div>
  )
}
