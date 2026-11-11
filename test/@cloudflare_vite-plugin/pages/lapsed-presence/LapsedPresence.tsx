export { LapsedPresence }

import React, { useEffect, useState } from 'react'
import { onPublish, onSubscribe } from './LapsedPresence.telefunc'

function LapsedPresence() {
  const [key] = useState(() => `lapsed-presence:${Math.random()}`)
  const [state, setState] = useState<Awaited<ReturnType<typeof onPublish>> | null>(null)
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Lapsed presence</h1>
      <pre id="lapsed-presence-state">{JSON.stringify(state)}</pre>
      <button type="button" id="subscribe" onClick={async () => setState(await onSubscribe(key))}>
        Subscribe
      </button>
      <button type="button" id="publish-during" onClick={async () => setState(await onPublish(key, 'during'))}>
        Publish while it's lapsed
      </button>
      <button type="button" id="publish-after" onClick={async () => setState(await onPublish(key, 'after'))}>
        Publish after
      </button>
    </div>
  )
}
