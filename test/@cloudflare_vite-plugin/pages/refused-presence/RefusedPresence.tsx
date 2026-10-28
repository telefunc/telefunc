export { RefusedPresence }

import React, { useEffect, useState } from 'react'
import { onPublish, onSubscribe } from './RefusedPresence.telefunc'

function RefusedPresence() {
  const [key] = useState(() => `refused-presence:${Math.random()}`)
  const [state, setState] = useState<{ published: boolean; received: string[] | null }>({
    published: false,
    received: null,
  })
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  const subscribe = async () => {
    await onSubscribe(key)
    setState((s) => ({ ...s, published: true }))
  }

  const publish = async () => {
    const received = await onPublish(key, 'again')
    setState((s) => ({ ...s, received }))
  }

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Refused presence</h1>
      <pre id="refused-presence-state">{JSON.stringify(state)}</pre>
      <button type="button" id="subscribe" onClick={subscribe}>
        Subscribe
      </button>
      <button type="button" id="publish" onClick={publish}>
        Publish
      </button>
    </div>
  )
}
