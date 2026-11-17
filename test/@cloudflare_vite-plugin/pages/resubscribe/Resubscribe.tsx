export { Resubscribe }

import React, { useEffect, useState } from 'react'
import { onPublish, onReceived, onResubscribe } from './Resubscribe.telefunc'

function Resubscribe() {
  const [key] = useState(() => `resubscribe:${Math.random()}`)
  const [state, setState] = useState<{ subscribed: boolean; received: string[] | null }>({
    subscribed: false,
    received: null,
  })
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  const resubscribe = async () => {
    await onResubscribe(key)
    setState((s) => ({ ...s, subscribed: true }))
  }

  const read = async () => {
    const received = await onReceived(key)
    setState((s) => ({ ...s, received }))
  }

  const publish = async () => {
    const received = await onPublish(key, 'hello')
    setState((s) => ({ ...s, received }))
  }

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Resubscribe</h1>
      <pre id="resubscribe-state">{JSON.stringify(state)}</pre>
      <button type="button" id="resubscribe" onClick={resubscribe}>
        Resubscribe
      </button>
      <button type="button" id="publish" onClick={publish}>
        Publish
      </button>
      <button type="button" id="read" onClick={read}>
        Read
      </button>
    </div>
  )
}
