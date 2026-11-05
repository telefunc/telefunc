export { UnsubscribeTwice }

import React, { useEffect, useState } from 'react'
import { onPublish, onReceived, onSubscribeAndUnsubscribeTwice } from './UnsubscribeTwice.telefunc'

/** Its server subscribes on the broadcast key `?key=` names, so a page in another session can publish to it. */
function UnsubscribeTwice() {
  const [state, setState] = useState<{ subscribed: boolean; received: string[] | null }>({
    subscribed: false,
    received: null,
  })
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  const key = () => new URLSearchParams(window.location.search).get('key')!

  const subscribe = async () => {
    await onSubscribeAndUnsubscribeTwice(key())
    setState((s) => ({ ...s, subscribed: true }))
  }

  const publish = () => onPublish(key(), 'hello')

  const read = async () => {
    const received = await onReceived(key())
    setState((s) => ({ ...s, received }))
  }

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Unsubscribe twice</h1>
      <pre id="unsubscribe-twice-state">{JSON.stringify(state)}</pre>
      <button type="button" id="subscribe" onClick={subscribe}>
        Subscribe twice, unsubscribe the first twice
      </button>
      <button type="button" id="publish" onClick={publish}>
        Publish
      </button>
      <button type="button" id="read" onClick={read}>
        Read what the server's subscriber got
      </button>
    </div>
  )
}
