export { RefusedDelivery }

import React, { useEffect, useState } from 'react'
import { onReceived, onSubscribe } from './RefusedDelivery.telefunc'

/** Its server subscribes on the broadcast key `?key=` names, as the page `?page=` names; the Worker publishes to it. */
function RefusedDelivery() {
  const [state, setState] = useState<{ subscribed: boolean; received: string[] | null }>({
    subscribed: false,
    received: null,
  })
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  const param = (name: string) => new URLSearchParams(window.location.search).get(name)!

  const subscribe = async () => {
    await onSubscribe(param('key'), param('page'))
    setState((s) => ({ ...s, subscribed: true }))
  }

  const read = async () => {
    const received = await onReceived(param('key'), param('page'))
    setState((s) => ({ ...s, received }))
  }

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Refused delivery</h1>
      <pre id="refused-delivery-state">{JSON.stringify(state)}</pre>
      <button type="button" id="subscribe" onClick={subscribe}>
        Subscribe
      </button>
      <button type="button" id="read" onClick={read}>
        Read what the server's subscriber got
      </button>
    </div>
  )
}
