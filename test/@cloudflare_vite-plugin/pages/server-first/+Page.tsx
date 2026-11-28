export { Page }

import React, { useEffect, useState } from 'react'
import { config } from 'telefunc/client'
import { onServerFirstChannel, onServerFirstPublish } from './ServerFirst.telefunc'

function Page() {
  const [received, setReceived] = useState<string[] | null>(null)
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  async function run() {
    // Test-only: the server reads the first `session` parameter, so the test picks this page's Durable Object.
    config.telefuncUrl = `/_telefunc?session=${new URLSearchParams(location.search).get('session')}`
    config.channel = { transports: ['ws'] }
    const key = `server-first:${crypto.randomUUID()}`
    const channel = await onServerFirstChannel(key)
    const messages: string[] = []
    channel.subscribe((message) => messages.push(message))
    await channel.opened
    await onServerFirstPublish(key)
    setTimeout(() => setReceived(messages), 2_000)
  }
  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="server-first-result">{JSON.stringify(received)}</pre>
      <button type="button" id="server-first" onClick={run}>
        Subscribe
      </button>
    </div>
  )
}
