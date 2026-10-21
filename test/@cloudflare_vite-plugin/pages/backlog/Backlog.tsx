export { Backlog }

import React, { useEffect, useRef, useState } from 'react'
import { onBacklog } from './Backlog.telefunc'

type BacklogState = { channelId: string | null; received: number; outOfOrder: number; refused: string[] | null }

function Backlog() {
  const [state, setState] = useState<BacklogState>({ channelId: null, received: 0, outOfOrder: 0, refused: null })
  const channelRef = useRef<Awaited<ReturnType<typeof onBacklog>> | null>(null)
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => {
    setHydrated(true)
    return () => {
      channelRef.current?.close()
    }
  }, [])

  const open = async () => {
    const channel = await onBacklog()
    channelRef.current = channel
    let expected = 0
    channel.listen((message) => {
      const inOrder = Number(message.trimEnd()) === expected++
      setState((s) => ({ ...s, received: s.received + 1, outOfOrder: s.outOfOrder + (inOrder ? 0 : 1) }))
    })
    setState((s) => ({ ...s, channelId: channel.id }))
  }

  const run = async () => {
    const refused = await channelRef.current!.send(6000, { ack: true })
    setState((s) => ({ ...s, refused }))
  }

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Backlog</h1>
      <pre id="backlog-state">{JSON.stringify(state)}</pre>
      <button type="button" id="backlog-open" onClick={open}>
        Open
      </button>
      <button type="button" id="backlog-run" onClick={run}>
        Send 96 MiB, unawaited
      </button>
    </div>
  )
}
