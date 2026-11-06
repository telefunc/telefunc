export { Backlog }

import React, { useEffect, useRef, useState } from 'react'
import { onBacklog } from './Backlog.telefunc'

type BacklogState = { channelId: string | null; received: number; outOfOrder: number; closeErr: string | null }

function Backlog() {
  const [state, setState] = useState<BacklogState>({ channelId: null, received: 0, outOfOrder: 0, closeErr: null })
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
    channel.onClose((err) => setState((s) => ({ ...s, closeErr: err?.name ?? 'none' })))
    setState((s) => ({ ...s, channelId: channel.id }))
  }

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <h1>Backlog</h1>
      <pre id="backlog-state">{JSON.stringify(state)}</pre>
      <button type="button" id="backlog-open" onClick={open}>
        Open
      </button>
      <button type="button" id="backlog-run-1500" onClick={() => channelRef.current!.send(1500)}>
        Send 24 MiB, unawaited
      </button>
      <button type="button" id="backlog-run-3000" onClick={() => channelRef.current!.send(3000)}>
        Send 48 MiB, unawaited
      </button>
    </div>
  )
}
