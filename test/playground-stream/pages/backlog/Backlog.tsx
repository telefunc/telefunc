export { Backlog }

import React, { useEffect, useRef, useState } from 'react'
import { onBacklog } from './Backlog.telefunc'

type BacklogState = { channelId: string | null; received: number; outOfOrder: number }

function Backlog() {
  const [state, setState] = useState<BacklogState>({ channelId: null, received: 0, outOfOrder: 0 })
  const channelRef = useRef<Awaited<ReturnType<typeof onBacklog>>['channel'] | null>(null)
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => {
    setHydrated(true)
    return () => {
      channelRef.current?.close()
    }
  }, [])

  const open = async () => {
    const { channel, channelId } = await onBacklog()
    channelRef.current = channel
    let expected = 0
    channel.listen((message) => {
      const inOrder = Number(message.trimEnd()) === expected++
      setState((s) => ({ ...s, received: s.received + 1, outOfOrder: s.outOfOrder + (inOrder ? 0 : 1) }))
    })
    setState((s) => ({ ...s, channelId }))
  }

  const run = (count: number, awaited: boolean) => channelRef.current?.send({ count, awaited })

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="backlog-state">{JSON.stringify(state)}</pre>
      <button type="button" id="backlog-open" onClick={open}>
        Open
      </button>
      <button type="button" id="backlog-run-awaited" onClick={() => run(4000, true)}>
        Send 64 MiB, awaited
      </button>
      <button type="button" id="backlog-run-unawaited" onClick={() => run(6000, false)}>
        Send 96 MiB, unawaited
      </button>
    </div>
  )
}
