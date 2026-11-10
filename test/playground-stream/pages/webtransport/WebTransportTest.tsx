export { WebTransportTest }
export type { WtState }

import React, { useCallback, useState } from 'react'
import { onWtLossy, onWtTwoChannels } from './WebTransport.telefunc'

type WtState = {
  /** Messages of the stalled channel its listener took, and ticks of the channel beside it. */
  twoChannels: { stalledReceived: number; ticks: number } | null
  /** Ordinals the lossy subscriber got, in order. */
  lossy: number[] | null
}

const LOSSY_COUNT = 2000

function WebTransportTest() {
  const [state, setState] = useState<WtState>({ twoChannels: null, lossy: null })

  const openTwoChannels = useCallback(async () => {
    const { stalled, ticks } = await onWtTwoChannels()
    const counts = { stalledReceived: 0, ticks: 0 }
    // Never finishes: the channel's credit runs out with what its first message left unread.
    stalled.listen(() => {
      counts.stalledReceived++
      return new Promise<void>(() => {})
    })
    ticks.listen(() => {
      counts.ticks++
      setState((s) => ({ ...s, twoChannels: { ...counts } }))
    })
  }, [])

  const runLossy = useCallback(async () => {
    const { broadcast, control } = await onWtLossy(LOSSY_COUNT)
    const received: number[] = []
    broadcast.subscribe((ordinal) => {
      received.push(ordinal)
      if (ordinal === LOSSY_COUNT - 1) setState((s) => ({ ...s, lossy: [...received] }))
    })
    await control.send('start', { ack: true })
  }, [])

  return (
    <div>
      <button type="button" id="wt-two-channels" onClick={openTwoChannels}>
        Two channels
      </button>
      <button type="button" id="wt-lossy" onClick={runLossy}>
        Lossy
      </button>
      <pre id="wt-state">{JSON.stringify(state)}</pre>
    </div>
  )
}
