export { RefIdentity }

import React, { useEffect, useState } from 'react'
import { onRefIdentity } from './RefIdentity.telefunc'

type State = {
  identity: {
    roomDupe: boolean
    roomList: boolean
    chDupe: boolean
    genDupe: boolean
  } | null
  chunkIdentity: boolean | null
  received: number[]
  pongs: string[]
}

function RefIdentity() {
  const [state, setState] = useState<State>({ identity: null, chunkIdentity: null, received: [], pongs: [] })
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="ref-identity-result">{JSON.stringify(state)}</pre>

      <button
        id="test-ref-identity"
        onClick={async () => {
          const result = await onRefIdentity()
          const { room, roomDupe, list, ch, chDupe, gen, genDupe } = result

          const identity = {
            roomDupe: room === roomDupe,
            roomList: room === list[0],
            chDupe: ch === chDupe,
            genDupe: gen === genDupe,
          }

          // The same broadcast yielded inside streamed chunks revives to the same object.
          const chunks: Array<{ tag: string; room: unknown }> = []
          for await (const chunk of gen) chunks.push(chunk)
          const chunkIdentity = chunks.length === 2 && chunks.every((chunk) => chunk.room === room)

          const received: number[] = []
          const pongs: string[] = []
          // Render on every event so the e2e autoRetry sees fresh data each iteration.
          const render = () => setState({ identity, chunkIdentity, received: [...received], pongs: [...pongs] })

          // Subscribe on the duplicate, publish on the original — one object, one subscription.
          // Duplicated wire subscriptions would show up as duplicated `n` values.
          roomDupe.subscribe((msg) => {
            if ('n' in msg) {
              received.push(msg.n)
              render()
            }
          })
          chDupe.listen((msg) => {
            pongs.push(msg)
            render()
          })

          render()
          await room.publish({ start: true })
          ch.send('ping')
        }}
      >
        Test reference identity
      </button>
    </div>
  )
}
