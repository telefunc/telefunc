export { RoomLost }

import React, { useState } from 'react'
import { onRoomToLoseFrom, onLoseJoin, onLoseLeave, onLoseMeta, onLoseClose } from './RoomLost.telefunc'

/** The store drops one of the room's frames; the page's view still ends on what happened. */
function RoomLost() {
  const [result, setResult] = useState('')
  const card = (id: string, lose: (roomId: string) => Promise<void>) => (
    <button
      type="button"
      id={`test-room-lost-${id}`}
      onClick={async () => {
        setResult('')
        const roomId = `e2e-lost-${id}:${crypto.randomUUID()}`
        const room = await onRoomToLoseFrom(roomId)
        const events: string[] = []
        room.onJoin((member) => events.push(`join:${member.meta.name}`))
        room.onLeave((member) => events.push(`leave:${member.meta.name}`))
        room.onClose(() => events.push('close'))
        await lose(roomId)
        // Render every 200 ms, so the test sees how soon the view heals.
        for (let i = 0; i < 250; i++) {
          setResult(JSON.stringify({ events, meta: room.meta, count: room.count }))
          await new Promise((resolve) => setTimeout(resolve, 200))
        }
      }}
    >
      {id}
    </button>
  )
  return (
    <div>
      <h2>Lost Frames</h2>
      <pre id="room-lost-result">{result}</pre>
      {card('join', onLoseJoin)}
      {card('leave', onLoseLeave)}
      {card('meta', onLoseMeta)}
      {card('close', onLoseClose)}
    </div>
  )
}
