export { Page }

import React from 'react'
import { Room } from './Room'
import { RoomLost } from './RoomLost'
import { Chat } from './Chat'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Room</h1>
      <Room />
      <RoomLost />
      <Chat />
    </div>
  )
}
