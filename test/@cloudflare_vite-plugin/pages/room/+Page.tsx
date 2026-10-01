export { Page }

import React, { useEffect, useState } from 'react'
import { onRoomRoundTrip } from './Room.telefunc'

function Page() {
  const [result, setResult] = useState('')
  useEffect(() => {
    onRoomRoundTrip().then(
      (value) => setResult(JSON.stringify(value)),
      (error: unknown) => setResult(String(error)),
    )
  }, [])
  return <pre id="room-result">{result}</pre>
}
