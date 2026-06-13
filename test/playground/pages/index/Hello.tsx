export { Hello }

import React, { useState, useEffect } from 'react'
import { onLoad } from './Hello.telefunc'

function Hello() {
  const [msg, setMsg] = useState<string | null>(null)
  useEffect(() => {
    ;(async () => {
      const { message } = await onLoad({ name: 'Eva' })
      setMsg(message)
    })()
  }, [])
  return <p>{!msg ? <>Loading...</> : <>{msg}</>}</p>
}
