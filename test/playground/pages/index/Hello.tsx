export { Hello }

import React, { useState, useEffect } from 'react'
import { onLoad, onStream } from './Hello.telefunc'

function Hello() {
  const [msg, setMsg] = useState<string | null>(null)
  const [streamText, setStreamText] = useState('')
  useEffect(() => {
    ;(async () => {
      const { message } = await onLoad({ name: 'Eva' })
      setMsg(message)
    })()
  }, [])
  return (
    <>
      <p>{!msg ? <>Loading...</> : <>{msg}</>}</p>
      <button
        type="button"
        onClick={async () => {
          let text = ''
          for await (const chunk of onStream()) text += chunk
          setStreamText(text)
        }}
      >
        Read stream
      </button>
      <output>{streamText}</output>
    </>
  )
}
