export { Counter }

import React, { useEffect, useState } from 'react'

function Counter() {
  const [count, setCount] = useState(0)
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  return (
    <button id={hydrated ? 'hydrated' : undefined} type="button" onClick={() => setCount((count) => count + 1)}>
      Counter {count}
    </button>
  )
}
