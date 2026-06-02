export { Page }

import React, { useEffect, useState } from 'react'
import { Hello } from './Hello'

function Page() {
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  return (
    <div id={hydrated ? 'hydrated' : undefined} className="max-w-3xl mx-auto px-8 py-10">
      <h1>Welcome</h1>
      <Counter />
      <Hello />
    </div>
  )
}

function Counter() {
  const [count, setCount] = useState(0)
  return (
    <div>
      This page is interactive:
      <button type="button" onClick={() => setCount((count) => count + 1)}>
        Counter {count}
      </button>
    </div>
  )
}
