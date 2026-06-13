export { Page }

import React, { useState } from 'react'
import { Hello } from './Hello'

function Page() {
  return (
    <>
      <h1>Welcome</h1>
      <Counter />
      <Hello />
    </>
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
