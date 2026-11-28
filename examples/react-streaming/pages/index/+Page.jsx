import React, { Suspense, useState, useEffect } from 'react'
import { Profile, TodoList } from './TodoList'

export { Page }

function Page({ showProfile }) {
  return (
    <>
      <h1>To-do List</h1>
      <Timer />
      <Suspense fallback={<div>Loading...</div>}>
        {showProfile && <Profile />}
        <TodoList />
      </Suspense>
      <Counter />
    </>
  )
}

function Timer() {
  const [counter, setCounter] = useState(0)
  useEffect(() => {
    setTimeout(() => {
      setCounter(counter + 1)
    }, 100)
  })
  return <p>(This page is interactive while data is loading: {counter})</p>
}

function Counter() {
  const [count, setCount] = useState(0)
  return (
    <div>
      <button type="button" onClick={() => setCount((count) => count + 1)}>
        Counter {count}
      </button>
    </div>
  )
}
