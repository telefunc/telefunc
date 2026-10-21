export { Page }

import React from 'react'
import { Backlog } from './Backlog'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Backlog</h1>
      <Backlog />
    </div>
  )
}
