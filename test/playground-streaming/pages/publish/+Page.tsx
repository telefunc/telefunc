export { Page }

import React from 'react'
import { Publish } from './Publish'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Publish</h1>
      <Publish />
    </div>
  )
}
