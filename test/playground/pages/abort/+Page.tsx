export { Page }

import React from 'react'
import { Abort } from './Abort'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Connection Abort</h1>
      <Abort />
    </div>
  )
}
