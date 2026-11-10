export { Page }

import React from 'react'
import { Meeting } from './Meeting'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Media</h1>
      <Meeting />
    </div>
  )
}
