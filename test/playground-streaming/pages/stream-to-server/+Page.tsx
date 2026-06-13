export { Page }

import React from 'react'
import { StreamToServer } from './StreamToServer'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Stream to Server</h1>
      <StreamToServer />
    </div>
  )
}
