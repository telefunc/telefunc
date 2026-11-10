export { Page }

import React from 'react'
import { WebTransportTest } from './WebTransportTest'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>WebTransport</h1>
      <WebTransportTest />
    </div>
  )
}
