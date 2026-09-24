export { Page }

import React from 'react'
import { RefIdentity } from './RefIdentity'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Reference Identity</h1>
      <RefIdentity />
    </div>
  )
}
