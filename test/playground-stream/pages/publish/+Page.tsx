export { Page }

import React from 'react'
import { Publish } from './Publish'
import { RedisReconnect } from './RedisReconnect'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>Publish</h1>
      <Publish />
      <RedisReconnect />
    </div>
  )
}
