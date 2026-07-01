export { Page }

import React from 'react'
import { FileDownload } from './FileDownload'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>File Download</h1>
      <FileDownload />
    </div>
  )
}
