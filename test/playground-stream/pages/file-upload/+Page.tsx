export { Page }

import React from 'react'
import { FileUpload } from './FileUpload'

function Page() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10">
      <h1>File Upload</h1>
      <FileUpload />
    </div>
  )
}
