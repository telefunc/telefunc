export { Layout }

import React from 'react'
import './global.css'

function Layout({ children }: { children: React.ReactNode }) {
  return <React.StrictMode>{children}</React.StrictMode>
}
