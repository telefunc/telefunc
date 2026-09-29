export { Layout }

import React from 'react'
import './global.css'

function Layout({ children }: { children: React.ReactNode }) {
  // No <React.StrictMode> here: the match renderer owns a PixiJS canvas and a single WebGL
  // context whose lifecycle is driven by explicit user events (enter match / leave), and
  // StrictMode's double-invoked mount effects would build and tear down the GL context twice
  // on every mount. Room wiring is likewise event-driven (never in render), so nothing here
  // relies on StrictMode to catch double-subscribes.
  return <>{children}</>
}
