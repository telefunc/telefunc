export { Toast }

import React from 'react'
import { useApp } from '../store'

function Toast() {
  const toast = useApp((s) => s.toast)
  if (!toast) return null
  return (
    <div className="fixed top-4 left-1/2 -translate-x-1/2 z-50 pointer-events-none">
      <div
        id="toast"
        className="px-4 py-2 rounded-lg bg-steel-800 border border-steel-600 text-steel-300 text-sm shadow-2xl shadow-black/50"
      >
        {toast}
      </div>
    </div>
  )
}
