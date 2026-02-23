export { Layout }

import React from 'react'
import './Layout.css'

const nav = [
  { href: '/', label: 'Home' },
  { href: '/about', label: 'About' },
  { href: '/streaming', label: 'Streaming' },
  { href: '/file-upload', label: 'Upload' },
  { href: '/chat', label: 'Chat' },
]

function Layout({ children }: { children: React.ReactNode }) {
  return (
    <React.StrictMode>
      <div className="h-screen flex overflow-hidden bg-white text-zinc-900 antialiased">
        {/* Sidebar */}
        <nav className="shrink-0 w-48 border-r border-zinc-100 flex flex-col">
          <div className="px-5 pt-6 pb-4">
            <span className="text-sm font-semibold tracking-tight text-zinc-900">Telefunc</span>
          </div>
          <div className="flex-1 flex flex-col gap-0.5 px-3">
            {nav.map(({ href, label }) => (
              <a
                key={href}
                href={href}
                className="block px-3 py-1.5 rounded-md text-[13px] text-zinc-600 hover:text-zinc-900 hover:bg-zinc-50 transition-colors no-underline"
              >
                {label}
              </a>
            ))}
          </div>
          <div className="px-5 py-4 text-[11px] text-zinc-400">Playground</div>
        </nav>

        {/* Content */}
        <main className="flex-1 min-w-0 overflow-y-auto">{children}</main>
      </div>
    </React.StrictMode>
  )
}
