export { App }

import React, { useEffect, useState } from 'react'
import { bootOnce, useApp } from '../store'
import { AuthScreen } from './AuthScreen'
import { Home } from './Home'
import { Lobby } from './Lobby'
import { Match } from './Match'
import { Toast } from './Toast'

function App() {
  const phase = useApp((s) => s.phase)
  const [hydrated, setHydrated] = useState(false)

  useEffect(() => {
    setHydrated(true)
    bootOnce()
  }, [])

  return (
    <div id={hydrated ? 'hydrated' : undefined} className="w-screen h-screen overflow-hidden">
      {phase === 'boot' && <Splash />}
      {phase === 'auth' && <AuthScreen />}
      {phase === 'home' && <Home />}
      {phase === 'lobby' && <Lobby />}
      {phase === 'match' && <Match />}
      <Toast />
    </div>
  )
}

function Splash() {
  return (
    <div className="w-full h-full grid place-items-center bg-steel-950">
      <div className="text-steel-400 tracking-widest text-sm animate-pulse">LOADING THEATER…</div>
    </div>
  )
}
