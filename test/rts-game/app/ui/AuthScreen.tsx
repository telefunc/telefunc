export { AuthScreen }

import React, { useState } from 'react'
import { signIn, useApp } from '../store'

function AuthScreen() {
  const [name, setName] = useState('')
  const pending = useApp((s) => s.authPending)
  const error = useApp((s) => s.authError)

  const submit = (e: React.FormEvent) => {
    e.preventDefault()
    if (name.trim()) signIn(name.trim())
  }

  return (
    <div className="w-full h-full grid place-items-center bg-steel-950 relative overflow-hidden">
      <Backdrop />
      <form
        onSubmit={submit}
        className="relative z-10 w-[380px] bg-steel-850/90 border border-steel-700 rounded-2xl p-8 shadow-2xl shadow-black/60 backdrop-blur"
      >
        <div className="text-center mb-6">
          <h1 className="text-3xl font-bold tracking-tight text-steel-300">
            TELE<span className="text-team-blue">FRONT</span>
          </h1>
          <p className="text-xs tracking-[0.3em] text-steel-400 mt-1">10&nbsp;V&nbsp;10 · REAL-TIME STRATEGY</p>
        </div>
        <label className="block text-xs text-steel-400 mb-1 tracking-wide">COMMANDER NAME</label>
        <input
          id="auth-name"
          autoFocus
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="e.g. Ironside"
          maxLength={20}
          className="w-full px-3 py-2 rounded-lg bg-steel-900 border border-steel-700 text-steel-200 outline-none focus:border-team-blue"
        />
        {error && <div className="text-team-red text-xs mt-2">{error}</div>}
        <button
          id="auth-submit"
          type="submit"
          disabled={pending || !name.trim()}
          className="mt-5 w-full py-2.5 rounded-lg bg-team-blue text-steel-950 font-semibold tracking-wide disabled:opacity-40 hover:brightness-110 transition"
        >
          {pending ? 'DEPLOYING…' : 'ENTER WAR ROOM'}
        </button>
      </form>
    </div>
  )
}

function Backdrop() {
  return (
    <div
      className="absolute inset-0 opacity-40"
      style={{
        backgroundImage:
          'radial-gradient(circle at 20% 30%, rgba(77,157,255,0.15), transparent 40%), radial-gradient(circle at 80% 70%, rgba(255,77,94,0.12), transparent 40%), linear-gradient(rgba(30,39,64,0.4) 1px, transparent 1px), linear-gradient(90deg, rgba(30,39,64,0.4) 1px, transparent 1px)',
        backgroundSize: '100% 100%, 100% 100%, 44px 44px, 44px 44px',
      }}
    />
  )
}
