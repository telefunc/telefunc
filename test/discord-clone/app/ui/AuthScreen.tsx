export { AuthScreen }

import React, { useState } from 'react'
import { loginUser, registerUser, useApp } from '../store'

const COLORS = ['#f472b6', '#fb923c', '#facc15', '#4ade80', '#2dd4bf', '#60a5fa', '#a78bfa', '#f87171']

/** Register / log in against the session API (httpOnly cookie — see server/api.ts). */
function AuthScreen() {
  const authError = useApp((s) => s.authError)
  const authPending = useApp((s) => s.authPending)
  const [mode, setMode] = useState<'login' | 'register'>('register')
  const [color, setColor] = useState(COLORS[5]!)

  return (
    <div className="flex-1 flex items-center justify-center">
      <form
        className="w-[420px] bg-[#2b2d31] rounded-xl p-8 shadow-2xl"
        onSubmit={(e) => {
          e.preventDefault()
          const form = new FormData(e.currentTarget)
          const name = String(form.get('name') ?? '').trim()
          const password = String(form.get('password') ?? '')
          if (mode === 'register') void registerUser(name, password, color)
          else void loginUser(name, password)
        }}
      >
        <h1 className="text-[20px] font-semibold text-white text-center">Telefunc HQ</h1>
        <p className="text-[13px] text-[#949ba4] text-center mt-1 mb-6">
          {mode === 'register' ? 'Create your account — the first account owns the server.' : 'Welcome back.'}
        </p>

        <div className="flex rounded-[4px] overflow-hidden mb-5 text-[13px] font-medium">
          {(['register', 'login'] as const).map((m) => (
            <button
              key={m}
              id={`auth-tab-${m}`}
              type="button"
              onClick={() => setMode(m)}
              className={`flex-1 py-1.5 cursor-pointer capitalize ${
                mode === m ? 'bg-[#5865f2] text-white' : 'bg-[#1e1f22] text-[#949ba4] hover:text-white'
              }`}
            >
              {m}
            </button>
          ))}
        </div>

        <label className="block text-[11px] font-semibold tracking-wide text-[#b5bac1] mb-1.5">USERNAME</label>
        <input
          id="auth-name"
          name="name"
          autoComplete="username"
          placeholder="e.g. alice"
          className="w-full bg-[#1e1f22] rounded-[4px] px-3 py-2.5 text-[14px] text-[#dbdee1] outline-none placeholder:text-[#5c5e66]"
        />

        <label className="block text-[11px] font-semibold tracking-wide text-[#b5bac1] mt-4 mb-1.5">PASSWORD</label>
        <input
          id="auth-password"
          name="password"
          type="password"
          autoComplete={mode === 'register' ? 'new-password' : 'current-password'}
          placeholder={mode === 'register' ? 'At least 8 characters' : ''}
          className="w-full bg-[#1e1f22] rounded-[4px] px-3 py-2.5 text-[14px] text-[#dbdee1] outline-none placeholder:text-[#5c5e66]"
        />

        {mode === 'register' && (
          <>
            <label className="block text-[11px] font-semibold tracking-wide text-[#b5bac1] mt-4 mb-1.5">COLOR</label>
            <div className="flex gap-2">
              {COLORS.map((c) => (
                <button
                  key={c}
                  type="button"
                  data-color={c}
                  onClick={() => setColor(c)}
                  className="w-7 h-7 rounded-full transition-transform"
                  style={{ background: c, transform: color === c ? 'scale(1.25)' : undefined }}
                  aria-label={`color ${c}`}
                />
              ))}
            </div>
          </>
        )}

        {authError && (
          <div id="auth-error" className="mt-4 text-[13px] text-[#fa777c]">
            {authError}
          </div>
        )}

        <button
          id="auth-submit"
          type="submit"
          disabled={authPending}
          className="mt-6 w-full bg-[#5865f2] hover:bg-[#4752c4] disabled:opacity-50 text-white font-medium rounded-[4px] py-2.5 transition-colors cursor-pointer"
        >
          {authPending ? 'One moment…' : mode === 'register' ? 'Create account & join' : 'Log in'}
        </button>
      </form>
    </div>
  )
}
