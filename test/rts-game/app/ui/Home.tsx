export { Home }

import React, { useState } from 'react'
import { createMatch, joinMatch, signOut, useApp } from '../store'

function Home() {
  const user = useApp((s) => s.user)
  const matches = useApp((s) => s.matches)
  const [name, setName] = useState('')

  return (
    <div className="w-full h-full bg-steel-950 flex flex-col">
      <header className="flex items-center justify-between px-8 py-4 border-b border-steel-800">
        <h1 className="text-xl font-bold tracking-tight text-steel-300">
          TELE<span className="text-team-blue">FRONT</span>
        </h1>
        <div className="flex items-center gap-3 text-sm">
          <span className="text-steel-400">
            Commander{' '}
            <span className="font-semibold" style={{ color: user?.color }}>
              {user?.name}
            </span>
          </span>
          <button id="sign-out" onClick={() => signOut()} className="text-steel-400 hover:text-steel-200 transition">
            Sign out
          </button>
        </div>
      </header>

      <div className="flex-1 grid place-items-center p-8">
        <div className="w-full max-w-2xl">
          <div className="bg-steel-850/80 border border-steel-700 rounded-2xl p-6 mb-6">
            <h2 className="text-sm tracking-widest text-steel-400 mb-3">CREATE A BATTLE</h2>
            <form
              onSubmit={(e) => {
                e.preventDefault()
                createMatch(name.trim() || 'Skirmish')
                setName('')
              }}
              className="flex gap-3"
            >
              <input
                id="create-match-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Battle name"
                maxLength={28}
                className="flex-1 px-3 py-2 rounded-lg bg-steel-900 border border-steel-700 text-steel-200 outline-none focus:border-team-blue"
              />
              <button
                id="create-match"
                type="submit"
                className="px-5 py-2 rounded-lg bg-team-blue text-steel-950 font-semibold hover:brightness-110 transition"
              >
                Create
              </button>
            </form>
          </div>

          <h2 className="text-sm tracking-widest text-steel-400 mb-3 px-1">OPEN BATTLES</h2>
          <div id="match-list" className="space-y-2">
            {matches.length === 0 && (
              <div className="text-steel-400 text-sm px-4 py-8 text-center border border-dashed border-steel-700 rounded-xl">
                No open battles. Create one to begin.
              </div>
            )}
            {matches.map((m) => (
              <div
                key={m.id}
                data-match={m.id}
                className="flex items-center justify-between px-4 py-3 rounded-xl bg-steel-850 border border-steel-700 hover:border-steel-600 transition"
              >
                <div>
                  <div className="font-semibold text-steel-200">{m.name}</div>
                  <div className="text-xs text-steel-400">
                    {m.players} / {m.size} commanders · Lobby
                  </div>
                </div>
                <button
                  data-join={m.id}
                  onClick={() => joinMatch(m.id)}
                  className="px-4 py-1.5 rounded-lg bg-steel-700 text-steel-200 font-medium hover:bg-steel-600 transition"
                >
                  Join
                </button>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
