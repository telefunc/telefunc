export { Match }

import React, { useEffect, useRef, useState } from 'react'
import { BLUE, NEUTRAL, RED, TEAM_COLOR, TEAM_NAME, type Team } from '../../shared/constants'
import { sendCommand } from '../net'
import { getMyTeam, leaveMatch, setHud, setSelection, useApp } from '../store'
import { Game } from '../engine/game'
import { Hud } from './Hud'
import { Chat } from './Chat'

function Match() {
  const hostRef = useRef<HTMLDivElement>(null)
  const [game, setGame] = useState<Game | null>(null)

  useEffect(() => {
    let g: Game | null = null
    let disposed = false
    ;(async () => {
      const host = hostRef.current
      if (!host) return
      g = new Game(host, getMyTeam, setSelection, setHud)
      setGame(g)
      await g.start()
      if (disposed) g.dispose()
    })()
    return () => {
      disposed = true
      g?.dispose()
      setGame(null)
    }
  }, [])

  return (
    <div id="match" className="relative w-screen h-screen overflow-hidden no-select bg-steel-950">
      <div ref={hostRef} className="absolute inset-0" />
      <TopBar />
      {game && <Hud game={game} />}
      <ChatDock />
      <Result />
    </div>
  )
}

function TopBar() {
  const hud = useApp((s) => s.hud)
  const myTeam = useApp((s) => s.myTeam)
  const meta = useApp((s) => s.matchMeta)
  const clock = hud ? fmt(hud.clockSec) : '0:00'
  const teamHex = myTeam === NEUTRAL ? '#9aa8cc' : '#' + TEAM_COLOR[myTeam].toString(16).padStart(6, '0')

  return (
    <div className="absolute top-0 left-0 right-0 flex items-center justify-between px-4 py-2 pointer-events-none">
      <div className="pointer-events-auto flex items-center gap-3 bg-steel-900/85 border border-steel-700 rounded-xl px-4 py-1.5 backdrop-blur">
        <span className="font-bold text-steel-200 text-sm">{meta?.name}</span>
        <span className="text-xs px-2 py-0.5 rounded" style={{ background: teamHex + '22', color: teamHex }}>
          {myTeam === NEUTRAL ? 'SPECTATOR' : `${TEAM_NAME[myTeam]} TEAM`}
        </span>
      </div>
      <div className="pointer-events-auto flex items-center gap-2">
        <div
          id="clock"
          className="bg-steel-900/85 border border-steel-700 rounded-xl px-4 py-1.5 font-mono text-steel-200 text-sm backdrop-blur"
        >
          {clock}
        </div>
        {myTeam !== NEUTRAL && (
          <button
            id="resign-btn"
            onClick={() => sendCommand({ t: 'resign' })}
            className="bg-steel-900/85 border border-steel-700 rounded-xl px-3 py-1.5 text-team-red text-sm hover:bg-steel-800 transition backdrop-blur"
          >
            Resign
          </button>
        )}
        <button
          id="leave-btn"
          onClick={() => leaveMatch()}
          className="bg-steel-900/85 border border-steel-700 rounded-xl px-3 py-1.5 text-steel-300 text-sm hover:bg-steel-800 transition backdrop-blur"
        >
          Leave
        </button>
      </div>
    </div>
  )
}

function ChatDock() {
  const [open, setOpen] = useState(false)
  return (
    <div className="absolute bottom-2 left-2 w-72 pointer-events-auto">
      <div className="bg-steel-900/85 border border-steel-700 rounded-xl backdrop-blur">
        <button
          id="chat-toggle"
          onClick={() => setOpen((o) => !o)}
          className="w-full text-left px-3 py-1.5 text-xs tracking-widest text-steel-400 hover:text-steel-200"
        >
          COMMS {open ? '▾' : '▸'}
        </button>
        {open && (
          <div className="px-3 pb-3">
            <Chat id="match-chat" compact />
          </div>
        )}
      </div>
    </div>
  )
}

function Result() {
  const result = useApp((s) => s.result)
  const myTeam = useApp((s) => s.myTeam)
  if (!result) return null
  const won = result.winner === myTeam
  const draw = result.winner === NEUTRAL
  const winnerHex = result.winner === RED ? '#ff4d5e' : result.winner === BLUE ? '#4d9dff' : '#9aa8cc'

  return (
    <div id="result" className="absolute inset-0 grid place-items-center bg-black/70 backdrop-blur-sm z-40">
      <div className="text-center">
        <div
          id={won ? 'result-victory' : draw ? 'result-draw' : 'result-defeat'}
          className="text-6xl font-black tracking-tight mb-2"
          style={{ color: draw ? '#9aa8cc' : won ? '#8be08a' : '#ff5a5a' }}
        >
          {draw ? 'NO CONTEST' : won ? 'VICTORY' : 'DEFEAT'}
        </div>
        {!draw && (
          <div className="text-steel-300 mb-6" style={{ color: winnerHex }}>
            {TEAM_NAME[result.winner as Team]} team wins the battle
          </div>
        )}
        <button
          id="result-leave"
          onClick={() => leaveMatch()}
          className="px-6 py-2.5 rounded-lg bg-team-blue text-steel-950 font-semibold hover:brightness-110 transition"
        >
          Return to base
        </button>
      </div>
    </div>
  )
}

function fmt(sec: number): string {
  const m = Math.floor(sec / 60)
  const s = sec % 60
  return `${m}:${s.toString().padStart(2, '0')}`
}
