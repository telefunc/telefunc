export { Lobby }

import React from 'react'
import { BLUE, RED, TEAM_COLOR, type Team } from '../../shared/constants'
import { leaveMatch, setTeam, startMatch, toggleReady, useApp } from '../store'
import { Chat } from './Chat'

function Lobby() {
  const meta = useApp((s) => s.matchMeta)
  const roster = useApp((s) => s.roster)
  const myTeam = useApp((s) => s.myTeam)
  const ready = useApp((s) => s.ready)
  const isHost = useApp((s) => s.isHost)

  const red = roster.filter((p) => p.team === RED)
  const blue = roster.filter((p) => p.team === BLUE)
  const canStart = red.length >= 1 && blue.length >= 1 && roster.every((p) => p.ready)

  return (
    <div id="lobby" className="w-full h-full bg-steel-950 flex flex-col">
      <header className="flex items-center justify-between px-8 py-4 border-b border-steel-800">
        <div>
          <h1 id="lobby-title" className="text-lg font-bold text-steel-200">
            {meta?.name ?? 'Battle'}
          </h1>
          <p className="text-xs text-steel-400">Assemble your teams · both sides need a commander</p>
        </div>
        <button
          id="leave-lobby"
          onClick={() => leaveMatch()}
          className="text-steel-400 hover:text-steel-200 text-sm transition"
        >
          Leave
        </button>
      </header>

      <div className="flex-1 grid grid-cols-[1fr_1fr_320px] gap-6 p-8 min-h-0">
        <TeamColumn team={RED} title="RED COMMAND" players={red} myTeam={myTeam} />
        <TeamColumn team={BLUE} title="BLUE COMMAND" players={blue} myTeam={myTeam} />

        <div className="flex flex-col gap-4 min-h-0">
          <div className="bg-steel-850/80 border border-steel-700 rounded-2xl p-4">
            <div className="text-xs tracking-widest text-steel-400 mb-3">YOUR ORDERS</div>
            <div className="flex gap-2 mb-3">
              <TeamBtn
                label="Join Red"
                id="team-red"
                active={myTeam === RED}
                color={TEAM_COLOR[RED]}
                onClick={() => setTeam(RED)}
              />
              <TeamBtn
                label="Join Blue"
                id="team-blue"
                active={myTeam === BLUE}
                color={TEAM_COLOR[BLUE]}
                onClick={() => setTeam(BLUE)}
              />
            </div>
            <button
              id="ready-btn"
              onClick={() => toggleReady()}
              className={`w-full py-2 rounded-lg font-semibold transition ${
                ready ? 'bg-crystal text-steel-950' : 'bg-steel-700 text-steel-200 hover:bg-steel-600'
              }`}
            >
              {ready ? '✓ READY' : 'READY UP'}
            </button>
            {isHost && (
              <button
                id="start-btn"
                disabled={!canStart}
                onClick={() => startMatch()}
                className="w-full mt-2 py-2.5 rounded-lg bg-team-blue text-steel-950 font-bold tracking-wide disabled:opacity-30 hover:brightness-110 transition"
              >
                LAUNCH BATTLE
              </button>
            )}
            {!isHost && <div className="text-center text-xs text-steel-400 mt-2">Waiting for the host to launch…</div>}
          </div>

          <div className="flex-1 bg-steel-850/80 border border-steel-700 rounded-2xl p-4 min-h-0">
            <div className="text-xs tracking-widest text-steel-400 mb-2">COMMS</div>
            <Chat id="lobby-chat" />
          </div>
        </div>
      </div>
    </div>
  )
}

function TeamColumn({
  team,
  title,
  players,
  myTeam,
}: {
  team: Team
  title: string
  players: { identity: string; name: string; ready: boolean }[]
  myTeam: Team
}) {
  const color = TEAM_COLOR[team]
  const hex = '#' + color.toString(16).padStart(6, '0')
  return (
    <div
      className="bg-steel-850/60 border rounded-2xl p-5 min-h-0 flex flex-col"
      style={{ borderColor: hex + '55' }}
      data-team-column={team}
    >
      <div className="flex items-center gap-2 mb-4">
        <div className="w-3 h-3 rounded-full" style={{ background: hex }} />
        <h2 className="font-bold tracking-wide" style={{ color: hex }}>
          {title}
        </h2>
        <span className="ml-auto text-xs text-steel-400">{players.length}</span>
      </div>
      <div className="space-y-2 overflow-y-auto">
        {players.map((p) => (
          <div
            key={p.identity}
            data-lobby-player={p.name}
            className="flex items-center gap-2 px-3 py-2 rounded-lg bg-steel-900/70 border border-steel-800"
          >
            <span className="text-steel-200 font-medium">{p.name}</span>
            {p.ready ? (
              <span className="ml-auto text-crystal text-xs" data-ready>
                READY
              </span>
            ) : (
              <span className="ml-auto text-steel-500 text-xs">…</span>
            )}
          </div>
        ))}
        {players.length === 0 && <div className="text-steel-500 text-sm italic px-3 py-2">Awaiting commanders…</div>}
      </div>
      {myTeam === team && (
        <div className="mt-auto pt-3 text-xs text-center" style={{ color: hex }}>
          You command here
        </div>
      )}
    </div>
  )
}

function TeamBtn({
  label,
  id,
  active,
  color,
  onClick,
}: { label: string; id: string; active: boolean; color: number; onClick: () => void }) {
  const hex = '#' + color.toString(16).padStart(6, '0')
  return (
    <button
      id={id}
      onClick={onClick}
      className="flex-1 py-2 rounded-lg text-sm font-semibold border transition"
      style={{
        background: active ? hex : 'transparent',
        color: active ? '#05070d' : hex,
        borderColor: hex + (active ? '' : '66'),
      }}
    >
      {label}
    </button>
  )
}
