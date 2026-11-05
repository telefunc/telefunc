export { Hud }

import React, { useEffect, useRef } from 'react'
import { Kind, NEUTRAL, STATS } from '../../shared/constants'
import { sendCommand, worldBuffer } from '../net'
import type { Game } from '../engine/game'
import { useApp } from '../store'

const BUILDABLE = [Kind.BARRACKS, Kind.FACTORY, Kind.DEPOT]

function Hud({ game }: { game: Game }) {
  const hud = useApp((s) => s.hud)
  const selection = useApp((s) => s.selection)
  const myTeam = useApp((s) => s.myTeam)
  const mmRef = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    if (mmRef.current) game.attachMinimap(mmRef.current)
  }, [game])

  const crystal = hud?.crystal ?? 0
  const spectator = myTeam === NEUTRAL

  return (
    <div className="absolute bottom-0 left-0 right-0 pointer-events-none">
      <div className="flex items-end justify-between gap-3 p-2">
        {/* Minimap */}
        <div className="pointer-events-auto bg-steel-900/90 border border-steel-700 rounded-xl p-2 backdrop-blur">
          <canvas
            id="minimap-canvas"
            ref={mmRef}
            width={184}
            height={184}
            className="rounded-lg cursor-pointer block"
          />
        </div>

        {/* Command / selection panel */}
        <div className="pointer-events-auto flex-1 max-w-2xl bg-steel-900/90 border border-steel-700 rounded-xl px-4 py-3 backdrop-blur min-h-[104px]">
          {spectator ? (
            <div className="text-steel-400 text-sm">Spectating — watching both sides is not fog-limited.</div>
          ) : (
            <CommandPanel
              game={game}
              selection={selection}
              crystal={crystal}
              supplyLeft={(hud?.supplyCap ?? 0) - (hud?.supplyUsed ?? 0)}
            />
          )}
        </div>

        {/* Resources */}
        <div className="pointer-events-auto bg-steel-900/90 border border-steel-700 rounded-xl px-4 py-3 backdrop-blur text-right min-w-[150px]">
          <Resource label="CRYSTAL" id="crystal" value={crystal} color="#52e0c4" />
          <Resource
            label="SUPPLY"
            id="supply"
            value={`${hud?.supplyUsed ?? 0} / ${hud?.supplyCap ?? 0}`}
            color="#ffcf5c"
          />
          <div className="flex items-center justify-end gap-3 mt-1 text-xs text-steel-400">
            <span id="unit-count">⚔ {hud?.myUnits ?? 0}</span>
            <span id="building-count">⌂ {hud?.myBuildings ?? 0}</span>
          </div>
        </div>
      </div>
    </div>
  )
}

function CommandPanel({
  game,
  selection,
  crystal,
  supplyLeft,
}: {
  game: Game
  selection: ReturnType<typeof useApp.getState>['selection']
  crystal: number
  supplyLeft: number
}) {
  if (!selection || selection.ids.length === 0) {
    return (
      <div className="text-steel-400 text-sm leading-relaxed">
        <div className="text-steel-300 font-semibold mb-1">No selection</div>
        Left-drag to box-select · Right-click to move / attack / gather · <span className="text-steel-300">A</span>{' '}
        attack-move · <span className="text-steel-300">S</span> stop · Arrows / minimap to pan
      </div>
    )
  }

  // Single building → production panel.
  if (selection.building) {
    const b = selection.building
    const s = STATS[b.kind as Kind]
    const live = worldBuffer.entities.get(b.id)
    return (
      <div id="selection-panel">
        <div className="flex items-center gap-2 mb-2">
          <span className="font-bold text-steel-200">{s.name}</span>
          {live && live.progress > 0.001 && live.state !== 6 && (
            <span className="text-xs text-team-blue">producing… {Math.round(live.progress * 100)}%</span>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          {s.produces.map((u) => (
            <ProduceButton key={u} building={b.id} unit={u} crystal={crystal} supplyLeft={supplyLeft} />
          ))}
          {s.produces.length > 0 && (
            <button
              id="cancel-produce"
              onClick={() => sendCommand({ t: 'cancel', building: b.id })}
              className="px-3 py-2 rounded-lg bg-steel-800 border border-steel-700 text-steel-300 text-sm hover:bg-steel-700"
            >
              Cancel
            </button>
          )}
        </div>
        {s.produces.length === 0 && (
          <div className="text-steel-400 text-sm">Supply structure · right-click to set rally</div>
        )}
      </div>
    )
  }

  // Units → summary + (if workers) build menu.
  const counts = new Map<number, number>()
  for (const id of selection.ids) {
    const e = worldBuffer.entities.get(id)
    if (e) counts.set(e.kind, (counts.get(e.kind) ?? 0) + 1)
  }
  return (
    <div id="selection-panel">
      <div id="selection-summary" className="flex flex-wrap gap-x-4 gap-y-1 mb-2 text-sm">
        {[...counts].map(([kind, n]) => (
          <span key={kind} className="text-steel-200">
            <span className="text-steel-400">{n}×</span> {STATS[kind as Kind].name}
          </span>
        ))}
      </div>
      {selection.hasWorker && (
        <div id="build-menu" className="flex flex-wrap gap-2">
          <span className="text-xs text-steel-400 self-center mr-1">BUILD:</span>
          {BUILDABLE.map((k) => (
            <BuildButton key={k} kind={k} crystal={crystal} onClick={() => game.beginPlacement(k)} />
          ))}
        </div>
      )}
    </div>
  )
}

function ProduceButton({
  building,
  unit,
  crystal,
  supplyLeft,
}: { building: number; unit: number; crystal: number; supplyLeft: number }) {
  const s = STATS[unit as Kind]
  const disabled = crystal < s.cost || supplyLeft < s.supply
  return (
    <button
      data-produce={s.name.toLowerCase()}
      disabled={disabled}
      onClick={() => sendCommand({ t: 'produce', building, unit })}
      className="px-3 py-2 rounded-lg bg-steel-800 border border-steel-700 text-left hover:border-steel-500 disabled:opacity-35 transition"
    >
      <div className="text-sm font-medium text-steel-200">{s.name}</div>
      <div className="text-xs text-crystal">◆ {s.cost}</div>
    </button>
  )
}

function BuildButton({ kind, crystal, onClick }: { kind: number; crystal: number; onClick: () => void }) {
  const s = STATS[kind as Kind]
  return (
    <button
      data-build={s.name.toLowerCase()}
      disabled={crystal < s.cost}
      onClick={onClick}
      className="px-3 py-1.5 rounded-lg bg-steel-800 border border-steel-700 text-sm text-steel-200 hover:border-steel-500 disabled:opacity-35 transition"
    >
      {s.name} <span className="text-crystal text-xs">◆{s.cost}</span>
    </button>
  )
}

function Resource({ label, id, value, color }: { label: string; id: string; value: number | string; color: string }) {
  return (
    <div className="flex items-center justify-end gap-2">
      <span className="text-[10px] tracking-widest text-steel-400">{label}</span>
      <span id={id} className="font-mono font-semibold text-lg" style={{ color }}>
        {value}
      </span>
    </div>
  )
}
