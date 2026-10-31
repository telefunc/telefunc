export { CallView }

import React from 'react'
import { getSelfStream, leaveCall, registerTile, toggleCamera, toggleMute, toggleScreenShare } from '../call'
import type { CallState, Member } from '../store'
import { useApp } from '../store'
import { IconMic, IconMicOff, IconMonitor, IconPhoneOff, IconVideo, IconVideoOff, IconVolume } from './Icons'

/**
 * The main pane while viewing the connected voice channel: a tile per participant (camera video
 * or avatar), screen shares as wide tiles, and the control bar. Tiles come straight from the
 * voice room's roster — camera/screen/mute flags ride each member's metadata, so late joiners
 * see the right tiles immediately; the pixels arrive with the next keyframe.
 */
function CallView() {
  const call = useApp((s) => s.call)
  const channels = useApp((s) => s.channels)
  const channel = channels.find((c) => c.id === call?.channelId)
  if (!call || !channel) return <main className="flex-1 bg-[#1e1f22]" />

  const shares = channel.occupants.filter((o) => o.screen)

  return (
    <main className="flex-1 min-w-0 flex flex-col bg-[#1e1f22]">
      <header className="shrink-0 h-12 flex items-center gap-2 px-4 text-[#dbdee1] border-b border-[#26282c]">
        <span className="text-[#80848e]">
          <IconVolume />
        </span>
        <span className="font-semibold text-white">{channel.name}</span>
        <span className="text-[12px] text-[#80848e]">
          {channel.occupants.length}/{channel.size} connected
        </span>
      </header>

      <div className="flex-1 min-h-0 overflow-y-auto p-4 flex flex-col gap-4">
        {/* Screen shares first, full width. */}
        {shares.map((occupant) => (
          <div
            key={`screen:${occupant.participantId}`}
            data-screen-tile={occupant.name}
            className="relative w-full aspect-video max-h-[55vh] bg-black rounded-lg overflow-hidden"
          >
            <TileVideo occupant={occupant} kind="screen" isSelf={occupant.participantId === call.myParticipantId} />
            <NameChip occupant={occupant} suffix="'s screen" />
          </div>
        ))}

        {/* One tile per connected participant. */}
        <div className="grid gap-4 [grid-template-columns:repeat(auto-fit,minmax(280px,1fr))]">
          {channel.occupants.map((occupant) => {
            const isSelf = occupant.participantId === call.myParticipantId
            return (
              <div
                key={occupant.participantId}
                data-tile={occupant.name}
                className="relative aspect-video bg-[#2b2d31] rounded-lg overflow-hidden flex items-center justify-center"
              >
                {occupant.camera ? (
                  <TileVideo occupant={occupant} kind="camera" isSelf={isSelf} />
                ) : (
                  <span className="w-16 h-16 rounded-full" style={{ background: occupant.color }} />
                )}
                <NameChip occupant={occupant} suffix={isSelf ? ' (you)' : ''} />
              </div>
            )
          })}
        </div>
      </div>

      <ControlBar call={call} />
    </main>
  )
}

/** My tiles render the local capture stream directly (nothing of mine echoes back over the
 *  wire — `selfDelivery: false`); everyone else's render decoded frames via the tile registry. */
function TileVideo({ occupant, kind, isSelf }: { occupant: Member; kind: 'camera' | 'screen'; isSelf: boolean }) {
  if (isSelf) {
    return (
      <video
        autoPlay
        muted
        playsInline
        className="w-full h-full object-contain"
        ref={(el) => {
          const stream = getSelfStream(kind)
          if (el && el.srcObject !== stream) el.srcObject = stream
        }}
      />
    )
  }
  return (
    <canvas
      className="w-full h-full object-contain"
      ref={(el) => registerTile(occupant.participantId, kind, el)} // decoder draws in; ref(null) detaches
    />
  )
}

function NameChip({ occupant, suffix }: { occupant: Member; suffix: string }) {
  return (
    <span className="absolute bottom-2 left-2 flex items-center gap-1.5 bg-black/60 text-white text-[12px] px-2 py-0.5 rounded">
      {occupant.muted && (
        <span data-muted className="text-[#f23f43]">
          <IconMicOff size={12} />
        </span>
      )}
      {occupant.name}
      {suffix}
    </span>
  )
}

function ControlBar({ call }: { call: CallState }) {
  return (
    <div className="shrink-0 flex items-center justify-center gap-3 py-4">
      <ControlButton
        id="call-mute"
        title={call.muted ? 'Unmute' : 'Mute'}
        active={!call.muted}
        disabled={!call.micAvailable}
        onClick={() => void toggleMute()}
      >
        {call.muted ? <IconMicOff size={20} /> : <IconMic size={20} />}
      </ControlButton>
      <ControlButton
        id="call-camera"
        title={call.cameraOn ? 'Turn off camera' : 'Turn on camera'}
        active={call.cameraOn}
        onClick={() => void toggleCamera()}
      >
        {call.cameraOn ? <IconVideo size={20} /> : <IconVideoOff size={20} />}
      </ControlButton>
      <ControlButton
        id="call-screen"
        title={call.screenOn ? 'Stop sharing' : 'Share your screen'}
        active={call.screenOn}
        onClick={() => void toggleScreenShare()}
      >
        <IconMonitor size={20} />
      </ControlButton>
      <button
        id="call-leave"
        title="Disconnect"
        onClick={() => void leaveCall()}
        className="w-12 h-12 rounded-full bg-[#f23f43] text-white flex items-center justify-center cursor-pointer hover:bg-[#d83c3e]"
      >
        <IconPhoneOff size={20} />
      </button>
    </div>
  )
}

function ControlButton({
  id,
  title,
  active,
  disabled,
  onClick,
  children,
}: {
  id: string
  title: string
  active: boolean
  disabled?: boolean
  onClick: () => void
  children: React.ReactNode
}) {
  return (
    <button
      id={id}
      title={title}
      disabled={disabled}
      onClick={onClick}
      className={`w-12 h-12 rounded-full flex items-center justify-center cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
        active ? 'bg-[#dbdee1] text-[#1e1f22] hover:bg-white' : 'bg-[#2b2d31] text-white hover:bg-[#35373c]'
      }`}
    >
      {children}
    </button>
  )
}
