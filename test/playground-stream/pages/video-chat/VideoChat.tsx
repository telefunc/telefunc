export { VideoChat }

import React, { useEffect, useRef, useState, useSyncExternalStore } from 'react'
import { onGetCall, onJoinCall, onSwitchHere } from './VideoChat.telefunc'

type Call = Awaited<ReturnType<typeof onGetCall>>
type Me = Awaited<ReturnType<typeof onJoinCall>>

declare const MediaStreamTrackProcessor: {
  new (opts: { track: MediaStreamTrack }): { readable: ReadableStream<VideoFrame> }
}

function VideoChat() {
  const [call, setCall] = useState<Call | null>(null)
  useEffect(() => void onGetCall().then(setCall), [])
  if (!call) return null
  return (
    <>
      <InTheCall call={call} />
      <div style={{ display: 'flex' }}>
        <Person call={call} name="Alice" />
        <div style={{ width: 1, background: '#e5e7eb' }} />
        <Person call={call} name="Bob" />
      </div>
    </>
  )
}

function InTheCall({ call }: { call: Call }) {
  const { participants } = useSyncExternalStore(call.onChange, call.snapshot)
  const names = participants.map((p) => (p.isConnected ? p.meta.name : `${p.meta.name} (reconnecting…)`))
  return <p style={{ margin: 20 }}>In the call: {names.join(', ')}</p>
}

function Person({ call, name }: { call: Call; name: string }) {
  const { participants } = useSyncExternalStore(call.onChange, call.snapshot)
  const [me, setMe] = useState<Me | null>(null)
  const [movedAway, setMovedAway] = useState(false)
  const [messages, setMessages] = useState<Array<{ name: string; text: string }>>([])
  const [draft, setDraft] = useState('')
  const localVideo = useRef<HTMLVideoElement>(null)
  const remoteVideo = useRef<HTMLCanvasElement>(null)
  const inOtherTab = !me && participants.some((p) => p.identity === name)
  const others = participants.filter((p) => p.id !== me?.id)
  const remoteNotice = !me
    ? null
    : others.length === 0
      ? 'Waiting for someone to join…'
      : others.every((p) => !p.isConnected)
        ? `${others[0]!.meta.name} is reconnecting…`
        : null

  async function join() {
    const camera = await navigator.mediaDevices.getUserMedia({ video: { width: 320, height: 240, frameRate: 15 } })
    localVideo.current!.srcObject = camera
    const me = inOtherTab ? await onSwitchHere(name) : await onJoinCall(name)

    const stopReceiving = showVideo(call, me.id, remoteVideo.current!)
    const stopSending = sendVideo(camera, me)
    setMessages([])
    const stopChat = call.subscribe((message, _info, from) => {
      setMessages((messages) => [...messages.slice(-49), { name: from.meta.name, text: message.text }])
    })

    me.onLeave((cause) => {
      stopChat()
      stopSending()
      stopReceiving()
      for (const track of camera.getTracks()) track.stop()
      setMovedAway(cause.type === 'removed')
      setMe(null)
    })
    setMovedAway(false)
    setMe(me)
  }

  async function send(event: React.FormEvent) {
    event.preventDefault()
    setDraft('')
    await me!.publish({ text: draft })
  }

  return (
    <section style={{ flex: 1, padding: 20, display: 'flex', flexDirection: 'column', gap: 12 }}>
      <h2 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>{name}</h2>
      <div style={{ display: 'flex', gap: 12 }}>
        <Video label="Local">
          <video ref={localVideo} width={320} height={240} autoPlay muted playsInline style={videoStyle} />
        </Video>
        <Video label="Remote" notice={remoteNotice}>
          <canvas ref={remoteVideo} width={320} height={240} style={videoStyle} />
        </Video>
      </div>
      {movedAway && <p style={{ margin: 0 }}>{name} moved to another tab</p>}
      {!movedAway && inOtherTab && <p style={{ margin: 0 }}>{name} is already in the call in another tab</p>}
      {me ? (
        <button onClick={() => me.leave()} style={{ ...buttonStyle, color: '#dc2626' }}>
          Leave
        </button>
      ) : (
        <button onClick={join} style={buttonStyle}>
          {inOtherTab ? 'Switch here' : `Join as ${name}`}
        </button>
      )}
      <ul aria-label="Chat" style={{ margin: 0, padding: 8, height: 160, overflowY: 'auto', background: '#f9fafb' }}>
        {messages.map((message, i) => (
          <li key={i} style={{ listStyle: 'none' }}>
            <b>{message.name}:</b> {message.text}
          </li>
        ))}
      </ul>
      <form onSubmit={send} style={{ display: 'flex', gap: 8 }}>
        <input value={draft} onChange={(e) => setDraft(e.target.value)} disabled={!me} placeholder="Message" />
        <button disabled={!me || !draft}>Send</button>
      </form>
    </section>
  )
}

function Video({ label, notice, children }: { label: string; notice?: string | null; children: React.ReactNode }) {
  return (
    <div>
      <div style={{ fontSize: 11, color: '#9ca3af', marginBottom: 4 }}>{label}</div>
      <div style={{ position: 'relative' }}>
        {children}
        {notice && <div style={noticeStyle}>{notice}</div>}
      </div>
    </div>
  )
}

const videoStyle = { background: '#111', borderRadius: 6, display: 'block' }
const buttonStyle = { alignSelf: 'flex-start', padding: '8px 20px' }
const noticeStyle = {
  position: 'absolute',
  inset: 0,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  color: '#e5e7eb',
  background: 'rgba(17, 17, 17, 0.8)',
  borderRadius: 6,
} as const

// Draws the other person's video.
function showVideo(call: Call, myId: string, canvas: HTMLCanvasElement) {
  const ctx = canvas.getContext('2d')!
  // One decoder per sender: VP8 frames of two senders through one decoder corrupt the picture
  const decoders = new Map<string, VideoDecoder>()
  const stopFrames = call.subscribeBinary(
    (frame, info, from) => {
      if (from.id === myId) return
      const isKeyframe = info.meta?.isKeyframe === true
      let decoder = decoders.get(from.id)
      if (!decoder) {
        if (!isKeyframe) return
        decoder = new VideoDecoder({
          output(videoFrame) {
            ctx.drawImage(videoFrame, 0, 0, 320, 240)
            videoFrame.close()
          },
          error(err) {
            console.error('decode error:', err)
          },
        })
        decoder.configure({ codec: 'vp8', codedWidth: 320, codedHeight: 240 })
        decoders.set(from.id, decoder)
      }
      decoder.decode(new EncodedVideoChunk({ type: isKeyframe ? 'key' : 'delta', timestamp: 0, data: frame }))
    },
    { track: 'camera' },
  )
  const stopLeaves = call.onLeave((member) => {
    decoders.get(member.id)?.close()
    decoders.delete(member.id)
  })
  return () => {
    stopFrames()
    stopLeaves()
    for (const decoder of decoders.values()) decoder.close()
  }
}

function sendVideo(camera: MediaStream, me: Me) {
  const encoder = new VideoEncoder({
    output(chunk) {
      const frame = new Uint8Array(chunk.byteLength)
      chunk.copyTo(frame)
      const isKeyframe = chunk.type === 'key'
      // Live video: not awaited, or each frame waits a round trip for the previous one
      void me.publishBinary(frame, { track: 'camera', meta: { isKeyframe }, retain: isKeyframe })
    },
    error(err) {
      console.error('encode error:', err)
    },
  })
  encoder.configure({ codec: 'vp8', width: 320, height: 240, bitrate: 500_000, framerate: 15 })

  const frames = new MediaStreamTrackProcessor({ track: camera.getVideoTracks()[0]! }).readable.getReader()
  void (async () => {
    // Ends when the camera stops
    for (let i = 0; ; i++) {
      const { done, value: frame } = await frames.read()
      if (done) return
      if (encoder.state === 'configured') encoder.encode(frame, { keyFrame: i % 30 === 0 })
      frame.close()
    }
  })()
  return () => encoder.close()
}
