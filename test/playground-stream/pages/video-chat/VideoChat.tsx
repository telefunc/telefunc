export { VideoChat }

import React, { useCallback, useEffect, useRef, useState } from 'react'
import type { Room, LocalParticipant, RemoteParticipant } from 'telefunc/client'
import { onJoinRoom } from './VideoChat.telefunc'

declare const MediaStreamTrackProcessor: {
  new (opts: { track: MediaStreamTrack }): { readable: ReadableStream<VideoFrame> }
}

function VideoChat() {
  return (
    <div style={{ display: 'flex', height: '100vh' }}>
      <Participant name="Alice" />
      <div style={{ width: 1, background: '#e5e7eb' }} />
      <Participant name="Bob" />
    </div>
  )
}

function Participant({ name }: { name: string }) {
  const [joined, setJoined] = useState(false)
  const videoRef = useRef<HTMLVideoElement>(null)
  const remoteRef = useRef<HTMLCanvasElement>(null)
  const cleanupRef = useRef<(() => void) | null>(null)
  // Held for the page's life: a room dropped while `me` is kept gets closed by the GC.
  const roomRef = useRef<Room | null>(null)
  const meRef = useRef<LocalParticipant | null>(null)

  const join = useCallback(async () => {
    const stream = await navigator.mediaDevices.getUserMedia({ video: { width: 320, height: 240, frameRate: 15 } })
    videoRef.current!.srcObject = stream
    videoRef.current!.play()

    const room = (roomRef.current = await onJoinRoom('demo'))
    const me = (meRef.current = await room.join({ meta: { name } }))

    // Receive: one decoder per other member; the canvas shows the first of them
    const remote = createRemoteView(remoteRef.current!)
    const watch = (member: RemoteParticipant) => {
      if (member.id !== me.id) remote.add(member)
    }
    const unwatch = room.onJoin(watch)
    for (const member of await room.getParticipants()) watch(member)

    // Send: camera → VP8 encode → publish
    const stop = captureAndPublish(stream, me)

    cleanupRef.current = () => {
      stop()
      unwatch()
      remote.close()
      stream.getTracks().forEach((t) => t.stop())
      void me.leave()
    }
    setJoined(true)
  }, [name])

  useEffect(() => () => cleanupRef.current?.(), [])

  return (
    <div style={{ flex: 1, padding: 20, display: 'flex', flexDirection: 'column', gap: 12 }}>
      <h2 style={{ margin: 0, fontSize: 16, fontWeight: 600 }}>{name}</h2>
      <div style={{ display: 'flex', gap: 12 }}>
        <div>
          <div style={{ fontSize: 11, color: '#9ca3af', marginBottom: 4 }}>Local</div>
          <video
            ref={videoRef}
            width={320}
            height={240}
            muted
            playsInline
            style={{ background: '#111', borderRadius: 6, display: 'block' }}
          />
        </div>
        <div>
          <div style={{ fontSize: 11, color: '#9ca3af', marginBottom: 4 }}>Remote</div>
          <canvas
            ref={remoteRef}
            width={320}
            height={240}
            style={{ background: '#111', borderRadius: 6, display: 'block' }}
          />
        </div>
      </div>
      {!joined ? (
        <button onClick={join} style={{ alignSelf: 'flex-start', padding: '8px 20px' }}>
          Join as {name}
        </button>
      ) : (
        <button
          onClick={() => {
            cleanupRef.current?.()
            cleanupRef.current = null
            setJoined(false)
          }}
          style={{ alignSelf: 'flex-start', padding: '8px 20px', color: '#dc2626' }}
        >
          Leave
        </button>
      )}
    </div>
  )
}

// --- Helpers (could be a library) ---

function createRemoteView(canvas: HTMLCanvasElement) {
  const ctx = canvas.getContext('2d')!
  const decoders = new Map<
    string,
    { decoder: VideoDecoder; jitter: ReturnType<typeof createJitterBuffer>; unsubscribe: () => void }
  >()
  const isShown = (id: string) => decoders.keys().next().value === id
  const remove = (id: string) => {
    const entry = decoders.get(id)
    if (!entry) return
    decoders.delete(id)
    entry.unsubscribe()
    entry.jitter.stop()
    entry.decoder.close()
  }
  return {
    add(member: RemoteParticipant) {
      if (decoders.has(member.id)) return
      const decoder = createDecoder((frame) => {
        if (isShown(member.id)) ctx.drawImage(frame, 0, 0, 320, 240)
      })
      const jitter = createJitterBuffer(decoder, 300, 15)
      const unsubscribe = member.subscribeBinary((frame, info) => jitter.push(frame, info.meta?.isKeyframe === true), {
        track: 'camera',
      })
      decoders.set(member.id, { decoder, jitter, unsubscribe })
      member.onLeave(() => remove(member.id))
    },
    close() {
      for (const id of [...decoders.keys()]) remove(id)
    },
  }
}

function createDecoder(draw: (frame: VideoFrame) => void) {
  const decoder = new VideoDecoder({
    output(frame) {
      draw(frame)
      frame.close()
    },
    error(err) {
      console.error('decode error:', err)
    },
  })
  decoder.configure({ codec: 'vp8', codedWidth: 320, codedHeight: 240 })
  return decoder
}

function createJitterBuffer(decoder: VideoDecoder, bufferMs: number, fps: number) {
  const queue: Array<{ data: Uint8Array; isKeyframe: boolean }> = []
  let timer: ReturnType<typeof setInterval> | null = null
  let started = false
  const drain = () => {
    const f = queue.shift()
    if (f) decoder.decode(new EncodedVideoChunk({ type: f.isKeyframe ? 'key' : 'delta', timestamp: 0, data: f.data }))
  }
  return {
    push(data: Uint8Array, isKeyframe: boolean) {
      queue.push({ data, isKeyframe })
      if (!started) {
        started = true
        setTimeout(() => {
          timer = setInterval(drain, 1000 / fps)
        }, bufferMs)
      }
    },
    stop() {
      if (timer) clearInterval(timer)
    },
  }
}

function captureAndPublish(stream: MediaStream, me: LocalParticipant) {
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

  const processor = new MediaStreamTrackProcessor({ track: stream.getVideoTracks()[0]! })
  const reader = processor.readable.getReader()
  const abort = new AbortController()
  let i = 0

  void (async () => {
    while (!abort.signal.aborted) {
      const { done, value: frame } = await reader.read()
      if (done || abort.signal.aborted) {
        frame?.close()
        break
      }
      encoder.encode(frame, { keyFrame: i++ % 30 === 0 })
      frame.close()
    }
    reader.cancel()
  })()

  return () => {
    abort.abort()
    encoder.close()
  }
}
