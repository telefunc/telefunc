export { VideoChat }

import React, { useCallback, useEffect, useRef, useState } from 'react'
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

  const join = useCallback(async () => {
    const stream = await navigator.mediaDevices.getUserMedia({ video: { width: 320, height: 240, frameRate: 15 } })
    videoRef.current!.srcObject = stream
    videoRef.current!.play()

    const channel = await onJoinRoom('demo')

    // Receive: decode VP8 → draw to canvas (with 300ms jitter buffer)
    const decoder = createDecoder(remoteRef.current!)
    const jitter = createJitterBuffer(decoder, 300, 15)
    channel.subscribeBinary(jitter.push)

    // Send: camera → VP8 encode → publish
    const stop = captureAndPublish(stream, channel)

    cleanupRef.current = () => {
      stop()
      jitter.stop()
      decoder.close()
      stream.getTracks().forEach((t) => t.stop())
      channel.close()
    }
    setJoined(true)
  }, [])

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

function createDecoder(canvas: HTMLCanvasElement) {
  const ctx = canvas.getContext('2d')!
  const decoder = new VideoDecoder({
    output(frame) {
      ctx.drawImage(frame, 0, 0, 320, 240)
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
  const queue: Uint8Array[] = []
  let timer: ReturnType<typeof setInterval> | null = null
  let started = false
  const drain = () => {
    const f = queue.shift()
    if (f) decodeFrame(decoder, f)
  }
  return {
    push(data: Uint8Array) {
      queue.push(data)
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

function decodeFrame(decoder: VideoDecoder, data: Uint8Array) {
  decoder.decode(new EncodedVideoChunk({ type: data[0] === 1 ? 'key' : 'delta', timestamp: 0, data: data.subarray(1) }))
}

function captureAndPublish(stream: MediaStream, channel: { publishBinary(data: Uint8Array): any }) {
  const encoder = new VideoEncoder({
    output(chunk) {
      const buf = new Uint8Array(1 + chunk.byteLength)
      buf[0] = chunk.type === 'key' ? 1 : 0
      chunk.copyTo(buf.subarray(1))
      channel.publishBinary(buf)
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
