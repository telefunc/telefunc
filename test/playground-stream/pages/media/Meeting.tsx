export { Meeting }
export type { MeetingResult }

import React, { useEffect, useRef, useState } from 'react'
import { play, publish, type Playback, type PlaybackStats } from '@telefunc/media'
import type { RemoteParticipant } from '@telefunc/room'
import { onJoinMeeting } from './Media.telefunc'

type Frames = { checked: number; broken: number; jumps: number; last: number }
/** What this page shows of each member it plays, by name. */
/** Whether the tone was heard; else what was loudest when last listened to, and the listening context's state, to tell
 *  why not. */
type Tone = { heard: boolean; peakHz: number; peakDb: number; context: AudioContextState | null }
type MeetingResult = Record<string, { stats: PlaybackStats; frames: Frames; tone: Tone }>

/** `?room=…&name=…&height=…`: joins room `room` as `name`, publishes a test picture and tone, and plays every other
 *  member in a `<video>` `height` px tall, reading what each shown frame says. */
function Meeting() {
  const [hydrated, setHydrated] = useState(false)
  const tiles = useRef<HTMLDivElement>(null)
  useEffect(() => setHydrated(true), [])

  async function join() {
    const params = new URLSearchParams(location.search)
    const { room, me } = await onJoinMeeting(params.get('room')!, params.get('name')!)
    publish(me, testSource())
    const shown: Record<string, { playback: Playback; frames: Frames; tone: Tone }> = {}
    ;(window as unknown as { meetingResult(): MeetingResult }).meetingResult = () =>
      Object.fromEntries(
        Object.entries(shown).map(([name, { playback, frames, tone }]) => [
          name,
          { stats: playback.stats(), frames, tone },
        ]),
      )
    const show = (member: RemoteParticipant<{ name: string }>) => {
      if (member.id === me.id) return
      const video = document.createElement('video')
      video.style.height = `${params.get('height') ?? 360}px`
      video.autoplay = true
      video.playsInline = true
      tiles.current!.append(video)
      shown[member.meta.name] = { playback: play(member, video), frames: readFrames(video), tone: hearTone(video) }
    }
    for (const member of await room.getParticipants()) show(member)
    room.onJoin(show)
  }

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <button id="media-join" type="button" onClick={join}>
        Join
      </button>
      <div ref={tiles} />
    </div>
  )
}

/** A 640×360 picture at 30 frames a second, whose top-left 240 px say its frame's number (8 bits, then their
 *  complement, in 60 px squares), with shapes moving beside them; and a 440 Hz tone. */
function testSource(): MediaStream {
  const canvas = document.createElement('canvas')
  canvas.width = 640
  canvas.height = 360
  const context = canvas.getContext('2d')!
  let n = 0
  setInterval(() => {
    n++
    const gradient = context.createLinearGradient(0, 0, 640, 360)
    gradient.addColorStop(0, '#234')
    gradient.addColorStop(1, '#876')
    context.fillStyle = gradient
    context.fillRect(0, 0, 640, 360)
    for (let i = 0; i < 20; i++) {
      context.fillStyle = `hsl(${(i * 37 + n * 3) % 360} 70% 50%)`
      context.fillRect(260 + ((i * 53 + n * (1 + (i % 5))) % 340), (i * 29 + n) % 330, 30, 24)
    }
    const value = (n & 0xff) | ((~n & 0xff) << 8)
    for (let bit = 0; bit < 16; bit++) {
      context.fillStyle = (value >> bit) & 1 ? '#fff' : '#000'
      context.fillRect((bit % 4) * 60, Math.floor(bit / 4) * 60, 60, 60)
    }
  }, 1000 / 30)
  const audio = new AudioContext()
  const tone = new OscillatorNode(audio, { frequency: 440 })
  const out = audio.createMediaStreamDestination()
  tone.connect(new GainNode(audio, { gain: 0.3 })).connect(out)
  tone.start()
  return new MediaStream([...canvas.captureStream(30).getVideoTracks(), ...out.stream.getAudioTracks()])
}

/** Reads each frame `video` shows: its number, and whether its complement agrees, as a frame decoded without the one
 *  before it smears what changed between them. */
function readFrames(video: HTMLVideoElement): Frames {
  const frames: Frames = { checked: 0, broken: 0, jumps: 0, last: -1 }
  const canvas = document.createElement('canvas')
  const context = canvas.getContext('2d', { willReadFrequently: true })!
  const next = () =>
    video.requestVideoFrameCallback(() => {
      const { videoWidth: width, videoHeight: height } = video
      canvas.width = width
      canvas.height = height
      context.drawImage(video, 0, 0)
      const square = height / 6
      let value = 0
      for (let bit = 0; bit < 16; bit++) {
        const [r, g, b] = context.getImageData(
          ((bit % 4) + 0.5) * square,
          (Math.floor(bit / 4) + 0.5) * square,
          1,
          1,
        ).data
        if (r! + g! + b! > 382) value |= 1 << bit
      }
      const number = value & 0xff
      frames.checked++
      if ((number ^ (value >> 8)) !== 0xff) frames.broken++
      else {
        if (frames.last >= 0 && ((number - frames.last) & 0xff) > 1) frames.jumps++
        frames.last = number
      }
      next()
    })
  next()
  return frames
}

/** Whether `video` plays the 440 Hz tone. */
function hearTone(video: HTMLVideoElement): Tone {
  const tone: Tone = { heard: false, peakHz: 0, peakDb: Number.NEGATIVE_INFINITY, context: null }
  const audio = new AudioContext()
  const listen = () => {
    tone.context = audio.state
    const track = (video.srcObject as MediaStream | null)?.getAudioTracks()[0]
    if (!track) return setTimeout(listen, 200)
    const analyser = new AnalyserNode(audio, { fftSize: 2048 })
    audio.createMediaStreamSource(new MediaStream([track])).connect(analyser)
    const bins = new Float32Array(analyser.frequencyBinCount)
    const at440 = Math.round((440 * analyser.fftSize) / audio.sampleRate)
    const poll = setInterval(() => {
      tone.context = audio.state
      analyser.getFloatFrequencyData(bins)
      const peak = bins.indexOf(Math.max(...bins))
      tone.peakHz = Math.round((peak * audio.sampleRate) / analyser.fftSize)
      tone.peakDb = bins[peak]!
      if (Math.abs(peak - at440) <= 1 && bins[peak]! > -60) {
        tone.heard = true
        clearInterval(poll)
      }
    }, 100)
  }
  listen()
  return tone
}
