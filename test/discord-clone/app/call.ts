export { joinCall, leaveCall, toggleMute, toggleCamera, toggleScreenShare, registerTile, getSelfStream }

// Voice & video calls, on one Room per voice channel.
//
// Round 2 of the stress test: media multiplexing is first-class now — mic/camera/screen ride
// *named binary tracks* (`publishBinary(data, { track, keyFrame })`, `subscribeBinary(cb,
// { track })`, `info.keyFrame`), which deleted this app's hand-rolled `[kind][flags]` frame
// envelope and demux switch (README finding 6, fixed upstream).
//
// Round 3: tracks are source-selective — the publish ack's `receivers` reports the track's live
// subscription count, so an unwatched stream pauses its encoder and probes for returning
// viewers (`receiverGate` below). Alone in a voice channel, nothing is encoded or uploaded.
//
// The Room-relevant shape:
// - The voice join is a telefunction (`onJoinVoice`): the membership carries my trusted
//   `identity`, and the room's `onJoin` guard enforces capacity server-side (finding 11).
// - `selfDelivery: false` at join — my own media must not come back to me.
// - Mute / camera / screen state rides the member's *metadata* (`setMeta`), so it reaches
//   every observer — including late joiners, who get it with the roster.
// - Decoder lifecycle is presence: `room.onJoin` attaches a peer, `member.onLeave` tears it
//   down (a member's listeners are released with it).

import type { LocalParticipant, RemoteParticipant } from 'telefunc'
import type { MemberMeta } from '../shared/types'
import { onJoinVoice } from '../telefunc/channels.telefunc'
import { getChannelRoom, getIdentity, patchCall, showToast } from './store'

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

type Session = {
  channelId: string
  membership: LocalParticipant<MemberMeta>
  muted: boolean
  micAvailable: boolean
  camera: CaptureHandle | null
  screen: CaptureHandle | null
  playback: AudioContext
  peers: Map<string, Peer>
  cleanups: Array<() => void>
}

type CaptureHandle = { stream: MediaStream; stop(): void }

let session: Session | null = null

async function joinCall(channelId: string): Promise<void> {
  const room = getChannelRoom(channelId)
  if (room === undefined || session?.channelId === channelId) return
  await leaveCall()

  let membership: LocalParticipant<MemberMeta>
  try {
    // Server-side join: trusted identity + the voice room's `onJoin` capacity guard — a full
    // channel rejects here, on the server (this used to be a client-side `isFull` check that
    // nothing enforced).
    membership = await onJoinVoice(channelId)
  } catch (err) {
    showToast(errorMessage(err))
    return
  }

  const current: Session = {
    channelId,
    membership,
    muted: false,
    micAvailable: true,
    camera: null,
    screen: null,
    playback: new AudioContext({ sampleRate: OPUS.sampleRate }), // created on the click — autoplay-safe
    peers: new Map(),
    cleanups: [],
  }
  session = current
  current.cleanups.push(() => void current.playback.close().catch(() => {}))

  try {
    // Microphone. No microphone (or permission denied) is a first-class state: join
    // listen-only, muted, and say so — the rest of the call works as usual.
    try {
      const mic = await navigator.mediaDevices.getUserMedia({
        audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
      })
      const micHandle = captureHandle(mic)
      current.cleanups.push(micHandle.stop)
      // Every Opus frame decodes on its own, so the paused-stream probe needs no keyframe
      // handling — any frame that gets through doubles as the probe.
      const gate = receiverGate(1000)
      const encoder = encodeOpus(resampledTrack(mic, current.cleanups), (frame) => {
        if (session !== current || current.muted || gate.skip()) return
        void current.membership.publishBinary(frame, { track: 'mic' }).then(gate.onAck, () => {})
      })
      current.cleanups.push(encoder.stop)
    } catch {
      current.micAvailable = false
      current.muted = true
      await current.membership.setMeta({ ...current.membership.meta, muted: true })
      showToast('Microphone unavailable — joined listen-only')
    }

    // Peers: decode every other member's tracks; lifecycle follows presence.
    const attachPeer = (member: RemoteParticipant<MemberMeta>) => {
      if (member.id === current.membership.id || current.peers.has(member.id)) return
      const peer = createPeer(member)
      current.peers.set(member.id, peer)
      member.onLeave(() => {
        peer.close()
        current.peers.delete(member.id)
      })
    }
    void room.getParticipants().then((members) => {
      if (session === current) members.forEach(attachPeer)
    })
    current.cleanups.push(room.onJoin(attachPeer))

    current.cleanups.push(room.onClose(() => void leaveCall())) // channel deleted under us

    syncCallState(current)
  } catch (err) {
    await leaveCall()
    showToast(`Couldn't join voice: ${errorMessage(err)}`)
  }

  function createPeer(member: RemoteParticipant<MemberMeta>): Peer {
    const audio = playOpus(current.playback)
    const camera = decodeVp8((frame) => drawToTile(member.id, 'camera', frame))
    const screen = decodeVp8((frame) => drawToTile(member.id, 'screen', frame))

    // Named tracks: subscribing declares exactly which substreams flow to me — the server
    // relays per (member, track). No frame envelope, no demux switch.
    const unsubscribes = [
      member.subscribeBinary((data) => audio.push(data), { track: 'mic' }),
      member.subscribeBinary((data, info) => camera.push(data, info.keyFrame), { track: 'camera' }),
      member.subscribeBinary((data, info) => screen.push(data, info.keyFrame), { track: 'screen' }),
    ]

    // A member turning a stream off should blank its decoder (fresh keyframe wait on re-on).
    const unwatch = member.onUpdate((meta) => {
      if (meta.camera !== true) camera.reset()
      if (meta.screen !== true) screen.reset()
    })

    return {
      close() {
        for (const unsubscribe of unsubscribes) unsubscribe()
        unwatch()
        audio.close()
        camera.close()
        screen.close()
      },
    }
  }
}

async function leaveCall(): Promise<void> {
  if (session === null) return
  const ending = session
  session = null
  ending.camera?.stop()
  ending.screen?.stop()
  for (const peer of ending.peers.values()) peer.close()
  ending.peers.clear()
  for (const cleanup of ending.cleanups) cleanup()
  patchCall(null)
  await ending.membership.leave().catch(() => {})
}

async function toggleMute(): Promise<void> {
  if (session === null || !session.micAvailable) return
  const current = session
  current.muted = !current.muted
  syncCallState(current)
  // Metadata is a full replace — spread the rest or lose it (README finding 5).
  await current.membership.setMeta({ ...current.membership.meta, muted: current.muted })
}

async function toggleCamera(): Promise<void> {
  if (session === null) return
  const current = session
  if (current.camera !== null) {
    current.camera.stop()
    current.camera = null
    syncCallState(current)
    await current.membership.setMeta({ ...current.membership.meta, camera: false })
    return
  }
  try {
    const stream = await navigator.mediaDevices.getUserMedia({
      video: { width: { ideal: 640 }, height: { ideal: 360 }, frameRate: { ideal: 15 } },
    })
    current.camera = videoPipeline(current, stream, 'camera', { keyframeEvery: 15, bitrate: 500_000 })
    syncCallState(current)
    await current.membership.setMeta({ ...current.membership.meta, camera: true })
  } catch {
    showToast('Camera unavailable')
  }
}

async function toggleScreenShare(): Promise<void> {
  if (session === null) return
  const current = session
  if (current.screen !== null) {
    current.screen.stop()
    current.screen = null
    syncCallState(current)
    await current.membership.setMeta({ ...current.membership.meta, screen: false })
    return
  }
  try {
    const stream = await navigator.mediaDevices.getDisplayMedia({ video: { frameRate: { ideal: 5 } } })
    current.screen = videoPipeline(current, stream, 'screen', { keyframeEvery: 5, bitrate: 1_200_000 })
    // The browser's own "stop sharing" UI ends the track behind our back — mirror it.
    stream.getVideoTracks()[0]?.addEventListener('ended', () => void toggleScreenShare())
    syncCallState(current)
    await current.membership.setMeta({ ...current.membership.meta, screen: true })
  } catch {
    showToast('Screen share unavailable')
  }
}

function syncCallState(current: Session): void {
  if (session !== current) return
  patchCall({
    channelId: current.channelId,
    myParticipantId: current.membership.id,
    muted: current.muted,
    micAvailable: current.micAvailable,
    cameraOn: current.camera !== null,
    screenOn: current.screen !== null,
  })
}

/** The local self-view: CallView renders my own camera/screen straight from the capture
 *  stream (with `selfDelivery: false` nothing of mine comes back over the wire). */
function getSelfStream(kind: 'camera' | 'screen'): MediaStream | null {
  if (session === null) return null
  return (kind === 'camera' ? session.camera : session.screen)?.stream ?? null
}

// ---------------------------------------------------------------------------
// Tiles (receive side)
// ---------------------------------------------------------------------------

// CallView registers a <canvas> per (participant, video track); decoders draw into whatever is
// currently registered. Tiles appear from metadata (camera/screen flags), so a canvas usually
// registers moments before the first keyframe arrives.
const tiles = new Map<string, HTMLCanvasElement>() // `${participantId}:${track}` → canvas

function registerTile(participantId: string, kind: 'camera' | 'screen', canvas: HTMLCanvasElement | null): void {
  const key = `${participantId}:${kind}`
  if (canvas === null) tiles.delete(key)
  else tiles.set(key, canvas)
}

function drawToTile(participantId: string, kind: 'camera' | 'screen', frame: VideoFrame): void {
  const canvas = tiles.get(`${participantId}:${kind}`)
  if (canvas) drawFrame(canvas, frame)
  frame.close()
}

type Peer = { close(): void }

// ---------------------------------------------------------------------------
// Media helpers (WebCodecs plumbing — nothing below touches the Room API)
// ---------------------------------------------------------------------------

const OPUS = { codec: 'opus', sampleRate: 48_000, numberOfChannels: 1 } as const

declare const MediaStreamTrackProcessor: {
  new (init: { track: MediaStreamTrack }): { readable: ReadableStream<AudioData | VideoFrame> }
}

function captureHandle(stream: MediaStream): CaptureHandle {
  return {
    stream,
    stop() {
      for (const track of stream.getTracks()) track.stop()
    },
  }
}

/** Opus wants 48kHz; microphones often run 44.1kHz — route through a 48kHz mono context. */
function resampledTrack(mic: MediaStream, cleanups: Array<() => void>): MediaStreamTrack {
  const ctx = new AudioContext({ sampleRate: OPUS.sampleRate })
  cleanups.push(() => void ctx.close().catch(() => {}))
  const destination = new MediaStreamAudioDestinationNode(ctx, { channelCount: 1 })
  ctx.createMediaStreamSource(mic).connect(destination)
  return destination.stream.getAudioTracks()[0]!
}

function encodeOpus(track: MediaStreamTrack, onFrame: (frame: Uint8Array) => void): { stop(): void } {
  const encoder = new AudioEncoder({
    output(chunk) {
      const bytes = new Uint8Array(chunk.byteLength)
      chunk.copyTo(bytes)
      onFrame(bytes)
    },
    error(err) {
      console.error('[discord:call] audio encode error', err)
    },
  })
  encoder.configure({ ...OPUS, bitrate: 32_000 })
  const stopReading = readFrames(track, (data) => {
    encoder.encode(data as AudioData)
    data.close()
  })
  return {
    stop() {
      stopReading()
      if (encoder.state !== 'closed') encoder.close()
    },
  }
}

function playOpus(ctx: AudioContext): { push(frame: Uint8Array): void; close(): void } {
  let playhead = 0
  let timestamp = 0
  const decoder = new AudioDecoder({
    output(audio) {
      const pcm = new Float32Array(audio.numberOfFrames)
      audio.copyTo(pcm, { planeIndex: 0, format: 'f32-planar' })
      audio.close()
      const buffer = ctx.createBuffer(1, pcm.length, OPUS.sampleRate)
      buffer.copyToChannel(pcm, 0)
      const source = new AudioBufferSourceNode(ctx, { buffer })
      source.connect(ctx.destination)
      playhead = Math.max(playhead, ctx.currentTime + 0.08) // small jitter cushion
      source.start(playhead)
      playhead += buffer.duration
    },
    error(err) {
      console.error('[discord:call] audio decode error', err)
    },
  })
  decoder.configure(OPUS)
  return {
    push(frame) {
      if (decoder.state === 'closed') return
      timestamp += 20_000 // 20ms Opus frames in µs — only monotonicity matters
      decoder.decode(new EncodedAudioChunk({ type: 'key', timestamp, data: frame }))
    },
    close() {
      if (decoder.state !== 'closed') decoder.close()
    },
  }
}

/** Camera/screen capture → VP8 on a named track. The encoder is (re)configured from the frames
 *  themselves, so any source size works — including a screen share resized mid-stream. */
function videoPipeline(
  current: Session,
  stream: MediaStream,
  track: 'camera' | 'screen',
  opts: { keyframeEvery: number; bitrate: number },
): CaptureHandle {
  const gate = receiverGate(2000)
  const encoder = new VideoEncoder({
    output(chunk) {
      if (session !== current) return
      const bytes = new Uint8Array(chunk.byteLength)
      chunk.copyTo(bytes)
      void current.membership.publishBinary(bytes, { track, keyFrame: chunk.type === 'key' }).then(gate.onAck, () => {})
    },
    error(err) {
      console.error('[discord:call] video encode error', err)
    },
  })
  let width = 0
  let height = 0
  let count = 0
  const stopReading = readFrames(stream.getVideoTracks()[0]!, (data) => {
    const frame = data as VideoFrame
    const probing = gate.paused // a probe must decode on its own — make it a keyframe
    if (gate.skip()) {
      frame.close()
      return
    }
    const w = frame.displayWidth - (frame.displayWidth % 2)
    const h = frame.displayHeight - (frame.displayHeight % 2)
    if (w !== width || h !== height) {
      width = w
      height = h
      count = 0
      encoder.configure({ codec: 'vp8', width, height, bitrate: opts.bitrate })
    }
    if (probing) count = 0
    encoder.encode(frame, { keyFrame: count++ % opts.keyframeEvery === 0 })
    frame.close()
  })
  return {
    stream,
    stop() {
      stopReading()
      if (encoder.state !== 'closed') encoder.close()
      for (const track of stream.getTracks()) track.stop()
    },
  }
}

function decodeVp8(draw: (frame: VideoFrame) => void): {
  push(payload: Uint8Array, isKeyframe: boolean): void
  reset(): void
  close(): void
} {
  const decoder = new VideoDecoder({
    output: draw,
    error(err) {
      console.error('[discord:call] video decode error', err)
    },
  })
  decoder.configure({ codec: 'vp8' })
  let timestamp = 0
  let awaitingKeyframe = true
  return {
    push(payload, isKeyframe) {
      if (decoder.state === 'closed') return
      if (awaitingKeyframe && !isKeyframe) return // can't start mid-stream — wait for a keyframe
      awaitingKeyframe = false
      timestamp += 33_000
      decoder.decode(new EncodedVideoChunk({ type: isKeyframe ? 'key' : 'delta', timestamp, data: payload }))
    },
    reset() {
      awaitingKeyframe = true
    },
    close() {
      if (decoder.state !== 'closed') decoder.close()
    },
  }
}

/** Pause-when-unwatched: publish acks carry the track's live `receivers` count. After an ack
 *  reports 0, frames are dropped at the source — except one probe every `probeMs`, whose ack
 *  detects returning receivers and reopens the stream. */
function receiverGate(probeMs: number): {
  readonly paused: boolean
  skip(): boolean
  onAck(ack: { receivers?: number }): void
} {
  let paused = false
  let lastProbeAt = 0
  return {
    get paused() {
      return paused
    },
    /** Drop this frame? While paused, everything but the periodic probe. */
    skip() {
      if (!paused) return false
      const now = Date.now()
      if (now - lastProbeAt < probeMs) return true
      lastProbeAt = now
      return false
    },
    onAck(ack) {
      if (typeof ack.receivers === 'number') paused = ack.receivers === 0
    },
  }
}

function drawFrame(canvas: HTMLCanvasElement, frame: VideoFrame): void {
  if (canvas.width !== frame.displayWidth || canvas.height !== frame.displayHeight) {
    canvas.width = frame.displayWidth
    canvas.height = frame.displayHeight
  }
  canvas.getContext('2d')?.drawImage(frame, 0, 0)
  if (canvas.dataset.live === undefined) canvas.dataset.live = '1' // "real frames arrived" marker
}

/** Pull frames off a track until stopped. */
function readFrames(track: MediaStreamTrack, onFrame: (data: AudioData | VideoFrame) => void): () => void {
  const reader = new MediaStreamTrackProcessor({ track }).readable.getReader()
  let stopped = false
  void (async () => {
    while (!stopped) {
      const { done, value } = await reader.read()
      if (done || stopped) {
        value?.close()
        break
      }
      onFrame(value)
    }
    void reader.cancel().catch(() => {})
  })()
  return () => {
    stopped = true
  }
}

function errorMessage(err: unknown): string {
  if (typeof err === 'object' && err !== null && 'abortValue' in err) {
    const value = (err as { abortValue: unknown }).abortValue
    if (typeof value === 'string') return value
  }
  return err instanceof Error ? err.message : String(err)
}
