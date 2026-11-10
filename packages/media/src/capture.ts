export { readFrames, readAudio }

/** Chrome's: a track's frames as a stream (mediacapture-transform). */
type TrackProcessor<T> = { readonly readable: ReadableStream<T> }
const TrackProcessor = (
  globalThis as { MediaStreamTrackProcessor?: new (init: { track: MediaStreamTrack }) => TrackProcessor<never> }
).MediaStreamTrackProcessor

/** Calls `onFrame` with each frame of video `track`, which it closes, until the function it returns is called. */
function readFrames(track: MediaStreamTrack, onFrame: (frame: VideoFrame) => void): () => void {
  if (TrackProcessor) return readProcessed<VideoFrame>(track, onFrame)
  // Where the page has no processor, a `<video>` plays the track and hands each frame it shows.
  const video = document.createElement('video')
  video.muted = true
  video.playsInline = true
  video.srcObject = new MediaStream([track])
  let stopped = false
  const next = (): void => {
    video.requestVideoFrameCallback((_now, { mediaTime }) => {
      if (stopped) return
      onFrame(new VideoFrame(video, { timestamp: Math.round(mediaTime * 1e6) }))
      next()
    })
  }
  void video.play().catch(() => {})
  next()
  return () => {
    stopped = true
    video.srcObject = null
  }
}

/** Calls `onData` with the sound of audio `track`, which it closes, until the function it returns is called. */
function readAudio(track: MediaStreamTrack, context: AudioContext, onData: (data: AudioData) => void): () => void {
  if (TrackProcessor) return readProcessed<AudioData>(track, onData)
  // Where the page has no processor, a worklet hands what the track plays.
  const source = context.createMediaStreamSource(new MediaStream([track]))
  let node: AudioWorkletNode | null = null
  let stopped = false
  void captureWorklet(context).then(() => {
    if (stopped) return
    node = new AudioWorkletNode(context, CAPTURE_PROCESSOR)
    let timestamp = 0
    node.port.onmessage = ({ data: channels }: MessageEvent<Float32Array[]>) => {
      const numberOfFrames = channels[0]!.length
      const data = new Float32Array(numberOfFrames * channels.length)
      for (const [i, channel] of channels.entries()) data.set(channel, i * numberOfFrames)
      onData(
        new AudioData({
          format: 'f32-planar',
          sampleRate: context.sampleRate,
          numberOfFrames,
          numberOfChannels: channels.length,
          timestamp,
          data,
        }),
      )
      timestamp += Math.round((numberOfFrames * 1e6) / context.sampleRate)
    }
    source.connect(node)
  })
  return () => {
    stopped = true
    source.disconnect()
    node?.port.close()
  }
}

function readProcessed<T extends VideoFrame | AudioData>(track: MediaStreamTrack, onFrame: (frame: T) => void) {
  const reader = (new TrackProcessor!({ track }) as TrackProcessor<T>).readable.getReader()
  void readAll(reader, onFrame)
  return () => void reader.cancel().catch(() => {})
}

async function readAll<T>(reader: ReadableStreamDefaultReader<T>, onFrame: (frame: T) => void): Promise<void> {
  for (;;) {
    const { done, value } = await reader.read().catch(() => ({ done: true, value: undefined }))
    if (done) return
    onFrame(value!)
  }
}

const CAPTURE_PROCESSOR = 'telefunc-media-capture'
/** The render quantum is 128 frames; posting each would wake the page 375 times a second, so it posts 10 ms at once. */
const CAPTURE_WORKLET = `registerProcessor('${CAPTURE_PROCESSOR}', class extends AudioWorkletProcessor {
  constructor() { super(); this.chunks = []; this.frames = 0 }
  process([input]) {
    if (input.length === 0) return true
    this.chunks.push(input.map((channel) => channel.slice()))
    this.frames += input[0].length
    if (this.frames >= sampleRate / 100) {
      const channels = input.map((_, c) => {
        const out = new Float32Array(this.frames)
        let at = 0
        for (const chunk of this.chunks) { out.set(chunk[c], at); at += chunk[c].length }
        return out
      })
      this.port.postMessage(channels, channels.map((channel) => channel.buffer))
      this.chunks = []
      this.frames = 0
    }
    return true
  }
})`
const workletsLoaded = new WeakMap<AudioContext, Promise<void>>()

function captureWorklet(context: AudioContext): Promise<void> {
  let loaded = workletsLoaded.get(context)
  if (loaded === undefined) {
    const url = URL.createObjectURL(new Blob([CAPTURE_WORKLET], { type: 'text/javascript' }))
    loaded = context.audioWorklet.addModule(url).finally(() => URL.revokeObjectURL(url))
    workletsLoaded.set(context, loaded)
  }
  return loaded
}
