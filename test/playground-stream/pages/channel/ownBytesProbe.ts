export { sendOwnBytes, listenOwnBytesAlone, listenOwnBytesBeside, OWN_BYTES_FRAMES }

type BytesChannel = {
  sendBinary(data: Uint8Array): unknown
  listenBinary(listener: (data: Uint8Array) => void): unknown
}

const SMALL_FRAMES = 16
const OWN_BYTES_FRAMES = 2 * (SMALL_FRAMES + 1)
const FIRST_MARKER = 0xa0

/** Twice, as the second round has a measured rate to gather by: small frames in one task, which share a message or a
 *  socket read, then a frame that goes alone. */
async function sendOwnBytes(channel: BytesChannel) {
  for (let round = 0; round < 2; round++) {
    for (let i = 0; i < SMALL_FRAMES; i++) void channel.sendBinary(new Uint8Array(32).fill(FIRST_MARKER + i))
    await channel.sendBinary(new Uint8Array(64 * 1024).fill(FIRST_MARKER + SMALL_FRAMES))
    await new Promise((resolve) => setTimeout(resolve, 200))
  }
}

/** Whether `data` is whole, and all its buffer holds of any frame's bytes is its own. */
function isOwn(data: Uint8Array): boolean {
  const marker = data[0]!
  const isMarker = (byte: number) => byte >= FIRST_MARKER && byte <= FIRST_MARKER + SMALL_FRAMES
  return (
    isMarker(marker) &&
    data.every((byte) => byte === marker) &&
    new Uint8Array(data.buffer).every((byte) => byte === marker || !isMarker(byte))
  )
}

const overwriteAll = (data: Uint8Array) => new Uint8Array(data.buffer).fill(0)
const transfer = (data: Uint8Array) => structuredClone(data, { transfer: [data.buffer as ArrayBuffer] })

/** A sole listener that overwrites its whole buffer and transfers every other one. `done` gets how many weren't its own. */
function listenOwnBytesAlone(channel: BytesChannel, done: (notOwn: number) => void) {
  let got = 0
  let notOwn = 0
  channel.listenBinary((data) => {
    if (!isOwn(data)) notOwn++
    overwriteAll(data)
    if (++got % 2) transfer(data)
    if (got === OWN_BYTES_FRAMES) done(notOwn)
  })
}

/** Three listeners: the first keeps its bytes, the second transfers its buffer, the last overwrites its whole buffer. */
function listenOwnBytesBeside(channel: BytesChannel, done: (notOwn: number) => void) {
  let got = 0
  let notOwn = 0
  let kept: Uint8Array
  channel.listenBinary((data) => {
    kept = data
  })
  channel.listenBinary((data) => {
    if (!isOwn(data)) notOwn++
    transfer(data)
  })
  channel.listenBinary((data) => {
    if (!isOwn(data)) notOwn++
    overwriteAll(data)
    if (!isOwn(kept)) notOwn++
    if (++got === OWN_BYTES_FRAMES) done(notOwn)
  })
}
