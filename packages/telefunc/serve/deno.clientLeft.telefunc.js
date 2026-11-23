// The telefunctions of serve/deno.clientLeft.spec.ts, which its Deno server (deno.clientLeft.server.fixture.mjs) loads
// as an app's: a call that keeps its File and returns a Channel, and a call that says how that File's read and that
// channel ended.

import { Channel } from '../wire-protocol/channel/server/ServerChannel.js'

/** What each row's call kept: how its File's read ended, and when its channel closed and why. */
const rows = new Map()

/** Keeps its File and returns a Channel; reads the File `readAfterMs` after the call, or never where that is null. */
export async function keepsFile(file, row, readAfterMs) {
  const start = performance.now()
  const kept = { read: readAfterMs === null ? 'not read' : 'pending', closed: 'open' }
  rows.set(row, kept)
  const channel = new Channel()
  channel.onClose((err) => {
    kept.closed = `closed at ${Math.round(performance.now() - start)} ms: ${err?.message ?? 'no error'}`
  })
  if (readAfterMs !== null)
    setTimeout(() => {
      file.arrayBuffer().then(
        (bytes) => void (kept.read = `read ${bytes.byteLength} bytes`),
        (err) => void (kept.read = `failed: ${err?.message ?? err}`),
      )
    }, readAfterMs)
  return channel
}

/** How the File and the channel of `row`'s call ended so far. */
export async function rowState(row) {
  return rows.get(row) ?? 'no such row'
}
