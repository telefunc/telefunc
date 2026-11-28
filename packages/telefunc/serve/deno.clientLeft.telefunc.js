// The telefunctions of serve/deno.clientLeft.spec.ts, which its Deno server (deno.clientLeft.server.fixture.mjs) loads
// as an app's. The server reads what each call saw in its own process (`seen`).

import { Channel } from '../wire-protocol/channel/server/ServerChannel.js'

/** What each row's call saw: how its File's read ended, and how its channel closed. */
const rows = new Map()

/** Keeps its File and returns a Channel; reads the File `readAfterMs` after the call, or never where that is null. */
export async function keepsFile(file, row, readAfterMs) {
  const kept = record(row, readAfterMs === null ? 'not read' : 'pending', 'open')
  const channel = new Channel()
  channel.onClose((err) => kept.set({ closed: `closed: ${err?.message ?? 'no error'}`, closedAt: performance.now() }))
  if (readAfterMs !== null) setTimeout(() => readInto(kept, file), readAfterMs)
  return channel
}

/** How `row`'s File read and channel ended: once neither is under way any more, or as they are `withinMs` from now. */
export async function seen(row, withinMs, uploadEndedAt) {
  const kept = rows.get(row)
  if (kept === undefined) return { read: 'no such row', closed: 'no such row', closedBeforeUploadEnd: false }
  let deadline
  await Promise.race([kept.settled, new Promise((resolve) => (deadline = setTimeout(resolve, withinMs)))])
  clearTimeout(deadline)
  const { read, closed, closedAt } = kept.state
  return { read, closed, closedBeforeUploadEnd: closedAt !== null && closedAt < uploadEndedAt }
}

/** Reads `file`, and keeps in `kept` how its read ended. */
function readInto(kept, file) {
  file.arrayBuffer().then(
    (bytes) => kept.set({ read: `read ${bytes.byteLength} bytes` }),
    (err) => kept.set({ read: `failed: ${err?.message ?? err}` }),
  )
}

/** `row`'s record, from its read's and its channel's start; `settled` resolves once neither is under way. */
function record(row, read, closed) {
  let settle
  const kept = {
    state: { read, closed, closedAt: null },
    settled: new Promise((resolve) => (settle = resolve)),
    set(change) {
      Object.assign(kept.state, change)
      if (kept.state.read !== 'pending' && kept.state.closed !== 'open') settle()
    },
  }
  rows.set(row, kept)
  return kept
}
