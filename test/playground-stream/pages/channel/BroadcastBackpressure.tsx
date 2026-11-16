export {
  PublishesBehind,
  BroadcastOverflow,
  BroadcastBehind,
  LossyKeepsUp,
  LossyBeforePlain,
  LossyTicker,
  BroadcastBehindCloses,
  BroadcastAwayShared,
  initialBroadcastBackpressureState,
  type BroadcastBackpressureState,
}

import React from 'react'
import { onChannelPublishesBehind, onChannelLossyTicker } from './Channel.telefunc'
import { openChannel } from './openChannel'

type BroadcastBackpressureState = {
  publishesBehind: {
    pingMs: number
    publishesAhead: number
    closed: string
    received: number
    closeErr: string
    abortReceived: number
    abortErr: string
    abortMs: number
  } | null
  broadcastOverflow: { received: number; closeErr: string } | null
  broadcastBehind: {
    seqsAhead: number[]
    isClosed: boolean
    receiptAhead: number
    opening: { receipts: number; last: string }
    attached: { receipts: number; last: string }
    reconnected: { receipts: number; last: string }
  } | null
  lossyKeepsUp: { fromServer: number; fromPage: number; last: string } | null
  lossyBeforePlain: { received: unknown[]; seqsAhead: number[] } | null
  lossyTicker: { received: number; last: number | undefined; worstMs: number; closed: string | null } | null
  broadcastBehindCloses: {
    seqsAhead: number[]
    closeErr: string | null
    pastMemory: { seqsAhead: number[]; closeErr: string }
    across: { publish: string; received: number; closeErr: string }
  } | null
  broadcastAwayShared: { received: number[][]; closed: string[] } | null
}

const initialBroadcastBackpressureState: BroadcastBackpressureState = {
  publishesBehind: null,
  broadcastOverflow: null,
  broadcastBehind: null,
  lossyKeepsUp: null,
  lossyBeforePlain: null,
  lossyTicker: null,
  broadcastBehindCloses: null,
  broadcastAwayShared: null,
}

type Show = (state: Partial<BroadcastBackpressureState>) => void

function PublishesBehind({ show }: { show: Show }) {
  async function testPublishesBehind() {
    show({ publishesBehind: null })
    /** A broadcast of 4 MiB bodies, how many of them the page got, and once it closed, why. */
    const openBehind = async () => {
      const { broadcast, control } = await openChannel(onChannelPublishesBehind)
      const got = { firstSeq: 0, received: 0 }
      const waiting: { count: number; resolve: () => void }[] = []
      broadcast.subscribeBinary((_, info) => {
        got.firstSeq ||= info.seq
        got.received++
        for (const { count, resolve } of waiting) if (got.received === count) resolve()
      })
      const ended = new Promise<string>((resolve) => broadcast.onClose((err) => resolve(err?.name ?? 'none')))
      const received = (count: number) => new Promise<void>((resolve) => waiting.push({ count, resolve }))
      return { broadcast, control, got, ended, received }
    }

    const closing = await openBehind()
    void closing.control.send(1)
    // Its answer meets that body on the wire, with its head gone and no later head of this channel ahead.
    const { seq: ownSeq } = await closing.broadcast.publish('answered behind the body')
    void closing.control.send(4)
    await closing.received(2)
    const pinged = performance.now()
    await closing.control.send('ping', { ack: true })
    const pingMs = Math.round(performance.now() - pinged)
    const closed = await closing.control.send('close', { ack: true })
    const closeErr = await closing.ended
    const { received, firstSeq } = closing.got
    closing.control.close()

    const aborting = await openBehind()
    const abortAt = performance.now()
    await aborting.control.send('abort', { ack: true })
    const abortErr = await aborting.ended
    const abortMs = Math.round(performance.now() - abortAt)
    const abortReceived = aborting.got.received
    aborting.control.close()

    const publishesBehind = {
      pingMs,
      publishesAhead: ownSeq - firstSeq,
      closed,
      received,
      closeErr,
      abortReceived,
      abortErr,
      abortMs,
    }
    show({ publishesBehind })
  }

  return (
    <button
      id="channel-test-publishes-behind"
      type="button"
      onClick={testPublishesBehind}
      className="px-3 py-1.5 text-sm rounded bg-cyan-800 text-white hover:bg-cyan-900"
    >
      Publishes Falling Behind
    </button>
  )
}

function BroadcastOverflow({ show }: { show: Show }) {
  async function testBroadcastOverflow() {
    show({ broadcastOverflow: null })
    const { broadcast, control } = await openChannel(onChannelPublishesBehind)
    let received = 0
    broadcast.subscribeBinary(() => {
      received++
    })
    const closing = new Promise<string>((resolve) => broadcast.onClose((err) => resolve(err?.name ?? 'none')))
    void control.send(3)
    const closeErr = await closing
    show({ broadcastOverflow: { received, closeErr } })
    control.close()
  }

  return (
    <button
      id="channel-test-broadcast-overflow"
      type="button"
      onClick={testBroadcastOverflow}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Broadcast Publishes More Than The Server Can Hold
    </button>
  )
}

function BroadcastBehind({ show }: { show: Show }) {
  async function testBroadcastBehind() {
    show({ broadcastBehind: null })
    const { broadcast, control } = await openChannel(() => onChannelPublishesBehind(256 * 1024, true))
    // The page's own 20 publishes at once, each its ordinal at the same size, and the last the server's subscription got.
    const publishOrdinals = async (from: number) => {
      const receipts = await Promise.all(
        Array.from({ length: 20 }, (_, n) =>
          broadcast.publish(`${from + n}:`.padEnd(256 * 1024, 'x'), { lossy: true }),
        ),
      )
      return {
        receipts: new Set(receipts.map((receipt) => receipt.seq)).size,
        last: await control.send('last', { ack: true }),
      }
    }
    // Before the first attach no wire takes them: only the newest waits, and all 20 settle with its receipt.
    const opening = await publishOrdinals(0)
    // Attached, on a link that keeps up: each goes, with a receipt of its own.
    const attached = await publishOrdinals(200)
    const { seq: before } = await broadcast.publish('before the burst')
    const seqsAhead: number[] = []
    broadcast.subscribeBinary((_, info) => seqsAhead.push(info.seq - before))
    const reaching = (ahead: number) =>
      new Promise<void>((resolve) => broadcast.subscribeBinary((_, info) => info.seq === before + ahead && resolve()))
    const burst = reaching(16)
    void control.send(16)
    await burst
    // A publish outstanding across a disconnect, as the page falls behind: its receipt and the newest, 17 later, come.
    const newest = reaching(34)
    await control.send('away', { ack: true })
    const { seq } = await broadcast.publish('across the disconnect')
    await newest
    // While the page is reconnecting: only the newest of them waits.
    await control.send('cut', { ack: true })
    await new Promise((resolve) => setTimeout(resolve, 150))
    const reconnected = await publishOrdinals(100)
    const broadcastBehind = {
      seqsAhead,
      isClosed: broadcast.isClosed,
      receiptAhead: seq - before,
      opening,
      attached,
      reconnected,
    }
    show({ broadcastBehind })
    control.close()
  }

  return (
    <button
      id="channel-test-broadcast-behind"
      type="button"
      onClick={testBroadcastBehind}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Lossy Broadcast Publishes More Than The Client Keeps Up With
    </button>
  )
}

function LossyKeepsUp({ show }: { show: Show }) {
  async function testLossyKeepsUp() {
    show({ lossyKeepsUp: null })
    const { broadcast, control } = await openChannel(() => onChannelPublishesBehind(4096, true))
    let fromServer = 0
    broadcast.subscribeBinary(() => void fromServer++)
    await control.send(100, { ack: true })
    for (let i = 0; i < 30 && fromServer < 100; i++) await new Promise((resolve) => setTimeout(resolve, 100))
    const receipts = await Promise.all(
      Array.from({ length: 100 }, (_, n) => broadcast.publish(`${n}:`.padEnd(4096, 'x'), { lossy: true })),
    )
    const fromPage = new Set(receipts.map((receipt) => receipt.seq)).size
    const lossyKeepsUp = { fromServer, fromPage, last: await control.send('last', { ack: true }) }
    show({ lossyKeepsUp })
    control.close()
  }

  return (
    <button
      id="channel-test-lossy-keeps-up"
      type="button"
      onClick={testLossyKeepsUp}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Lossy Broadcast On A Link That Keeps Up
    </button>
  )
}

function LossyBeforePlain({ show }: { show: Show }) {
  async function testLossyBeforePlain() {
    show({ lossyBeforePlain: null })
    const { broadcast, control } = await openChannel(() => onChannelPublishesBehind(1024))
    const received: unknown[] = []
    const seqsAhead: number[] = []
    let firstSeq: number | undefined
    const newestArrived = new Promise<void>((resolve) =>
      broadcast.subscribe((message, info) => {
        firstSeq ??= info.seq
        received.push(message)
        seqsAhead.push(info.seq - firstSeq)
        if (message === 20) resolve()
      }),
    )
    // While its connection is down, ten lossy publishes, a plain one and ten more: of each ten, only the newest waits to
    // go, and the second ten can't take the place of the first.
    broadcast.onDisconnect(() => {
      for (let n = 1; n <= 10; n++) void broadcast.publish(n, { lossy: true })
      void broadcast.publish('plain')
      for (let n = 11; n <= 20; n++) void broadcast.publish(n, { lossy: true })
    })
    await control.send('cut', { ack: true })
    await newestArrived
    show({ lossyBeforePlain: { received, seqsAhead } })
    control.close()
  }

  return (
    <button
      id="channel-test-lossy-before-plain"
      type="button"
      onClick={testLossyBeforePlain}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Lossy Publishes, Then A Plain One, From A Page That Is Away
    </button>
  )
}

function LossyTicker({ show }: { show: Show }) {
  async function testLossyTicker(bytes: number, count: number, close: boolean) {
    show({ lossyTicker: null })
    const { broadcast, control } = await openChannel(() => onChannelLossyTicker(bytes, count, 1000 / 60, close))
    const received: number[] = []
    let worstMs = 0
    let closed: string | null = null
    broadcast.subscribe((message) => {
      const [ordinal, sentAt] = message.split(':')
      received.push(Number(ordinal))
      worstMs = Math.max(worstMs, Date.now() - Number(sentAt))
    })
    broadcast.onClose((err) => {
      closed = err?.name ?? 'none'
    })
    await control.send('start', { ack: true })
    const newest = Date.now() + 25_000
    const done = () => (close ? closed !== null : received.at(-1) === count - 1)
    while (!done() && Date.now() < newest) await new Promise((resolve) => setTimeout(resolve, 100))
    const lossyTicker = { received: received.length, last: received.at(-1), worstMs, closed }
    show({ lossyTicker })
    control.close()
  }

  return (
    <>
      <button
        id="channel-test-lossy-ticker"
        type="button"
        onClick={() => testLossyTicker(4096, 300, false)}
        className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
      >
        Lossy Broadcast Ticker
      </button>
      <button
        id="channel-test-lossy-ticker-small"
        type="button"
        onClick={() => testLossyTicker(100, 300, false)}
        className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
      >
        Lossy Broadcast Ticker Of Small Messages
      </button>
      <button
        id="channel-test-lossy-ticker-close"
        type="button"
        onClick={() => testLossyTicker(4096, 120, true)}
        className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
      >
        Lossy Broadcast Ticker Closed Gracefully
      </button>
    </>
  )
}

function BroadcastBehindCloses({ show }: { show: Show }) {
  async function testBroadcastBehindCloses() {
    show({ broadcastBehindCloses: null })
    const opened = () => openChannel(() => onChannelPublishesBehind(256 * 1024))
    const { broadcast, control } = await opened()
    const { seq: before } = await broadcast.publish('before the burst')
    const seqsAhead: number[] = []
    // It closes, or, where the publishes are lossy, reads on up to the newest.
    const ended = new Promise<string | null>((resolve) => {
      broadcast.onClose((err) => resolve(err?.name ?? 'none'))
      broadcast.subscribeBinary((_, info) => {
        seqsAhead.push(info.seq - before)
        if (info.seq === before + 16) resolve(null)
      })
    })
    void control.send(16)
    const closeErr = await ended
    control.close()
    // Three publishes, then one the server can't hold: its page gets the three, then the close.
    const behindMemory = await opened()
    const { seq: start } = await behindMemory.broadcast.publish('before the four')
    const pastMemory = { seqsAhead: [] as number[], closeErr: '' }
    behindMemory.broadcast.subscribeBinary((_, info) => pastMemory.seqsAhead.push(info.seq - start))
    const memoryClosed = new Promise<string>((resolve) =>
      behindMemory.broadcast.onClose((err) => resolve(err?.name ?? 'none')),
    )
    void behindMemory.control.send('past memory')
    pastMemory.closeErr = await memoryClosed
    behindMemory.control.close()
    // A publish outstanding across a disconnect, as the page falls behind: it settles with the close, and nothing that
    // follows it comes.
    const away = await opened()
    let received = 0
    away.broadcast.subscribeBinary(() => received++)
    const closed = new Promise<string>((resolve) => away.broadcast.onClose((err) => resolve(err?.name ?? 'none')))
    await away.control.send('away', { ack: true })
    const publish = await away.broadcast.publish('across the disconnect').then(
      () => 'resolved',
      (err: Error) => err.name,
    )
    const across = { publish, received, closeErr: await closed }
    show({ broadcastBehindCloses: { seqsAhead, closeErr, pastMemory, across } })
    away.control.close()
  }

  return (
    <button
      id="channel-test-broadcast-behind-closes"
      type="button"
      onClick={testBroadcastBehindCloses}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Broadcast Closes A Client That Falls Behind
    </button>
  )
}

function BroadcastAwayShared({ show }: { show: Show }) {
  async function testBroadcastAwayShared() {
    show({ broadcastAwayShared: null })
    // Two broadcasts on one key, with lossy bodies of 1200 KiB: one fits the lane's 2 MiB, two don't.
    const key = `away-shared:${crypto.randomUUID()}`
    const pages = await Promise.all(
      [0, 1].map(() => openChannel(() => onChannelPublishesBehind(1200 * 1024, true, key))),
    )
    const { broadcast, control } = pages[0]!
    const { seq: before } = await broadcast.publish('before the disconnect')
    const received: number[][] = [[], []]
    const closed = ['open', 'open']
    // Its publish (1) is outstanding as its connection drops, and 17 bodies follow while it is away: each receives the
    // newest (18), or closes.
    const newest = pages.map(
      ({ broadcast }, i) =>
        new Promise<void>((resolve) => {
          broadcast.subscribeBinary((_, info) => {
            received[i]!.push(info.seq - before)
            if (info.seq === before + 18) resolve()
          })
          broadcast.onClose((err) => {
            closed[i] = err?.name ?? 'none'
            resolve()
          })
        }),
    )
    await control.send('away', { ack: true })
    void broadcast.publish('across the disconnect').catch(() => {})
    await Promise.all(newest)
    show({ broadcastAwayShared: { received, closed } })
    for (const { control } of pages) control.close()
  }

  return (
    <button
      id="channel-test-broadcast-away-shared"
      type="button"
      onClick={testBroadcastAwayShared}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Two Lossy Broadcasts Whose Client Is Away Past What Two Bodies Fit
    </button>
  )
}
