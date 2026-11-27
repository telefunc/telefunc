export { ChannelsQueued, AcksWaiting, initialSendsPastMemoryState, type SendsPastMemoryState }

import React from 'react'
import { onChannelAcksWaiting, onChannelsQueuedBeforeReturn } from './Channel.telefunc'
import { onUnreadSends, onKeptUpload, onHello } from './SmallPendingExtra.telefunc'
import { openChannel } from './openChannel'

type SendsPastMemoryState = {
  channelsQueued: { queued: string[]; received: string[][]; closed: boolean[] } | null
  acksWaiting:
    | { ended: string; started: number; settled: Record<string, number>; next: string | null }
    | { error: string }
    | null
  unreadSends: { opened: true } | null
  keptUpload: { next: string; answer: string; read: string } | null
}

const initialSendsPastMemoryState: SendsPastMemoryState = {
  channelsQueued: null,
  acksWaiting: null,
  unreadSends: null,
  keptUpload: null,
}

type Show = (state: Partial<SendsPastMemoryState>) => void

function ChannelsQueued({ show }: { show: Show }) {
  async function testChannelsQueued() {
    show({ channelsQueued: null })
    const channels = await openChannel(onChannelsQueuedBeforeReturn)
    const received = channels.map((channel) => {
      const queuedMessages: string[] = []
      channel.listenBinary(() => queuedMessages.push('queued'))
      return queuedMessages
    })
    // Its answer comes after anything queued before it, and is a message the channel can still carry.
    const queued = await Promise.all(
      channels.map((channel) => channel.send('how', { ack: true }).catch((err: Error) => err.name)),
    )
    const closed = channels.map((channel) => channel.isClosed)
    for (const channel of channels) channel.close()
    show({ channelsQueued: { queued, received, closed } })
  }

  return (
    <button
      id="channel-test-channels-queued"
      type="button"
      onClick={testChannelsQueued}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Eight Channels Queue 1 MiB Each Before Their Telefunction Returns
    </button>
  )
}

function AcksWaiting({ show }: { show: Show }) {
  async function testAcksWaiting(count: number) {
    show({ acksWaiting: null })
    const acksWaiting = await acksWaitingOutcome(count).catch((err: Error) => ({
      error: `${err.name}: ${err.message}`,
    }))
    show({ acksWaiting })
  }

  return (
    <>
      <button
        id="channel-test-acks-waiting"
        type="button"
        onClick={() => testAcksWaiting(500)}
        className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
      >
        500 Acknowledged Sends Wait For Their Answers
      </button>
      <button
        id="channel-test-acks-flood"
        type="button"
        onClick={() => testAcksWaiting(10_000)}
        className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
      >
        10,000 Acknowledged Sends Wait For Their Answers
      </button>
      <UnreadSends show={show} />
      <KeptUpload show={show} />
    </>
  )
}

// `count` acknowledged sends at once, each waiting on the server for its answer. As many as the server can hold room for
// their answers run; the rest close the channel. Then their answers come while the page is away.
async function acksWaitingOutcome(count: number) {
  const { channel, control } = await openChannel(onChannelAcksWaiting)
  const closed = new Promise<string>((resolve) => channel.onClose((err) => resolve(err?.name ?? 'none')))
  let settledEarly = 0
  const sends = Array.from({ length: count }, async (_, n) => {
    try {
      await channel.send(n, { ack: true })
      return 'answered'
    } catch (err) {
      return err instanceof Error ? err.name : String(err)
    } finally {
      settledEarly++
    }
  })
  // Until each waits on the server, or was refused as it came.
  const allIn = async () => {
    while (!channel.isClosed && Number(await control.send('started', { ack: true })) + settledEarly < count) {
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    return 'open'
  }
  const ended = await Promise.race([closed, allIn()])
  const started = Number(await control.send('started', { ack: true }))
  await control.send('release', { ack: true })
  const settled: Record<string, number> = {}
  for (const outcome of await Promise.all(sends)) settled[outcome] = (settled[outcome] ?? 0) + 1
  const next = channel.isClosed ? null : await channel.send('next', { ack: true })
  channel.close()
  control.close()
  return { ended, started, settled, next }
}

// A channel the page reads nothing of while the server sends to it: the test runs the sends.
function UnreadSends({ show }: { show: Show }) {
  async function testUnreadSends() {
    show({ unreadSends: null })
    const channel = await openChannel(onUnreadSends)
    channel.listen(() => {})
    show({ unreadSends: { opened: true } })
  }

  return (
    <button
      id="channel-test-unread-sends"
      type="button"
      onClick={testUnreadSends}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Server Sends To A Page That Reads Nothing
    </button>
  )
}

// A call that returns a channel and keeps its 4 MiB Blob unread, then the next call, then an answer of 512 KiB, then
// the Blob's read.
function KeptUpload({ show }: { show: Show }) {
  async function testKeptUpload() {
    show({ keptUpload: null })
    const channel = await openChannel(() => onKeptUpload(new Blob([new Uint8Array(4 * 1024 * 1024).fill(120)])))
    const next = await onHello()
    const answer = await channel.send('answer', { ack: true }).then(
      (text) => `${text.length} bytes`,
      (err: Error) => err.name,
    )
    const read = await channel.send('read', { ack: true }).catch((err: Error) => err.name)
    channel.close()
    show({ keptUpload: { next, answer, read } })
  }

  return (
    <button
      id="channel-test-kept-upload"
      type="button"
      onClick={testKeptUpload}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Call Keeps A 4 MiB Upload Past Its Answer
    </button>
  )
}
