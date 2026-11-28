export { LossySends, LossyFromPage, initialLossySendsState, type LossySendsState }

import React, { useEffect, useRef } from 'react'
import { onChannelLossySends, onChannelLossyFromPage } from './Channel.telefunc'
import { openChannel } from './openChannel'

type LossySendsState = {
  lossySends: string[] | null
  /** The channel the page's lossy sends go on; the server keeps what it read. */
  lossyFromPageChannelId: string | null
}

const initialLossySendsState: LossySendsState = {
  lossySends: null,
  lossyFromPageChannelId: null,
}

type Show = (state: Partial<LossySendsState>) => void

function LossySends({ show }: { show: Show }) {
  async function testLossySends() {
    show({ lossySends: null })
    const { channel, control } = await openChannel(() => onChannelLossySends(60))
    const read: string[] = []
    channel.listen((value) => void read.push(value))
    await control.send('start', { ack: true })
    const newest = Date.now() + 15_000
    while (read.at(-1) !== 'done' && Date.now() < newest) await new Promise((resolve) => setTimeout(resolve, 100))
    show({ lossySends: read })
    control.close()
    channel.close()
  }

  return (
    <button
      id="channel-test-lossy-sends"
      type="button"
      onClick={testLossySends}
      className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
    >
      Lossy Sends On A Channel
    </button>
  )
}

function LossyFromPage({ show }: { show: Show }) {
  const channelRef = useRef<Awaited<ReturnType<typeof onChannelLossyFromPage>>['channel'] | null>(null)
  useEffect(() => () => void channelRef.current?.close(), [])

  async function openLossyFromPage() {
    show({ lossyFromPageChannelId: null })
    const { channel, channelId } = await openChannel(onChannelLossyFromPage)
    channelRef.current = channel
    show({ lossyFromPageChannelId: channelId })
  }

  /** 60 lossy sends, 1000 / 30 ms apart, each its ordinal, with a plain one after every 20th, then a plain `'done'`. */
  async function testLossyFromPage() {
    await openLossyFromPage()
    const channel = channelRef.current
    if (!channel) throw new Error('no channel open')
    for (let i = 0; i < 60; i++) {
      void channel.send(String(i), { lossy: true })
      if (i % 20 === 19) void channel.send(`R${i}`)
      await new Promise((resolve) => setTimeout(resolve, 1000 / 30))
    }
    void channel.send('done')
  }

  /** What the page sends while it is offline: two lossy, a plain one, two lossy. */
  function sendLossyFromPage() {
    const channel = channelRef.current
    if (!channel) throw new Error('no channel open')
    void channel.send('L1', { lossy: true })
    void channel.send('L2', { lossy: true })
    void channel.send('R1')
    void channel.send('L3', { lossy: true })
    void channel.send('L4', { lossy: true })
  }

  return (
    <>
      <button
        id="channel-test-lossy-from-page"
        type="button"
        onClick={testLossyFromPage}
        className="px-3 py-1.5 text-sm rounded bg-sky-700 text-white hover:bg-sky-800"
      >
        Lossy Sends From The Page
      </button>
      <button
        id="channel-test-lossy-from-page-open"
        type="button"
        onClick={openLossyFromPage}
        className="px-3 py-1.5 text-sm rounded bg-sky-600 text-white hover:bg-sky-700"
      >
        Open A Channel For Lossy Sends From The Page
      </button>
      <button
        id="channel-test-lossy-from-page-send"
        type="button"
        onClick={sendLossyFromPage}
        className="px-3 py-1.5 text-sm rounded bg-sky-600 text-white hover:bg-sky-700"
      >
        Send L1 L2 R1 L3 L4 From The Page
      </button>
    </>
  )
}
