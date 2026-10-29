export { Page }

import React, { useEffect, useRef, useState } from 'react'
import {
  onRoomRoundTrip,
  onOrderChannels,
  onSubscribeThenPublish,
  onListen,
  onPublishFrom,
  onReceived,
  onListInOrder,
  onRetainLarge,
  onCloseWhenLeft,
  onMetaKeyOrder,
} from './Room.telefunc'

type Channels = Awaited<ReturnType<typeof onOrderChannels>>

const PUBLISHES = 30

function Page() {
  const [result, setResult] = useState('')
  const [cardResult, setCardResult] = useState('')
  const [hydrated, setHydrated] = useState(false)
  const channels = useRef<Channels | null>(null)
  const order = useRef<{ broadcast: string[]; room: string[] }>({ broadcast: [], room: [] })
  useEffect(() => {
    setHydrated(true)
    onRoomRoundTrip().then(
      (value) => setResult(JSON.stringify(value)),
      (error: unknown) => setResult(String(error)),
    )
  }, [])

  // Two pages share `id`; `page` names this one.
  const query = () => new URLSearchParams(window.location.search)
  const card = (id: string, run: () => Promise<unknown>) => (
    <button
      type="button"
      id={`room-${id}`}
      onClick={async () => {
        setCardResult('')
        setCardResult(JSON.stringify((await run()) ?? null))
      }}
    >
      {id}
    </button>
  )

  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <pre id="room-result">{result}</pre>
      <pre id="room-card-result">{cardResult}</pre>
      {card('order-subscribe', async () => {
        const { channel, room } = await onOrderChannels(query().get('id')!)
        channels.current = { channel, room }
        channel.subscribe(({ from, n }) => order.current.broadcast.push(`${from}:${n}`))
        room.subscribe(({ from, n }) => order.current.room.push(`${from}:${n}`))
        return 'subscribed'
      })}
      {card('order-publish', async () => {
        const { channel, room } = channels.current!
        const from = query().get('page')!
        const me = await room.join({ meta: { name: from } })
        for (let n = 1; n <= PUBLISHES; n++) {
          void channel.publish({ from, n })
          void me.publish({ from, n })
        }
        return 'published'
      })}
      {card('order-received', async () => order.current)}
      {card('subscribe-then-publish', () => onSubscribeThenPublish())}
      {card('listen', () => onListen(query().get('id')!, query().get('page')!))}
      {card('publish', () => onPublishFrom(query().get('id')!, 'hello'))}
      {card('received', () => onReceived(query().get('id')!, query().get('page')!))}
      {card('list', () => onListInOrder())}
      {card('retain-large', () => onRetainLarge())}
      {card('close-when-left', () => onCloseWhenLeft())}
      {card('meta-key-order', () => onMetaKeyOrder())}
    </div>
  )
}
