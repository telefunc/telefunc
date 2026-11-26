export { Page }

import React, { useEffect, useRef, useState } from 'react'
import {
  onRoomRoundTrip,
  onOrderChannels,
  onJoinRoom,
  onSubscribeThenPublish,
  onListen,
  onPublishFrom,
  onReceived,
  onListInOrder,
  onCreateRoom,
  onListRooms,
  onRetainLarge,
  onCloseWhenLeft,
  onHoldView,
  onCloseHeld,
  onSubscribeHeld,
  onMetaKeyOrder,
  onThrowingListener,
  onTwoRoutes,
  onJoinClosing,
  onJoinRefused,
  onReplyOutcome,
  onRemovalOutcome,
  onJoinOutcome,
  onMetaUpdateOutcome,
  onSendOutcome,
  onBesideRead,
  onSendAfterLapse,
  onSendToClaimed,
} from './Room.telefunc'

type Channels = Awaited<ReturnType<typeof onOrderChannels>>

const PUBLISHES = 30

function Page() {
  const [result, setResult] = useState('')
  const [cardResult, setCardResult] = useState('')
  const [hydrated, setHydrated] = useState(false)
  const channels = useRef<Channels | null>(null)
  const order = useRef<{ broadcast: string[]; room: string[] }>({ broadcast: [], room: [] })
  // The length of the value a size card's call takes.
  const length = useRef<HTMLInputElement>(null)
  useEffect(() => {
    setHydrated(true)
    onRoomRoundTrip().then(
      (value) => setResult(JSON.stringify(value)),
      (error: unknown) => setResult(String(error)),
    )
  }, [])

  // Two pages share `id`; `page` names this one.
  const param = (name: string) => {
    const value = new URLSearchParams(window.location.search).get(name)
    if (value === null) throw new Error(`Test: the page's URL names no '${name}'`)
    return value
  }
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
        const { channel, room } = await onOrderChannels(param('id'))
        channels.current = { channel, room }
        channel.subscribe(({ from, n }) => order.current.broadcast.push(`${from}:${n}`))
        room.subscribe(({ from, n }) => order.current.room.push(`${from}:${n}`))
        return 'subscribed'
      })}
      {card('order-publish', async () => {
        if (channels.current === null) throw new Error('Test: order-subscribe comes first')
        const { channel, room } = channels.current
        const from = param('page')
        const me = await room.join({ meta: { name: from } })
        for (let n = 1; n <= PUBLISHES; n++) {
          void channel.publish({ from, n })
          void me.publish({ from, n })
        }
        return 'published'
      })}
      {card('order-received', async () => order.current)}
      {card('join', () => onJoinRoom(param('id')))}
      {card('subscribe-then-publish', () => onSubscribeThenPublish())}
      {card('listen', () => onListen(param('id'), param('page')))}
      {card('publish', () => onPublishFrom(param('id'), 'hello'))}
      {card('received', () => onReceived(param('id'), param('page')))}
      {card('list', () => onListInOrder())}
      {card('create', () => onCreateRoom(param('id'), param('page')))}
      {card('list-created', () => onListRooms(param('id')))}
      {card('retain-large', () => onRetainLarge())}
      {card('close-when-left', () => onCloseWhenLeft())}
      {card('hold-view', () => onHoldView(param('id')))}
      {card('close-held', () => onCloseHeld(param('id')))}
      {card('subscribe-held', () => onSubscribeHeld(param('id')))}
      {card('meta-key-order', () => onMetaKeyOrder())}
      {card('throwing-listener', () => onThrowingListener())}
      {card('two-routes', () => onTwoRoutes())}
      {card('join-closing', () => onJoinClosing())}
      {card('join-refused', () => onJoinRefused())}
      <input id="room-length" ref={length} type="number" />
      {card('reply-outcome', () => onReplyOutcome(Number(length.current?.value)))}
      {card('removal-outcome', () => onRemovalOutcome(Number(length.current?.value)))}
      {card('join-outcome', () => onJoinOutcome(Number(length.current?.value)))}
      {card('meta-update-outcome', () => onMetaUpdateOutcome(Number(length.current?.value)))}
      {card('send-outcome', () => onSendOutcome(Number(length.current?.value)))}
      {card('beside-read', () => onBesideRead())}
      {card('send-after-lapse', () => onSendAfterLapse())}
      {card('send-to-claimed', () => onSendToClaimed())}
    </div>
  )
}
