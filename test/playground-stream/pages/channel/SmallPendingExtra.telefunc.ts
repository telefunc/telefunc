export { onBroadcastKeys, onUnreadSends, onUnreadEventStream }

import { Channel, BroadcastChannel } from 'telefunc'
import { cleanupState } from '../../cleanup-state'
import { onServerAction } from '../../server-actions'
import { watchChannelMemory, channelMemoryHeld } from '../../channel-memory'

/** Two broadcasts of keys of their own. Once the page asks, a publish on the first larger than the server may hold,
 *  then a small one on the second. */
async function onBroadcastKeys() {
  const first = new BroadcastChannel<string>({ key: `keys-first:${crypto.randomUUID()}` })
  const second = new BroadcastChannel<string>({ key: `keys-second:${crypto.randomUUID()}` })
  const control = new Channel<(msg: 'publish') => string, never>()
  control.listen(async () => {
    const oversized = first.publishBinary(new Uint8Array(8 * 1024 * 1024)).then(
      () => 'published',
      (err: Error) => err.name,
    )
    void second.publish('after the oversized publish')
    return oversized
  })
  return { first, second, control: control.client }
}

/** The pages' channels the server sends to on the test's `unreadSends` action, each with how its sends ended. */
const unread: { channel: Channel<never, string>; outcomes: Record<string, number> }[] = []

/** A channel the server sends 340 messages of 15 KiB on, unawaited, once the test runs `unreadSends`; then the test's
 *  `unreadSendsPeak` puts the most the server's channel memory held meanwhile in `cleanupState.unreadSendsPeak`. */
async function onUnreadSends() {
  const channel = new Channel<never, string>()
  unread.push({ channel, outcomes: {} })
  onServerAction('unreadSends', sendToUnread, { once: false })
  return channel.client
}

function sendToUnread() {
  const peak = watchChannelMemory()
  const pages = unread.splice(0)
  onServerAction(
    'unreadSendsPeak',
    () => {
      cleanupState.unreadSendsPeak = JSON.stringify({ ...peak(), outcomes: pages.map(({ outcomes }) => outcomes) })
    },
    { once: false },
  )
  const text = 'x'.repeat(15 * 1024)
  for (const { channel, outcomes } of pages) {
    const count = (outcome: string) => void (outcomes[outcome] = (outcomes[outcome] ?? 0) + 1)
    // The server may already have ended a channel whose page reads nothing.
    if (channel.isClosed) {
      count('closed')
      continue
    }
    for (let i = 0; i < 340; i++)
      void channel.send(text).then(
        () => count('sent'),
        (err: Error) => count(err.name),
      )
  }
}

/** A channel for a page of the test's own, which reads nothing of its event stream. How it ends goes in
 *  `cleanupState.unreadEventStream_<id>`; the test's `channelMemory` action puts what the server's channel memory holds,
 *  and the most it held since this call, in `cleanupState.channelMemory`. */
async function onUnreadEventStream() {
  const channel = new Channel<never, never>()
  const id = channel.id
  const peak = watchChannelMemory()
  channel.onClose((err) => void (cleanupState[`unreadEventStream_${id}`] = err ? err.name : 'clean'))
  onServerAction(
    'channelMemory',
    () => void (cleanupState.channelMemory = JSON.stringify({ ...peak(), total: channelMemoryHeld() })),
    { once: false },
  )
  return { channel: channel.client, channelId: id }
}
