export { onServerFirstChannel, onServerFirstPublish }

import { Broadcast, BroadcastChannel } from 'telefunc'

/** The server subscribes before it returns the channel, after an `await`, as a telefunction that reads a database does. */
async function onServerFirstChannel(key: string) {
  await new Promise((resolve) => setTimeout(resolve, 20))
  const channel = new BroadcastChannel<string>({ key })
  channel.subscribe(() => {})
  return channel
}

async function onServerFirstPublish(key: string) {
  for (const text of ['p1', 'p2', 'p3']) await Broadcast.publish(key, text)
}
