export { onColdPublish }

import { Broadcast } from 'telefunc'

/** A listener answers the publish that a key's first subscriber is still waiting on, which holds it past the buffer's
 *  budget. Nothing here awaits the answer's promise: it is the delivery's request's, which may have ended by then. */
async function onColdPublish() {
  const key = `cold-publish:${crypto.randomUUID()}`
  const padding = 'x'.repeat(300 * 1024)
  const received: string[] = []
  const answered = new Promise<void>((resolve) => {
    setTimeout(resolve, 3_000)
    const off = Broadcast.subscribe<string>(key, (message) => {
      received.push(message[0]!)
      if (message[0] === 'a') void Broadcast.publish(key, `b${padding}`)
      else {
        off()
        resolve()
      }
    })
  })
  const first = await Promise.resolve(Broadcast.publish(key, `a${padding}`)).then(
    () => 'resolved',
    () => 'rejected',
  )
  await answered
  return { first, received }
}
