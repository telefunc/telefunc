export { onColdPublish }

import { Broadcast } from 'telefunc'

/** A listener answers the first publish to a key while that publish is still delivered, as the key's first presence
 *  is written. Nothing here awaits the answer's promise: it is the delivery's request's, which may have ended by then. */
async function onColdPublish() {
  const key = `cold-publish:${crypto.randomUUID()}`
  const received: string[] = []
  const answered = new Promise<void>((resolve) => {
    setTimeout(resolve, 3_000)
    const off = Broadcast.subscribe<string>(key, (message) => {
      received.push(message)
      if (message === 'a') void Broadcast.publish(key, 'b')
      else {
        off()
        resolve()
      }
    })
  })
  const first = await Promise.resolve(Broadcast.publish(key, 'a')).then(
    () => 'resolved',
    () => 'rejected',
  )
  await answered
  return { first, received }
}
