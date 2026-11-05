export { onPublishLoop }

import { Broadcast } from 'telefunc'

const isAscending = (values: number[]) => values.every((value, i) => i === 0 || value > values[i - 1]!)

/** An unawaited loop of publishes to one key, every other one large, with a subscriber in this Durable Object. */
async function onPublishLoop() {
  const key = `publish-order:${crypto.randomUUID()}`
  const arrivals: number[] = []
  const seqs: number[] = []
  const off = Broadcast.subscribe<{ i: number }>(key, ({ i }, { seq }) => {
    arrivals.push(i)
    seqs.push(seq)
  })
  await new Promise((resolve) => setTimeout(resolve, 100))
  const padding = 'x'.repeat(64 * 1024)
  const receipts = await Promise.all(
    Array.from({ length: 200 }, (_, i) => Broadcast.publish(key, { i, padding: i % 2 ? padding : '' })),
  )
  off()
  return {
    received: arrivals.length,
    seqInCallOrder: isAscending(receipts.map(({ seq }) => seq)),
    arrivedInSeqOrder: isAscending(seqs),
    arrivedInCallOrder: isAscending(arrivals),
  }
}
