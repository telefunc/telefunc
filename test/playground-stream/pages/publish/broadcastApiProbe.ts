export { prepareBroadcastApi }

import type { BroadcastChannel } from 'telefunc'

type Source = Pick<BroadcastChannel<string>, 'subscribe' | 'subscribeBinary' | 'messages' | 'onOpen' | 'onClose'>

function prepareBroadcastApi(source: Source) {
  const subscriptions = new AbortController()
  const skipped = AbortSignal.abort('skipped')
  const callbacks: unknown[] = []
  let skippedCalls = 0
  source.subscribe((data, info) => callbacks.push({ type: 'text', data, info }), { signal: subscriptions.signal })
  source.subscribeBinary((data, info) => callbacks.push({ type: 'binary', data: [...data], info }), {
    signal: subscriptions.signal,
  })
  source.subscribe(() => skippedCalls++, { signal: skipped })
  source.subscribeBinary(() => skippedCalls++, { signal: skipped })
  source.onOpen(
    () => {
      skippedCalls++
    },
    { signal: skipped },
  )
  source.onClose(
    () => {
      skippedCalls++
    },
    { signal: skipped },
  )
  const read = (async () => {
    const messages: unknown[] = []
    for await (const message of source.messages()) {
      messages.push({ ...message, data: message.type === 'binary' ? [...message.data] : message.data })
      if (messages.length === 2) break
    }
    return messages
  })()
  const cancellation = new AbortController()
  const cancelled = source
    .messages({ signal: cancellation.signal })
    .next()
    .then(
      () => 'none',
      (err) => err,
    )
  cancellation.abort('stop')
  const preAborted = source
    .messages({ signal: skipped })
    .next()
    .then(
      () => 'none',
      (err) => err,
    )
  return {
    async stop() {
      const messages = await read
      subscriptions.abort()
      return { messages, callbacks, cancelled: await cancelled, preAborted: await preAborted, skippedCalls }
    },
    callbacks: () => callbacks,
  }
}
