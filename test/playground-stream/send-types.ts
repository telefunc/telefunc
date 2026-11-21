// The types of `send()` and `sendBinary()`, checked by `pnpm run test:types` and never run.

export { serverSendTypes, pageSendTypes }

import { expectTypeOf } from 'vitest'
import type { Channel, ClientChannel } from 'telefunc'

/** A generic helper forwards its data to an untyped channel, and a typed channel refuses bytes. */
function serverSendTypes(channel: Channel, chat: Channel<never, { text: string }>) {
  function relay<T>(data: T) {
    return channel.send(data)
  }
  function relayText<T extends { text: string }>(data: T) {
    return channel.send(data)
  }
  void relay(1)
  void relayText({ text: 'x' })
  // @ts-expect-error
  void chat.send(new Uint8Array(1))
}

/** A lossy send on the page is `Promise<void>` on any channel, and takes no `ack: true`. */
function pageSendTypes(page: ClientChannel<string, never, true>) {
  expectTypeOf(page.send('x', { lossy: true })).toEqualTypeOf<Promise<void>>()
  expectTypeOf(page.sendBinary(new Uint8Array(1), { lossy: true, ack: false })).toEqualTypeOf<Promise<void>>()
  const opts = { lossy: true } as const
  expectTypeOf(page.send('x', opts)).toEqualTypeOf<Promise<void>>()
  // @ts-expect-error
  void page.sendBinary(new Uint8Array(1), { lossy: true, ack: true })
}
