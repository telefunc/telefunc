import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { Telefunc } from '../../../serve/node.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { WIRE_VERSION } from '../../constants.js'
import { ERROR_REASON, decode, encode, isOneFrame, messageFrames, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { END_CAUSE } from '../endCause.js'

/** A page's WebSocket to a real Node server, its RECONCILE attaching the channel `id` at index 0; resolves once the page
 *  read the RECONCILED. */
async function openPage(id: string) {
  const telefunc = new Telefunc()
  const server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('Expected a TCP address')
  const { port } = address
  const channel = new ServerChannel<string, string>({ id })
  getChannelMux().registerChannel(channel)

  const page = new WebSocket(`ws://127.0.0.1:${port}/_telefunc?wire=${WIRE_VERSION}`)
  page.binaryType = 'arraybuffer'
  const frames: DecodedFrame[] = []
  let reconciled = () => {}
  const read = new Promise<void>((resolve) => void (reconciled = resolve))
  page.addEventListener('message', ({ data }: MessageEvent<ArrayBuffer>) => {
    const message = new Uint8Array(data)
    for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) frames.push(decode(frame))
    if (frames.some(({ tag }) => tag === TAG.RECONCILED)) reconciled()
  })
  const closed = new Promise<CloseEvent>((resolve) => page.addEventListener('close', resolve))
  await new Promise((resolve) => page.addEventListener('open', resolve))
  page.send(encode.reconcile({ attempt: 1, open: [{ id, ix: 0, lastSeq: 0, initial: true }] }))
  await read
  const close = async () => {
    const event = await closed
    server.close()
    return event
  }
  return { page, frames, close, channel }
}

test("a frame of no channel the server cannot read ends its channels with the connection's sentence, told by the socket's close: its code and the refusal as its reason", async () => {
  const { page, frames, close, channel } = await openPage('rejected-A')
  // A wire-level frame of a tag no version has: it names no channel.
  page.send(new Uint8Array([0x0f, 0, 0]))
  const { code, reason } = await close()

  expect([frames.filter(({ tag }) => tag === TAG.ERROR), code, reason]).toEqual([
    [],
    4002,
    `text/vnd.telefunc.refusal; kind=frames; cause=${END_CAUSE.unreadable.code}`,
  ])
  await expect(channel.closed).resolves.toHaveProperty('message', END_CAUSE.unreadable.error().message)
})

test("a channel's frame the server refuses ends that channel once, by the socket's close: its code and the refusal naming the channel as its reason, with no ERROR before it", async () => {
  const { page, frames, close, channel } = await openPage('rejected-B')
  // A page sends no ERROR but a lost one.
  page.send(encode.error(0, ERROR_REASON.UNCONFIRMED, 1))
  const { code, reason } = await close()

  expect([frames.filter(({ tag }) => tag === TAG.ERROR), code, reason]).toEqual([
    [],
    4002,
    `text/vnd.telefunc.refusal; kind=frames; channel=0; cause=${END_CAUSE.frameRefused.code}`,
  ])
  await expect(channel.closed).resolves.toHaveProperty('message', END_CAUSE.frameRefused.error().message)
})
