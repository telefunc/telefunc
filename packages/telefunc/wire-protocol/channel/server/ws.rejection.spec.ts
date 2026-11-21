import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { Telefunc } from '../../../serve/node.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { WIRE_VERSION } from '../../constants.js'
import { decode, encode, isOneFrame, messageFrames, type DecodedFrame } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import { END_CAUSE } from '../endCause.js'

test("a frame of no channel the server cannot read ends its channels with the connection's sentence, each told on the socket before it closes", async () => {
  const telefunc = new Telefunc()
  const server = createServer((req, res) => void telefunc.serve({ req, res }))
  telefunc.installWebSocket(server)
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('Expected a TCP address')
  const { port } = address
  const channel = new ServerChannel<string, string>({ id: 'rejected-A' })
  getChannelMux().registerChannel(channel)

  const page = new WebSocket(`ws://127.0.0.1:${port}/_telefunc?wire=${WIRE_VERSION}`)
  page.binaryType = 'arraybuffer'
  const frames: DecodedFrame[] = []
  page.addEventListener('message', ({ data }: MessageEvent<ArrayBuffer>) => {
    const message = new Uint8Array(data)
    for (const frame of isOneFrame(message) ? [message] : messageFrames(message)) frames.push(decode(frame))
  })
  const closed = new Promise<CloseEvent>((resolve) => page.addEventListener('close', resolve))
  await new Promise((resolve) => page.addEventListener('open', resolve))
  page.send(encode.reconcile({ open: [{ id: 'rejected-A', ix: 0, lastSeq: 0, initial: true }] }))
  // A wire-level frame of a tag no version has: it names no channel.
  page.send(new Uint8Array([0x0f, 0, 0]))
  const { code } = await closed
  server.close()

  expect([frames.filter(({ tag }) => tag === TAG.ERROR), code]).toEqual([
    [{ tag: TAG.ERROR, index: 0, seq: 1, reason: END_CAUSE.unreadable.code }],
    1000,
  ])
  await expect(channel.closed).resolves.toHaveProperty('message', END_CAUSE.unreadable.error().message)
})
