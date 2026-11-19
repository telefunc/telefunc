import { createServer } from 'node:http'
import { expect, test } from 'vitest'
import { Telefunc } from '../../../serve/node.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { WIRE_VERSION, WS_CLOSE_REJECTED, WIRE_REJECTED_REASON } from '../../constants.js'
import { encode } from '../../wire/frame.js'

test('a frame the server cannot read reaches the page as 4002 with its reason, and ends its channels with it', async () => {
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
  const closed = new Promise<CloseEvent>((resolve) => page.addEventListener('close', resolve))
  await new Promise((resolve) => page.addEventListener('open', resolve))
  page.send(encode.reconcile({ open: [{ id: 'rejected-A', ix: 0, lastSeq: 0, initial: true }] }))
  page.send(new Uint8Array([0xff, 0, 0]))
  const { code, reason } = await closed
  server.close()

  expect({ code, reason }).toEqual({ code: WS_CLOSE_REJECTED, reason: WIRE_REJECTED_REASON })
  await expect(channel.closed).resolves.toHaveProperty(
    'message',
    expect.stringContaining('the server refused a frame it could not read'),
  )
})
