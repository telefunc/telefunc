// What a wire holds while its RECONCILED is owed is bounded by the limit on what waits on its socket or stream, whatever
// keeps the hold on (decisions/516-sse-carrier.md msg44, the review's R1.1); a channel's replay is its own, not the
// limit's. The real SSE handler and ChannelMux run.

import { expect, test, vi } from 'vitest'

import { MAX_CHANNELS_PER_CONNECTION, WIRE_VERSION } from '../../constants.js'
import { encodeSseRequest } from '../../wire/envelope.js'
import { decode, encode } from '../../wire/frame.js'
import { SEQ_OFFSET, readU32 } from '../../wire/header.js'
import { TAG } from '../../wire/tags.js'
import { getChannelMux } from './ChannelMux.js'
import { ServerChannel } from './ServerChannel.js'
import { openStream, openWebSocket, order } from './serverPage.fixture.js'
import { handleSseChannelRequest } from './sse.js'

// The send backlog's base at 1 MiB, not 64 MiB: the rows that pass it build what passes it in milliseconds.
vi.mock('../../constants.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../constants.js')>()),
  WIRE_SEND_BACKLOG_BASE_BYTES: 2 ** 20,
}))

/** A PING listing as many channels as a page may have, none of them open: its PONG answers each, about 28 KB. */
const widePing = () => encode.ping(Array.from({ length: MAX_CHANNELS_PER_CONNECTION }, (_, ix) => ({ ix, lastSeq: 0 })))

/** More wide PINGs than the send backlog's base (mocked above) allows PONGs for. */
const PINGS_PAST_THE_LIMIT = 100

test.each([
  ['tells', true],
  ["can't tell", false],
])(
  "on a WebSocket whose runtime %s its socket's backlog, what a hold kept on by anything else holds ends its wire past the backlog limit",
  async (_runtime, knowsBacklog) => {
    const page = openWebSocket({ knowsBacklog })
    vi.spyOn(page.mux, 'sendReconciled').mockImplementation(() => {})
    // Its PONG is measured before the hold begins.
    await page.mux.onWireMessage(page.wire, encode.ping([]))
    await page.mux.onWireMessage(page.wire, encode.reconcile({ open: [] }))
    let pings = 0
    for (; pings < PINGS_PAST_THE_LIMIT && !page.state.terminated; pings++)
      await page.mux.onWireMessage(page.wire, widePing())
    expect(page.state.terminated).toBe(true)
    expect(page.frames.filter((frame) => frame[0] === TAG.PONG)).toHaveLength(1)
    vi.restoreAllMocks()
    page.close()
  },
)

test('on SSE, PINGs in batch POSTs while a hold is kept on by anything else end the wire past the backlog limit', async () => {
  const mux = getChannelMux()
  vi.spyOn(mux, 'sendReconciled').mockImplementation(() => {})
  const url = `http://test.local/_telefunc?wire=${WIRE_VERSION}`
  const connId = crypto.randomUUID()
  const opened = await handleSseChannelRequest(
    new Request(url, {
      method: 'POST',
      body: encodeSseRequest({ connId, streamResponse: true }, [encode.reconcile({ open: [] })]),
    }),
  )
  expect(opened.statusCode).toBe(200)
  let status = 200
  for (let post = 0; post < PINGS_PAST_THE_LIMIT / 10 && status === 200; post++) {
    const pings = Array.from({ length: 10 }, widePing)
    const answered = await handleSseChannelRequest(
      new Request(url, { method: 'POST', body: encodeSseRequest({ connId }, pings) }),
    )
    status = answered.statusCode
  }
  expect(status).toBe(400)
  vi.restoreAllMocks()
})

test("over SSE, a reconnect whose replay passes the backlog base is not ended by the limit: the replay is its channel's", async () => {
  const mux = getChannelMux()
  const channel = new ServerChannel()
  mux.registerChannel(channel)
  const first = await openStream(
    'web',
    encode.reconcile({ open: [{ id: channel.id, ix: 0, lastSeq: 0, initial: true }] }),
  )
  const [reconciled] = await first.readUntil((frame) => frame[0] === TAG.RECONCILED)
  const answer = reconciled === undefined ? undefined : decode(reconciled)
  if (answer?.tag !== TAG.RECONCILED) throw new Error('no RECONCILED on the first wire')
  // 2.5 MiB the page never confirms, past what the backlog allows a wire beside its channels' (its base, mocked to
  // 1 MiB, and its largest frame).
  const names = ['m1', 'm2', 'm3', 'm4', 'm5']
  for (const name of names) void channel.send(name + 'x'.repeat(2 ** 19))
  /** The frame of the last message, read from its header alone. */
  const last = (frame: Uint8Array) => frame[0] === TAG.TEXT && readU32(frame, SEQ_OFFSET) === names.length
  await first.readUntil(last)
  await first.drop()
  const second = await openStream(
    'web',
    encode.reconcile({ sessionId: answer.payload.sessionId, open: [{ id: channel.id, ix: 0, lastSeq: 0 }] }),
  )
  // Each message's name: what comes before its padding.
  expect(order(await second.readUntil(last)).map((label) => label.split('x')[0])).toEqual([
    'RECONCILED',
    ...names.map((name, i) => `${i + 1}:${name}`),
  ])
  await second.drop()
})
