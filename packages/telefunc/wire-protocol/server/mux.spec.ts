import { expect, test } from 'vitest'
import { ChannelMux, type ServerTransport } from './mux.js'
import { ServerChannel } from './channel.js'
import { decode, encode, TAG, type DecodedFrame } from '../shared-ws.js'

test("a reconnect waiting for a new channel keeps the channels it moved when the previous wire's close lands", async () => {
  const mux = new ChannelMux()
  const clock = new ServerChannel<string, string>({ id: 'clock' })
  mux.registerChannel(clock)
  const sessions = new Map<object, string>()
  const sent = new Map<object, DecodedFrame[]>()
  const transport: ServerTransport<object> = {
    getSessionId: (wire) => sessions.get(wire),
    setSessionId: (wire, id) => void sessions.set(wire, id),
    getConnId: () => null,
    sendNow: (wire, frame) => void sent.get(wire)!.push(decode(frame)),
    terminateConnection: () => {},
  }
  const open = () => {
    const wire = {}
    sent.set(wire, [])
    mux.onConnectionOpen(wire, transport)
    return wire
  }
  const previous = open()
  await mux.onConnectionRawMessage(
    previous,
    encode.reconcile({ open: [{ id: 'clock', ix: 0, lastSeq: 0, initial: true }] }),
  )
  const next = open()
  // The page reconnects with a callback's channel the server hasn't registered yet (its call failed in the drop).
  const reconciling = mux.onConnectionRawMessage(
    next,
    encode.reconcile({
      sessionId: sessions.get(previous),
      open: [
        { id: 'clock', ix: 0, lastSeq: 0 },
        { id: 'callback', ix: 1, lastSeq: 0, initial: true },
      ],
    }),
  )
  await new Promise((resolve) => setTimeout(resolve, 10))
  mux.onConnectionClosed(previous, { permanent: false }) // the previous wire's ping deadline, inside that wait
  mux.registerChannel(new ServerChannel({ id: 'callback' }))
  await reconciling
  void clock.send('tick')
  expect(sent.get(next)!.some((frame) => frame.tag === TAG.TEXT && frame.text.includes('tick'))).toBe(true)
})
