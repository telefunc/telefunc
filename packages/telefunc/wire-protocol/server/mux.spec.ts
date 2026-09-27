import { expect, test } from 'vitest'
import { ChannelMux, type ServerTransport } from './mux.js'
import { ServerChannel } from './channel.js'
import { decode, encode, TAG, type DecodedFrame } from '../shared-ws.js'

test("a frame sent to a reconnect's wire that dropped while its first reconcile was held replays on the next reconnect", async () => {
  const mux = new ChannelMux()
  const clock = new ServerChannel<string, string>({ id: 'clock-first' })
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
  const first = open()
  await mux.onConnectionRawMessage(
    first,
    encode.reconcile({ open: [{ id: 'clock-first', ix: 0, lastSeq: 0, initial: true }] }),
  )
  const known = sessions.get(first)!
  mux.onConnectionClosed(first, { permanent: false })
  // The reconnect names a callback whose call was lost in the cut, so the server holds its first reconcile.
  const held = open()
  void mux.onConnectionRawMessage(
    held,
    encode.reconcile({
      sessionId: known,
      open: [
        { id: 'clock-first', ix: 0, lastSeq: 0 },
        { id: 'lost-callback', ix: 1, lastSeq: 0, initial: true },
      ],
    }),
  )
  await new Promise((resolve) => setTimeout(resolve, 10))
  mux.onConnectionClosed(held, { permanent: false }) // cut again, inside the hold
  void clock.send('in-the-hold')
  const live = open()
  await mux.onConnectionRawMessage(
    live,
    encode.reconcile({ sessionId: known, open: [{ id: 'clock-first', ix: 0, lastSeq: 0 }] }),
  )
  expect(sent.get(live)!.some((frame) => frame.tag === TAG.TEXT && frame.text.includes('in-the-hold'))).toBe(true)
})
