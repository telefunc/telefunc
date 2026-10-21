export { WireDemo }

import React, { useCallback, useRef, useState } from 'react'
import { Abort } from 'telefunc/client'
import { onWireResume, onWireOverrun, onWireEnd } from './Wire.telefunc'
import { Tally, textMessage, binaryMessage, type TallyState } from './wire-messages'

type WireState = {
  resume: { channelId: string; received: TallyState } | null
  overrun: { channelId: string; received: TallyState; onClose: string | null } | null
  /** onClose: 'none', 'abort:<value as JSON>' or the error's name; close: what close() resolved to. */
  end: { channelId: string; onClose: string | null; close: string | null } | null
}

const RESUME_COUNT = 256
const OVERRUN_COUNT = 16

function WireDemo() {
  const [state, setState] = useState<WireState>({ resume: null, overrun: null, end: null })
  const endChannel = useRef<Awaited<ReturnType<typeof onWireEnd>>['channel'] | null>(null)
  const overrunChannel = useRef<Awaited<ReturnType<typeof onWireOverrun>>['channel'] | null>(null)

  const resume = useCallback(async () => {
    const { channel, channelId } = await onWireResume(RESUME_COUNT)
    const tally = new Tally()
    const show = () => setState((s) => ({ ...s, resume: { channelId, received: { ...tally.state } } }))
    show()
    channel.listen((message) => {
      tally.text(message)
      show()
    })
    channel.listenBinary((message) => {
      tally.binary(message)
      show()
    })
    for (let n = 0; n < RESUME_COUNT; n++) {
      await channel.send(textMessage(n))
      await channel.sendBinary(binaryMessage(n))
    }
  }, [])

  const overrunOpen = useCallback(async () => {
    const { channel, channelId } = await onWireOverrun(OVERRUN_COUNT)
    const tally = new Tally()
    let onClose: string | null = null
    const show = () => setState((s) => ({ ...s, overrun: { channelId, received: { ...tally.state }, onClose } }))
    show()
    channel.onClose((err) => {
      onClose = err ? err.name : 'none'
      show()
    })
    channel.listen((message) => {
      tally.text(message)
      show()
    })
    overrunChannel.current = channel
  }, [])
  const overrunSend = useCallback(() => {
    for (let n = 0; n < OVERRUN_COUNT; n++) void overrunChannel.current!.send(textMessage(n))
  }, [])

  const endOpen = useCallback(async () => {
    const { channel, channelId } = await onWireEnd()
    endChannel.current = channel
    setState((s) => ({ ...s, end: { channelId, onClose: null, close: null } }))
    channel.onClose((err) => {
      const onClose = !err ? 'none' : err instanceof Abort ? `abort:${JSON.stringify(err.abortValue)}` : err.name
      setState((s) => ({ ...s, end: { ...s.end!, onClose } }))
    })
  }, [])
  const endClose = useCallback(async () => {
    const close = String(await endChannel.current!.close())
    setState((s) => ({ ...s, end: { ...s.end!, close } }))
  }, [])
  const endAbort = useCallback(() => endChannel.current!.abort({ by: 'page' }), [])

  return (
    <div className="mt-6 p-4 border rounded">
      <h2 className="text-lg font-semibold mb-2">Wire</h2>
      <div className="flex flex-wrap gap-2 mb-2">
        <button
          id="wire-resume"
          type="button"
          onClick={resume}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Send {RESUME_COUNT} messages each way, connection dropped halfway
        </button>
        <button
          id="wire-overrun-open"
          type="button"
          onClick={overrunOpen}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Open overrun channel
        </button>
        <button
          id="wire-overrun-send"
          type="button"
          onClick={overrunSend}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Send {OVERRUN_COUNT} messages unawaited
        </button>
        <button
          id="wire-end-open"
          type="button"
          onClick={endOpen}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Open a channel to end
        </button>
        <button
          id="wire-end-close"
          type="button"
          onClick={endClose}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Close it
        </button>
        <button
          id="wire-end-abort"
          type="button"
          onClick={endAbort}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Abort it
        </button>
      </div>
      <pre id="wire-state" className="text-xs">
        {JSON.stringify(state)}
      </pre>
    </div>
  )
}
