export { WireDemo }

import React, { useCallback, useRef, useState } from 'react'
import { Abort, withContext } from 'telefunc/client'
import {
  onWireResume,
  onWireOverrun,
  onWireEnd,
  onWireLarge,
  onWireOpen,
  onWireRelease,
  onWireUpload,
} from './Wire.telefunc'
import { Tally, textMessage, binaryMessage, type TallyState } from './wire-messages'

type WireState = {
  resume: { channelId: string; received: TallyState } | null
  overrun: { channelId: string; received: TallyState; onClose: string | null } | null
  /** received: the messages, then 'onClose:' and 'none', 'abort:<value as JSON>' or the error's name; close: what close() resolved to. */
  end: { channelId: string; received: string[]; close: string | null } | null
  /** The length of the large message, once it arrived. */
  large: number | null
  /** How the channel opened after the page opened and closed `closed` others went: 'open' or the error's name. */
  many: { closed: number; next: string } | null
  /** close: what the page's close() resolved to, once the channel opened and the server's messages arrived. */
  release: { channelId: string; close: string } | null
  upload: { channelId: string; sent: number } | null
}

const RESUME_COUNT = 256
const OVERRUN_COUNT = 16
const END_BEFORE = 3
const END_CLOSE_TIMEOUT = 500
const LARGE_BYTES = 1_000_000
/** Past the 4,096 channels a connection holds at once. */
const MANY_CHANNELS = 4_200
const RELEASE_COUNT = 3

function WireDemo() {
  const [state, setState] = useState<WireState>({
    resume: null,
    overrun: null,
    end: null,
    large: null,
    many: null,
    release: null,
    upload: null,
  })
  const endChannel = useRef<Awaited<ReturnType<typeof onWireEnd>>['channel'] | null>(null)
  const overrunChannel = useRef<Awaited<ReturnType<typeof onWireOverrun>>['channel'] | null>(null)
  // A channel the page drops closes once garbage-collected.
  const largeChannel = useRef<Awaited<ReturnType<typeof onWireLarge>>['channel'] | null>(null)

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

  const endCall = useRef(new AbortController())
  const endOpen = useCallback(async () => {
    const { channel, channelId } = await withContext(onWireEnd, { signal: endCall.current.signal })(END_BEFORE)
    endChannel.current = channel
    setState((s) => ({ ...s, end: { channelId, received: [], close: null } }))
    const receive = (item: string) =>
      setState((s) => ({ ...s, end: { ...s.end!, received: [...s.end!.received, item] } }))
    channel.listen(receive)
    channel.onClose((err) => {
      receive(`onClose:${!err ? 'none' : err instanceof Abort ? `abort:${JSON.stringify(err.abortValue)}` : err.name}`)
    })
  }, [])
  const endClose = useCallback(async (opts?: { timeout: number }) => {
    const close = String(await endChannel.current!.close(opts))
    setState((s) => ({ ...s, end: { ...s.end!, close } }))
  }, [])
  const endAbort = useCallback(() => endChannel.current!.abort({ by: 'page' }), [])

  const large = useCallback(async () => {
    const { channel } = await onWireLarge(LARGE_BYTES)
    largeChannel.current = channel
    channel.listen((message) => setState((s) => ({ ...s, large: message.length })))
  }, [])

  // One channel stays open throughout, so the page keeps its connection.
  const many = useCallback(async () => {
    const kept = await onWireOpen()
    for (let opened = 0; opened < MANY_CHANNELS; opened += 100) {
      await Promise.all(Array.from({ length: 100 }, async () => (await onWireOpen()).close()))
    }
    const next = await onWireOpen()
    const opens = new Promise<string>((resolve) => {
      next.onOpen(() => resolve('open'))
      next.onClose((err) => resolve(err?.name ?? 'closed'))
    })
    setState((s) => ({ ...s, many: { closed: MANY_CHANNELS, next: '' } }))
    const outcome = await opens
    setState((s) => ({ ...s, many: { closed: MANY_CHANNELS, next: outcome } }))
    next.close()
    kept.close()
  }, [])

  const uploadChannel = useRef<Awaited<ReturnType<typeof onWireUpload>>['channel'] | null>(null)
  const uploadSent = useRef(0)
  // Sends its first message in the turn the call returns, so it rides on the wire's first batch.
  const uploadOpen = useCallback(async () => {
    const { channel, channelId } = await onWireUpload()
    uploadChannel.current = channel
    uploadSent.current = 1
    void channel.send(textMessage(0))
    setState((s) => ({ ...s, upload: { channelId, sent: 1 } }))
  }, [])
  const uploadSend = useCallback(() => {
    void uploadChannel.current!.send(textMessage(uploadSent.current++))
    const sent = uploadSent.current
    setState((s) => ({ ...s, upload: { ...s.upload!, sent } }))
  }, [])

  const release = useCallback(async (when: 'arrived' | 'opened') => {
    const { channel, channelId } = await onWireRelease(RELEASE_COUNT)
    let received = 0
    const arrived = new Promise<void>((resolve) =>
      channel.listen(() => {
        if (++received === RELEASE_COUNT) resolve()
      }),
    )
    await (when === 'arrived' ? arrived : new Promise<void>((resolve) => channel.onOpen(resolve)))
    const close = String(await channel.close())
    setState((s) => ({ ...s, release: { channelId, close } }))
  }, [])

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
          id="wire-large"
          type="button"
          onClick={large}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Receive a {LARGE_BYTES} byte message as the channel opens
        </button>
        <button
          id="wire-many"
          type="button"
          onClick={many}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Open and close {MANY_CHANNELS} channels, then open one more
        </button>
        <button
          id="wire-release"
          type="button"
          onClick={() => release('arrived')}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Close a channel once the server's {RELEASE_COUNT} messages arrived
        </button>
        <button
          id="wire-release-early"
          type="button"
          onClick={() => release('opened')}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Close a channel as it opens, the server's {RELEASE_COUNT} messages on their way
        </button>
        <button
          id="wire-upload-open"
          type="button"
          onClick={uploadOpen}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Open a channel and send on it at once
        </button>
        <button
          id="wire-upload-send"
          type="button"
          onClick={uploadSend}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Send the next message
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
          onClick={() => endClose()}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Close it
        </button>
        <button
          id="wire-end-close-timeout"
          type="button"
          onClick={() => endClose({ timeout: END_CLOSE_TIMEOUT })}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Close it, waiting {END_CLOSE_TIMEOUT} ms for the server
        </button>
        <button
          id="wire-end-abort"
          type="button"
          onClick={endAbort}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Abort it
        </button>
        <button
          id="wire-end-abort-call"
          type="button"
          onClick={() => endCall.current.abort()}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Abort the call that opened it
        </button>
      </div>
      <pre id="wire-state" className="text-xs">
        {JSON.stringify(state)}
      </pre>
    </div>
  )
}
