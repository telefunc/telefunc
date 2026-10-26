export { WireDemo }

import React, { useCallback, useRef, useState } from 'react'
import { Abort, withContext } from 'telefunc/client'
import {
  onWireResume,
  onWireOverrun,
  onWireEnd,
  onWireLarge,
  onWireOpen,
  onWireHeld,
  onWireRelease,
  onWireUpload,
  onWireAnswer,
  onWireAsk,
  onWireConnection,
  onWirePresence,
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
  /** Of the channels opened while `HELD_OPEN` others stayed open: how many got their first message, and whose close() the server acknowledged. */
  held: { run: string; first: number; closed: number } | null
  /** close: what the page's close() resolved to, once the channel opened and the server's messages arrived. */
  release: { channelId: string; close: string } | null
  upload: { channelId: string; sent: number } | null
  /** closed: 'none' or the error's name, once the answerer's channel closed. */
  answerer: { channelId: string; closed: string | null } | null
  /** The answer, or the error's name, of the question to the other page's answerer, then to the page's own. */
  ask: { toOther: string; toOwn: string } | null
  /** What the page's end saw: 'open', 'disconnect', 'reconnect', each with isConnected. */
  connection: { channelId: string; events: string[] } | null
  /** closed: 'abort:<value as JSON>' or the error's name, once the channel closed. */
  presence: { channelId: string; online: boolean; closed?: string } | null
}

const RESUME_COUNT = 256
const OVERRUN_COUNT = 16
const END_BEFORE = 3
const END_CLOSE_TIMEOUT = 500
const LARGE_BYTES = 1_000_000
/** Past the 4,096 channels a connection holds at once. */
const MANY_CHANNELS = 4_200
const RELEASE_COUNT = 3
const HELD_OPEN = 1_000
const HELD_NEXT = 100

function WireDemo() {
  const [state, setState] = useState<WireState>({
    resume: null,
    overrun: null,
    end: null,
    large: null,
    many: null,
    held: null,
    release: null,
    upload: null,
    answerer: null,
    ask: null,
    connection: null,
    presence: null,
  })
  const endChannel = useRef<Awaited<ReturnType<typeof onWireEnd>>['channel'] | null>(null)
  const overrunChannel = useRef<Awaited<ReturnType<typeof onWireOverrun>>['channel'] | null>(null)
  // A channel the page drops closes once garbage-collected.
  const largeChannel = useRef<Awaited<ReturnType<typeof onWireLarge>>['channel'] | null>(null)
  const resumeChannel = useRef<Awaited<ReturnType<typeof onWireResume>>['channel'] | null>(null)
  const connectionChannel = useRef<Awaited<ReturnType<typeof onWireConnection>>['channel'] | null>(null)
  const presenceChannel = useRef<Awaited<ReturnType<typeof onWirePresence>>['channel'] | null>(null)

  const connection = useCallback(async () => {
    const { channel, channelId } = await onWireConnection()
    connectionChannel.current = channel
    const events: string[] = []
    const report = (event: string) => {
      events.push(`${event}:${channel.isConnected}`)
      setState((s) => ({ ...s, connection: { channelId, events: [...events] } }))
    }
    channel.onOpen(() => report('open'))
    channel.onDisconnect(() => report('disconnect'))
    channel.onReconnect(() => report('reconnect'))
  }, [])

  const presence = useCallback(async () => {
    const { channel, channelId } = await onWirePresence()
    presenceChannel.current = channel
    const show = (online: boolean) => setState((s) => ({ ...s, presence: { channelId, online } }))
    let loaded = false
    const loadMissedMessages = async () => {
      if (!loaded) {
        loaded = true
        throw new Error('Could not load the missed messages')
      }
    }
    channel.onOpen(() => show(true))
    channel.onDisconnect(() => show(false))
    channel.onReconnect(async () => {
      await loadMissedMessages()
    })
    channel.onReconnect(() => show(true))
    channel.onClose((err) => {
      const closed = err instanceof Abort ? `abort:${JSON.stringify(err.abortValue)}` : err?.name
      setState((s) => ({ ...s, presence: { channelId, online: false, closed } }))
    })
  }, [])

  const resume = useCallback(async () => {
    const { channel, channelId } = await onWireResume(RESUME_COUNT)
    resumeChannel.current = channel
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
    // A channel the next open replaced may still close: its events aren't the next one's.
    const receive = (item: string) =>
      setState((s) =>
        s.end?.channelId === channelId ? { ...s, end: { ...s.end, received: [...s.end.received, item] } } : s,
      )
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

  const held = useCallback(async () => {
    const run = crypto.randomUUID()
    const open = async (n: number, onRegistered?: () => void) => {
      const channel = await onWireHeld(run, n)
      onRegistered?.()
      const first = await new Promise<string>((resolve) => {
        channel.listen(resolve)
        channel.onClose((err) => resolve(`onClose:${err?.name ?? 'none'}`))
      })
      return { channel, first: first === `first ${n}` }
    }
    const kept: Awaited<ReturnType<typeof open>>[] = []
    for (let n = 0; n < HELD_OPEN; n += 100) {
      kept.push(...(await Promise.all(Array.from({ length: 100 }, (_, k) => open(n + k)))))
    }
    let first = kept.filter((each) => each.first).length
    let closed = 0
    // One at a time, each closed before the next opens, all while the kept ones stay open and one of them sends.
    for (let n = HELD_OPEN; n < HELD_OPEN + HELD_NEXT; n++) {
      const next = await open(n, () => void kept[0]!.channel.send(`kept ${n}`))
      if (next.first) first++
      if ((await next.channel.close()) === 0) closed++
    }
    for (const close of await Promise.all(kept.map(({ channel }) => channel.close()))) if (close === 0) closed++
    setState((s) => ({ ...s, held: { run, first, closed } }))
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

  const answerer = useRef<Awaited<ReturnType<typeof onWireAnswer>>['channel'] | null>(null)
  const answerMalformed = useCallback(async () => {
    const { channel, channelId } = await onWireAnswer()
    answerer.current = channel
    setState((s) => ({ ...s, answerer: { channelId, closed: null } }))
    channel.onClose((err) => setState((s) => ({ ...s, answerer: { channelId, closed: err ? err.name : 'none' } })))
    channel.listen(() => 'answer')
    // The page's connection, past the channel that encodes its answers, as a hostile page can reach it.
    const { _connection: connection } = channel as unknown as {
      _connection: { sendAckRes(answered: unknown, ackedSeq: number, result: string): void }
    }
    const sendAckRes = connection.sendAckRes
    connection.sendAckRes = (answered, ackedSeq) => sendAckRes.call(connection, answered, ackedSeq, '{')
  }, [])

  // Held: a channel the page drops closes once garbage-collected.
  const asking = useRef<unknown[]>([])
  const ask = useCallback(async () => {
    const other = new URLSearchParams(location.search).get('answerer')!
    const own = await onWireAnswer()
    own.channel.listen((question) => `answer to ${question}`)
    const channel = await onWireAsk()
    asking.current = [own.channel, channel]
    // A send on a closed channel throws at once.
    const askTo = (to: string) =>
      Promise.resolve()
        .then(() => channel.send({ to, text: 'question' }, { ack: true }))
        .catch((err: Error) => err.name)
    const toOther = await askTo(other)
    const toOwn = await askTo(own.channelId)
    setState((s) => ({ ...s, ask: { toOther, toOwn } }))
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
          id="wire-held"
          type="button"
          onClick={held}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Open {HELD_NEXT} channels one at a time while {HELD_OPEN} stay open, then close them all
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
          id="wire-connection"
          type="button"
          onClick={connection}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Open a channel that shows its connection
        </button>
        <button
          id="wire-presence"
          type="button"
          onClick={presence}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Go online
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
        <button
          id="wire-answer-malformed"
          type="button"
          onClick={answerMalformed}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Answer relayed questions with an acknowledgement the server can't read
        </button>
        <button
          id="wire-ask"
          type="button"
          onClick={ask}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Ask the answerer named in the URL, then the page's own
        </button>
      </div>
      <pre id="wire-state" className="text-xs">
        {JSON.stringify(state)}
      </pre>
    </div>
  )
}
