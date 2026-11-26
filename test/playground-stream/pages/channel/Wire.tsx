export { WireDemo }

import React, { useCallback, useRef, useState } from 'react'
import { Abort, withContext } from 'telefunc/client'
import {
  onWireResume,
  onWireBurst,
  onWireEnd,
  onWireLarge,
  onWireOpen,
  onWireHeld,
  onWireRelease,
  onWireUpload,
} from './Wire.telefunc'
import { Tally, textMessage, binaryMessage, type TallyState } from './wire-messages'

type WireState = {
  resume: { channelId: string; received: TallyState } | null
  burst: { channelId: string; received: number[]; onClose: string | null } | null
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
  /** Two channels on the page's connection, as the server refuses a frame of `refused`'s: how each ended, if it did
   *  ('open' while it hasn't), and what `kept` sent. */
  uploadRefused: {
    refused: { channelId: string; close: string }
    kept: { channelId: string; sent: number; close: string }
  } | null
}

const RESUME_COUNT = 256
const END_BEFORE = 3
const END_CLOSE_TIMEOUT = 500
const LARGE_BYTES = 1_000_000
const KiB = 1024
/** Nearly all of the page's first credit, then nearly 64 MiB each: from the 6th on, past what one channel holds. */
const burstSizes = (sends: number) => [2048 * KiB - KiB, ...Array.from({ length: sends - 1 }, () => 65536 * KiB - KiB)]
/** On a server whose `maxMemory` is 2 MiB: one held, one that waits for the page to confirm it, and one that would carry
 *  what is held and what waits past `maxMemory`. */
const PAST_MEMORY_SIZES = [1200 * KiB, 700 * KiB, 200 * KiB]
const SLOW_LINK_LARGE_BYTES = 4_000_000
/** Past the 4,096 channels a connection holds at once. */
const MANY_CHANNELS = 4_200
const RELEASE_COUNT = 3
const HELD_OPEN = 1_000
const HELD_NEXT = 100

function WireDemo() {
  const [state, setState] = useState<WireState>({
    resume: null,
    burst: null,
    end: null,
    large: null,
    many: null,
    held: null,
    release: null,
    upload: null,
    uploadRefused: null,
  })
  const endChannel = useRef<Awaited<ReturnType<typeof onWireEnd>>['channel'] | null>(null)
  const burstChannel = useRef<Awaited<ReturnType<typeof onWireBurst>>['channel'] | null>(null)
  // A channel the page drops closes once garbage-collected.
  const largeChannel = useRef<Awaited<ReturnType<typeof onWireLarge>>['channel'] | null>(null)
  const resumeChannel = useRef<Awaited<ReturnType<typeof onWireResume>>['channel'] | null>(null)

  const resume = useCallback(async () => {
    const { channel, channelId } = await onWireResume(RESUME_COUNT)
    resumeChannel.current = channel
    const tally = new Tally({ alternating: true })
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

  const burstOpen = useCallback(async (sizes: number[], kind: 'text' | 'binary') => {
    const { channel, channelId } = await onWireBurst(sizes, kind)
    const received: number[] = []
    let onClose: string | null = null
    const show = () => setState((s) => ({ ...s, burst: { channelId, received: [...received], onClose } }))
    show()
    channel.onClose((err) => {
      onClose = err ? err.name : 'none'
      show()
    })
    const onMessage = (index: number) => {
      received.push(index)
      show()
    }
    channel.listen((message) => onMessage(Number.parseInt(message)))
    channel.listenBinary((data) => onMessage(new DataView(data.buffer, data.byteOffset).getUint32(0)))
    burstChannel.current = channel
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
    if (!endChannel.current) throw new Error("Open the end card's channel first")
    const close = (await endChannel.current.close(opts))?.name ?? 'clean'
    setState((s) => ({ ...s, end: { ...s.end!, close } }))
  }, [])
  const endAbort = useCallback(() => endChannel.current!.abort({ by: 'page' }), [])

  const large = useCallback(async (bytes: number) => {
    const { channel } = await onWireLarge(bytes)
    largeChannel.current = channel
    channel.listen((message) => setState((s) => ({ ...s, large: message.length })))
  }, [])

  // One channel stays open throughout, so the page keeps its connection.
  const many = useCallback(async () => {
    const kept = await onWireOpen()
    let closed = 0
    for (let opened = 0; opened < MANY_CHANNELS; opened += 100) {
      const closes = await Promise.all(Array.from({ length: 100 }, async () => (await onWireOpen()).close()))
      for (const close of closes) if (close === null) closed++
    }
    const next = await onWireOpen()
    const opens = new Promise<string>((resolve) => {
      next.onOpen(() => resolve('open'))
      next.onClose((err) => resolve(err?.name ?? 'closed'))
    })
    setState((s) => ({ ...s, many: { closed, next: '' } }))
    const outcome = await opens
    setState((s) => ({ ...s, many: { closed, next: outcome } }))
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
      if ((await next.channel.close()) === null) closed++
    }
    for (const close of await Promise.all(kept.map(({ channel }) => channel.close()))) if (close === null) closed++
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

  const refusedChannels = useRef<{
    refused: Awaited<ReturnType<typeof onWireUpload>>['channel']
    kept: Awaited<ReturnType<typeof onWireUpload>>['channel']
  } | null>(null)
  const keptSent = useRef(0)
  // Both open before either sends, so what they send goes on the wire's streaming upload where the page has one.
  const uploadRefusedOpen = useCallback(async () => {
    const [refused, kept] = await Promise.all([onWireUpload(), onWireUpload()])
    await Promise.all([refused.channel.opened, kept.channel.opened])
    refusedChannels.current = { refused: refused.channel, kept: kept.channel }
    const ended = (which: 'refused' | 'kept') => (err: unknown) =>
      setState((s) => {
        if (!s.uploadRefused) return s
        const close = err instanceof Error ? err.message : String(err)
        return { ...s, uploadRefused: { ...s.uploadRefused, [which]: { ...s.uploadRefused[which], close } } }
      })
    refused.channel.onClose(ended('refused'))
    kept.channel.onClose(ended('kept'))
    setState((s) => ({
      ...s,
      uploadRefused: {
        refused: { channelId: refused.channelId, close: 'open' },
        kept: { channelId: kept.channelId, sent: 0, close: 'open' },
      },
    }))
  }, [])
  const uploadRefusedSend = useCallback((which: 'refused' | 'kept') => {
    const channels = refusedChannels.current
    if (!channels) return
    if (which === 'refused') return void channels.refused.send('refused').catch(() => {})
    void channels.kept.send(textMessage(keptSent.current++)).catch(() => {})
    const sent = keptSent.current
    setState((s) =>
      s.uploadRefused ? { ...s, uploadRefused: { ...s.uploadRefused, kept: { ...s.uploadRefused.kept, sent } } } : s,
    )
  }, [])

  const release = useCallback(async (when: 'arrived' | 'opened') => {
    const { channel, channelId } = await onWireRelease(RELEASE_COUNT)
    let received = 0
    const arrived = new Promise<void>((resolve) =>
      channel.listen(() => {
        if (++received === RELEASE_COUNT) resolve()
      }),
    )
    await (when === 'arrived' ? arrived : channel.opened)
    const close = (await channel.close())?.name ?? 'clean'
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
        {(['text', 'binary'] as const).map((kind) =>
          [5, 6].map((sends) => (
            <button
              key={`${kind}-${sends}`}
              id={`wire-burst-open-${kind}-${sends}`}
              type="button"
              onClick={() => burstOpen(burstSizes(sends), kind)}
              className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
            >
              Open {kind} burst channel, {sends} large messages
            </button>
          )),
        )}
        {(['text', 'binary'] as const).map((kind) => (
          <button
            key={`${kind}-past-memory`}
            id={`wire-burst-open-${kind}-past-memory`}
            type="button"
            onClick={() => burstOpen(PAST_MEMORY_SIZES, kind)}
            className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
          >
            Open {kind} burst channel past a 2 MiB maxMemory
          </button>
        ))}
        <button
          id="wire-large"
          type="button"
          onClick={() => large(LARGE_BYTES)}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Receive a {LARGE_BYTES} byte message as the channel opens
        </button>
        <button
          id="wire-large-slow-link"
          type="button"
          onClick={() => large(SLOW_LINK_LARGE_BYTES)}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Receive a {SLOW_LINK_LARGE_BYTES} byte message as the channel opens
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
          id="wire-upload-refused-open"
          type="button"
          onClick={uploadRefusedOpen}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Open two channels on the page's connection
        </button>
        <button
          id="wire-upload-refused-send-refused"
          type="button"
          onClick={() => uploadRefusedSend('refused')}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Send on the first
        </button>
        <button
          id="wire-upload-refused-send-kept"
          type="button"
          onClick={() => uploadRefusedSend('kept')}
          className="px-3 py-1.5 text-sm rounded bg-slate-600 text-white hover:bg-slate-700"
        >
          Send the second's next message
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
