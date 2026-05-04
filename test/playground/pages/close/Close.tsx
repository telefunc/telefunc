export { Close }

import React, { useEffect, useRef, useState } from 'react'
import { close } from 'telefunc/client'
import type { ClientChannel } from 'telefunc'
import {
  onMixedForClose,
  onCloseGen,
  onCloseStream,
  onCloseChannel,
  onCloseFn,
  onCloseChannelOnClose,
  onCloseStreamAndChannelOnClose,
  onClosePassedFnOnClose,
} from './Close.telefunc'

function Close() {
  const [hydrated, setHydrated] = useState(false)
  const [result, setResult] = useState<string>('')
  const pendingChannelRef = useRef<ClientChannel | null>(null)
  const pendingPassedFnRef = useRef<object | null>(null)
  useEffect(() => setHydrated(true), [])

  return (
    <div>
      {hydrated && <span id="hydrated" />}
      <pre id="close-result">{result}</pre>

      <h2>Generator</h2>

      <button
        id="test-close-gen"
        onClick={async () => {
          setResult('')
          const gen = onCloseGen()
          const values: string[] = []
          const first = await gen.next()
          if (!first.done) values.push(first.value)
          close(gen)
          let nextDone: boolean | null = null
          let error: string | null = null
          try {
            const r = await gen.next()
            nextDone = r.done ?? null
          } catch (e: any) {
            error = e.message
          }
          setResult(JSON.stringify({ method: 'close(gen)', values, nextDone, error }))
        }}
      >
        Generator: close(gen)
      </button>

      <h2>Stream</h2>

      <button
        id="test-close-stream"
        onClick={async () => {
          setResult('')
          const stream = await onCloseStream()
          const reader = stream.getReader()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          const first = await reader.read()
          if (!first.done) chunks.push(decoder.decode(first.value, { stream: true }))
          close(stream)
          setResult(JSON.stringify({ method: 'close(stream)', chunks }))
        }}
      >
        Stream: close(stream)
      </button>

      <h2>Channel</h2>

      <button
        id="test-close-channel"
        onClick={async () => {
          setResult('')
          const channel = await onCloseChannel()
          let channelCloseClean: boolean | null = null
          const channelClosed = new Promise<void>((resolve) => {
            channel.onClose((err) => {
              channelCloseClean = err === undefined
              resolve()
            })
          })
          close(channel)
          await channelClosed
          setResult(JSON.stringify({ method: 'close(channel)', channelCloseClean }))
        }}
      >
        Channel: close(channel)
      </button>

      <h2>Function</h2>

      <button
        id="test-close-fn"
        onClick={async () => {
          setResult('')
          const retFn = await onCloseFn(() => {})
          // Call before close — must succeed
          await retFn()
          close(retFn)
          // Call after close — must throw
          let errorAfterClose: string | null = null
          try {
            await retFn()
          } catch (e: any) {
            errorAfterClose = e.message
          }
          setResult(JSON.stringify({ method: 'close(fn)', errorAfterClose }))
        }}
      >
        Fn: close(fn)
      </button>

      <h2>Channel onClose ordering</h2>

      <button
        id="test-close-channel-onclose"
        onClick={async () => {
          setResult('')
          const channel = await onCloseChannelOnClose()
          const channelClosed = new Promise<void>((resolve) => {
            channel.onClose(() => resolve())
          })
          channel.close()
          await channelClosed
          setResult(JSON.stringify({ method: 'close(channel-onclose)', done: true }))
        }}
      >
        Channel: context.onClose waits for channel
      </button>

      <h2>Stream + Channel onClose ordering</h2>

      <button
        id="test-close-stream-channel-onclose"
        onClick={async () => {
          setResult('')
          const { stream, channel } = await onCloseStreamAndChannelOnClose()
          const reader = stream.getReader()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          while (true) {
            const { done, value } = await reader.read()
            if (done) break
            chunks.push(decoder.decode(value, { stream: true }))
          }
          // Stream is done but channel is still open
          setResult(JSON.stringify({ method: 'close(stream-channel-onclose)', phase: 'stream-done', chunks }))
          // Store channel ref so the close button can access it
          pendingChannelRef.current = channel
        }}
      >
        Stream + Channel: context.onClose waits for both
      </button>
      <button
        id="test-close-stream-channel-close-now"
        onClick={async () => {
          const channel = pendingChannelRef.current
          if (!channel) return
          pendingChannelRef.current = null
          const channelClosed = new Promise<void>((resolve) => {
            channel.onClose(() => resolve())
          })
          channel.close()
          await channelClosed
          setResult(JSON.stringify({ method: 'close(stream-channel-onclose)', phase: 'all-done' }))
        }}
      >
        Close channel now
      </button>

      <h2>Passed function onClose ordering</h2>

      <button
        id="test-close-passed-fn-onclose"
        onClick={async () => {
          setResult('')
          const res = await onClosePassedFnOnClose(() => {})
          // Telefunc returned — the request-side channel backing the callback is still open
          pendingPassedFnRef.current = res
          setResult(JSON.stringify({ method: 'close(passed-fn-onclose)', phase: 'returned' }))
        }}
      >
        Passed fn: context.onClose waits for request channel
      </button>
      <button
        id="test-close-passed-fn-close-now"
        onClick={async () => {
          const res = pendingPassedFnRef.current
          if (!res) return
          pendingPassedFnRef.current = null
          await close(res)
          setResult(JSON.stringify({ method: 'close(passed-fn-onclose)', phase: 'closed' }))
        }}
      >
        Close passed fn result
      </button>

      <h2>{'Mixed: { generator, stream, channel, fn }'}</h2>

      <button
        id="test-close-mixed"
        onClick={async () => {
          setResult('')

          // Pass a callback — server calls it immediately to prove fn round-trip
          const messages: string[] = []
          const result = await onMixedForClose((msg) => {
            messages.push(msg)
          })

          const { generator, stream, channel, fn: retFn } = result

          // Attach channel onClose before close(result) fires
          let channelCloseClean: boolean | null = null
          const channelClosed = new Promise<void>((resolve) => {
            channel.onClose((err) => {
              channelCloseClean = err === undefined
              resolve()
            })
          })

          // Read first token from generator
          const genValues: string[] = []
          const firstGen = await generator.next()
          if (!firstGen.done) genValues.push(firstGen.value)

          // Read first chunk from stream
          const reader = stream.getReader()
          const decoder = new TextDecoder()
          const chunks: string[] = []
          const firstChunk = await reader.read()
          if (!firstChunk.done) chunks.push(decoder.decode(firstChunk.value, { stream: true }))

          // Call the returned fn once (and await the ack) — proves it's callable before close
          await retFn()

          // close(result) — closes generator, stream, channel, AND fn's backing channel
          close(result)

          let genNextDone: boolean | null = null
          let genError: string | null = null
          try {
            const r = await generator.next()
            genNextDone = r.done ?? null
          } catch (e: any) {
            genError = e.message
          }

          let streamReadDone: boolean | null = null
          try {
            const { done } = await reader.read()
            streamReadDone = done ?? null
          } catch {}

          await channelClosed

          // Call retFn after close — must throw ChannelClosedError
          let retFnAfterCloseError: string | null = null
          try {
            await retFn()
          } catch (e: any) {
            retFnAfterCloseError = e.message
          }

          setResult(
            JSON.stringify({
              method: 'close(mixed)',
              passedFnMessages: messages,
              genValues,
              genNextDone,
              genError,
              chunks,
              streamReadDone,
              channelCloseClean,
              retFnAfterCloseError,
            }),
          )
        }}
      >
        Mixed: close(result)
      </button>
    </div>
  )
}
