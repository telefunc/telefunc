export {
  onEcho,
  onCollect,
  onRelay,
  onPassthrough,
  onSlowConsumer,
  onBackpressure,
  onAbortMidStream,
  onLiveLoopback,
  onAbortMidRelay,
}

import { Abort } from 'telefunc'
import { sleep } from '../../sleep'

/** Read all chunks, return as string array. */
async function onEcho(stream: ReadableStream<Uint8Array>) {
  const chunks: string[] = []
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    chunks.push(decoder.decode(value))
  }
  return { chunks }
}

/** Count bytes and chunks. */
async function onCollect(stream: ReadableStream<Uint8Array>) {
  let totalBytes = 0
  let chunkCount = 0
  const reader = stream.getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    totalBytes += value.byteLength
    chunkCount++
  }
  return { totalBytes, chunkCount }
}

/** Read client stream, yield each chunk back as an async generator. */
async function* onRelay(stream: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    yield decoder.decode(value)
  }
}

/** Return the stream as-is — client sends, server returns the same stream. */
async function onPassthrough(stream: ReadableStream<Uint8Array>) {
  return { stream }
}

/** Read one chunk per 200ms — tests backpressure from slow server consumer. */
async function onSlowConsumer(stream: ReadableStream<Uint8Array>) {
  const chunks: string[] = []
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    chunks.push(decoder.decode(value))
    await sleep(200)
  }
  return { chunks }
}

/** Transfer 50 MB through a ReadableStream with credit-based backpressure. */
async function onBackpressure(stream: ReadableStream<Uint8Array>) {
  let totalBytes = 0
  let chunkCount = 0
  const reader = stream.getReader()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    totalBytes += value.byteLength
    chunkCount++
  }

  return {
    totalMB: Math.round(totalBytes / 1024 / 1024),
    chunkCount,
  }
}

/** Read 3 chunks then throw Abort — tests abort propagation to client stream source. */
async function onAbortMidStream(stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  const chunks: string[] = []
  for (let i = 0; i < 3; i++) {
    const { done, value } = await reader.read()
    if (done) break
    chunks.push(decoder.decode(value))
  }
  throw Abort({ reason: 'enough', chunksRead: chunks.length })
}

/** Relay 2 chunks then throw Abort — tests abort propagation from request stream listener to response generator. */
async function* onAbortMidRelay(stream: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let count = 0
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    count++
    yield decoder.decode(value)
    if (count >= 2) throw Abort({ reason: 'mid-relay-abort', chunksRelayed: count })
  }
}

/** Pure loopback: read each chunk from client stream, yield it back immediately. */
async function* onLiveLoopback(stream: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    yield decoder.decode(value)
  }
}
