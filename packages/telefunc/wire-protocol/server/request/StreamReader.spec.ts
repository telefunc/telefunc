import { describe, it, expect } from 'vitest'
import { StreamReader } from './StreamReader.js'

function streamOf(...chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk)
      controller.close()
    },
  })
}

const enc = new TextEncoder()

describe('StreamReader.readAllText', () => {
  it('reads an un-framed body as text, across chunk boundaries, under the cap', async () => {
    const reader = new StreamReader(streamOf(enc.encode('{"x":'), enc.encode('1}')), 100)
    expect(await reader.readAllText()).toBe('{"x":1}')
  })

  it('rejects a body over the per-message cap instead of buffering it unbounded', async () => {
    const reader = new StreamReader(streamOf(enc.encode('x'.repeat(50)), enc.encode('y'.repeat(60))), 64)
    await expect(reader.readAllText()).rejects.toThrow(/per-message limit/)
  })
})
