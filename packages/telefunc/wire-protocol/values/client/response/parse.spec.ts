import { afterEach, expect, test, vi } from 'vitest'

import { parseResponse } from './parse.js'
import { concat, encodeIndexedFrame, encodeU32, textEncoder } from '../../../wire/envelope.js'
import { SERIALIZER_PREFIX_BLOB_DOWNLOAD } from '../../../constants.js'
import { isBlobDownload } from './DownloadClasses.js'

const KiB = 1024

/** The binary body of a call that returned `{ a, b }`, two downloads: `a`'s chunks and end, then `b`'s. */
function twoDownloadsBody(aChunks: number, bChunks: number): Uint8Array<ArrayBuffer> {
  const download = (index: number, chunks: number) =>
    SERIALIZER_PREFIX_BLOB_DOWNLOAD + JSON.stringify({ __index: index, type: '', size: chunks * 64 * KiB })
  const metadata = textEncoder.encode(JSON.stringify({ ret: { a: download(0, aChunks), b: download(1, bChunks) } }))
  const frames = (index: number, chunks: number) => [
    ...Array.from({ length: chunks }, () => encodeIndexedFrame(index, new Uint8Array(64 * KiB))),
    encodeIndexedFrame(index, new Uint8Array(0)),
  ]
  return concat(encodeU32(metadata.length), metadata, ...frames(0, aChunks), ...frames(1, bChunks), encodeU32(0))
}

async function parseBinaryBody(body: BodyInit): Promise<unknown> {
  const response = new Response(body, { headers: { 'content-type': 'application/octet-stream' } })
  const result = await parseResponse(
    response,
    {
      telefunctionName: 'onDownloads',
      telefuncFilePath: '/downloads.telefunc.ts',
      abortController: new AbortController(),
      requestCloseHandlers: [],
      extensionResponseTypes: [],
    },
    { telefuncUrl: '/_telefunc', transports: ['sse'] },
  )
  if (!(typeof result === 'object' && result !== null && 'ret' in result)) throw new Error('Expected a return value')
  return result.ret
}

test('a download read to its end and then cancelled leaves the next download of the response whole', async () => {
  const ret = await parseBinaryBody(twoDownloadsBody(2, 16))
  if (!(typeof ret === 'object' && ret !== null && 'a' in ret && 'b' in ret)) throw new Error('Expected `{ a, b }`')
  const { a, b } = ret
  if (!isBlobDownload(a) || !isBlobDownload(b)) throw new Error('Expected two downloads')
  expect((await a.bytes()).byteLength).toBe(2 * 64 * KiB)
  a.cancel()
  expect((await b.bytes()).byteLength).toBe(16 * 64 * KiB)
})

/** The body of a call that returned `{ a, b, c }`, three downloads: 1 KiB and then 1 MiB of `a`, then a chunk of `b` and
 *  of `c`, then the ends. It is sent one wire piece per pull, counting them in `sent`; `beforePull` runs before each. */
function threeDownloadsBody() {
  const download = (index: number, chunks: number) =>
    SERIALIZER_PREFIX_BLOB_DOWNLOAD + JSON.stringify({ __index: index, type: '', size: chunks * 64 * KiB })
  const metadata = textEncoder.encode(
    JSON.stringify({ ret: { a: download(0, 17), b: download(1, 1), c: download(2, 1) } }),
  )
  const pieces = [
    concat(encodeU32(metadata.length), metadata),
    encodeIndexedFrame(0, new Uint8Array(KiB)),
    ...Array.from({ length: 16 }, () => encodeIndexedFrame(0, new Uint8Array(64 * KiB))),
    encodeIndexedFrame(1, new Uint8Array(64 * KiB)),
    encodeIndexedFrame(2, new Uint8Array(64 * KiB)),
    encodeIndexedFrame(1, new Uint8Array(0)),
    encodeIndexedFrame(2, new Uint8Array(0)),
    encodeIndexedFrame(0, new Uint8Array(0)),
    encodeU32(0),
  ]
  const state: { sent: number; beforePull?: (sent: number) => void } = { sent: 0 }
  const body = new ReadableStream<Uint8Array<ArrayBuffer>>(
    {
      pull(controller) {
        state.beforePull?.(state.sent)
        const piece = pieces[state.sent++]
        if (piece) controller.enqueue(piece)
        else controller.close()
      },
    },
    { highWaterMark: 0 },
  )
  return { body, state }
}

async function settle(): Promise<void> {
  for (let i = 0; i < 10; i++) await new Promise((resolve) => setImmediate(resolve))
}

test.each(['read', 'cancelled'])(
  'a download left with 1 MiB unread holds the others till it is %s',
  async (release) => {
    const { body, state } = threeDownloadsBody()
    const ret = await parseBinaryBody(body)
    if (!(typeof ret === 'object' && ret !== null && 'a' in ret && 'b' in ret && 'c' in ret)) {
      throw new Error('Expected `{ a, b, c }`')
    }
    const { a, b, c } = ret
    if (!isBlobDownload(a) || !isBlobDownload(b) || !isBlobDownload(c)) throw new Error('Expected three downloads')
    const bBytes = b.bytes()
    await settle()
    // The metadata and `a`'s 1 KiB and 1 MiB: reading stopped on `a`.
    expect(state.sent).toBe(18)
    const cBytes = c.bytes()
    await settle()
    expect(state.sent).toBe(18)

    if (release === 'cancelled') {
      a.cancel()
    } else {
      // The stream takes `a`'s first chunk at once, which leaves 1 MiB unread.
      const aReader = a.stream().getReader()
      await settle()
      expect(state.sent).toBe(18)
      expect((await aReader.read()).value?.byteLength).toBe(KiB)
    }
    expect((await bBytes).byteLength).toBe(64 * KiB)
    expect((await cBytes).byteLength).toBe(64 * KiB)
  },
)

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

test.each(['left unread', 'read'])(
  'a download waited for once reading stopped on another, with none waiting then, warns after 5 s while that one is %s',
  async (release) => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const { body, state } = threeDownloadsBody()
    const ret = await parseBinaryBody(body)
    if (!(typeof ret === 'object' && ret !== null && 'a' in ret && 'b' in ret && 'c' in ret)) {
      throw new Error('Expected `{ a, b, c }`')
    }
    const { a, b, c } = ret
    if (!isBlobDownload(a) || !isBlobDownload(b) || !isBlobDownload(c)) throw new Error('Expected three downloads')
    // `b` stops waiting while the piece that fills `a`'s 1 MiB is on its way: reading stops on `a` with none waiting.
    state.beforePull = (sent) => sent === 17 && b.cancel()
    const bBytes = b.bytes()
    await expect(bBytes).rejects.toThrow('Stream cancelled')
    await settle()
    expect(state.sent).toBe(18)
    const cBytes = c.bytes()
    await settle()
    expect(state.sent).toBe(18)

    if (release === 'read') {
      expect((await a.stream().getReader().read()).value?.byteLength).toBe(KiB)
      expect((await cBytes).byteLength).toBe(64 * KiB)
    }
    vi.advanceTimersByTime(4_999)
    expect(warn).not.toHaveBeenCalled()
    vi.advanceTimersByTime(1)
    if (release === 'read') expect(warn).not.toHaveBeenCalled()
    else expect(warn).toHaveBeenCalledWith(expect.stringContaining('onDownloads() returned is waiting for a stream'))
  },
)
