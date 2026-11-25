import { assert, expect, test } from 'vitest'

import { parseResponse } from './parse.js'
import { concat, encodeIndexedFrame, encodeU32, textEncoder } from '../../../wire/envelope.js'
import { SERIALIZER_PREFIX_BLOB_DOWNLOAD } from '../../../constants.js'
import { isBlobDownload } from './DownloadClasses.js'
import { hasProp } from '../../../../utils/hasProp.js'

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

test('a download read to its end and then cancelled leaves the next download of the response whole', async () => {
  const response = new Response(twoDownloadsBody(2, 16), { headers: { 'content-type': 'application/octet-stream' } })
  const result = await parseResponse(
    response,
    {
      telefunctionName: 'onTwo',
      telefuncFilePath: '/two.telefunc.ts',
      abortController: new AbortController(),
      requestCloseHandlers: [],
      extensionResponseTypes: [],
      extensionLazyResponseTypes: [],
    },
    { telefuncUrl: '/_telefunc', transports: ['sse'] },
  )
  assert(hasProp(result, 'ret', 'object'), 'The call returns its value under `ret`')
  const { a, b } = result.ret
  assert(isBlobDownload(a) && isBlobDownload(b), 'Both values read back as downloads')
  expect((await a.bytes()).byteLength).toBe(2 * 64 * KiB)
  a.cancel()
  expect((await b.bytes()).byteLength).toBe(16 * 64 * KiB)
})
