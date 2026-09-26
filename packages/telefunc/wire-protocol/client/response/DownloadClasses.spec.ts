import { expect, it } from 'vitest'
import { BlobDownload, FileDownload } from './DownloadClasses.js'
import { LazyBlob, LazyFile } from '../../LazyFile.js'
import type { StreamSource } from '../../types.js'

const source = {
  readNextChunk: async () => null,
  bytes: async () => new Uint8Array(),
  stream: () => new ReadableStream(),
  cancel: () => {},
  abort: () => {},
} as unknown as StreamSource

// wrapProxy forwards own keys and client close() walks Object.values(), so download state stays #private.
it('downloads add no own enumerable keys to their lazy base', () => {
  const file = new FileDownload({ name: 'a.txt', type: 'text/plain', size: 1, lastModified: 0 } as never, source)
  const blob = new BlobDownload({ type: 'text/plain', size: 1 } as never, source)
  expect(Object.keys(file)).toEqual(Object.keys(new LazyFile(1, 'text/plain', 'a.txt', 0, source.stream)))
  expect(Object.keys(blob)).toEqual(Object.keys(new LazyBlob(1, 'text/plain', source.stream)))
})
