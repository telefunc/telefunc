export { testFileDownload }

import { page, test, expect, autoRetry, getServerUrl } from '@brillout/test-e2e'
import { waitForHydration, getResult } from '../../e2e-utils'

const TEXT = 'Hello, file download!'
const TEXT_BYTES = TEXT.length // ASCII — 1 byte per char
const FIXED_LAST_MODIFIED = 1700000000000

function testFileDownload() {
  test('file download: native File auto-materializes on client', async () => {
    await page.goto(`${getServerUrl()}/file-download`)
    await waitForHydration()

    await page.click('#test-file-eager')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isFile: true,
        name: 'eager.txt',
        size: TEXT_BYTES,
        type: 'text/plain',
        lastModified: FIXED_LAST_MODIFIED,
        content: TEXT,
      })
    })
  })

  test('file download: native Blob auto-materializes on client', async () => {
    await page.click('#test-blob-eager')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isBlob: true,
        isFile: false,
        size: TEXT_BYTES,
        type: 'text/plain',
        content: TEXT,
      })
    })
  })

  test('file download: download(stream, {name}) → FileDownload', async () => {
    await page.click('#test-download-file-stream')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isFile: true,
        name: 'streamed.txt',
        size: TEXT_BYTES,
        type: 'text/plain',
        lastModified: FIXED_LAST_MODIFIED,
        content: TEXT,
        // dl.* metadata available pre-materialize must match the materialized File.
        dl: {
          name: 'streamed.txt',
          type: 'text/plain',
          size: TEXT_BYTES,
          lastModified: FIXED_LAST_MODIFIED,
        },
      })
    })
  })

  test('file download: download(stream, {name}) without size', async () => {
    await page.click('#test-download-file-stream-no-size')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isFile: true,
        name: 'streamed-nosize.txt',
        size: TEXT_BYTES,
        type: 'text/plain',
        lastModified: FIXED_LAST_MODIFIED,
        content: TEXT,
        // dl.size is undefined (server declared none) while the materialized File.size is
        // the buffered length — JSON drops undefined, so size is absent from dl here.
        dl: {
          name: 'streamed-nosize.txt',
          type: 'text/plain',
          lastModified: FIXED_LAST_MODIFIED,
        },
      })
    })
  })

  test('file download: download(stream, {type}) → BlobDownload', async () => {
    await page.click('#test-download-blob-stream')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isBlob: true,
        isFile: false,
        size: TEXT_BYTES,
        type: 'text/plain',
        content: TEXT,
        dl: { type: 'text/plain', size: TEXT_BYTES },
      })
    })
  })

  test('file download: download(stream, {type}) without size', async () => {
    await page.click('#test-download-blob-stream-no-size')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isBlob: true,
        size: TEXT_BYTES,
        type: 'text/plain',
        content: TEXT,
        dl: { type: 'text/plain' },
      })
    })
  })

  test('file download: download(file) wraps native File', async () => {
    await page.click('#test-download-wrap-file')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isFile: true,
        name: 'wrapped.txt',
        size: TEXT_BYTES,
        type: 'text/plain',
        lastModified: FIXED_LAST_MODIFIED,
        content: TEXT,
        // download(File) inherits all metadata from the wrapped native File — must match.
        dl: {
          name: 'wrapped.txt',
          type: 'text/plain',
          size: TEXT_BYTES,
          lastModified: FIXED_LAST_MODIFIED,
        },
      })
    })
  })

  test('file download: download(blob) wraps native Blob', async () => {
    await page.click('#test-download-wrap-blob')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isBlob: true,
        size: TEXT_BYTES,
        type: 'text/plain',
        content: TEXT,
        dl: { type: 'text/plain', size: TEXT_BYTES },
      })
    })
  })

  test('file download: download → onProgress with known total', async () => {
    await page.click('#test-progress-known')
    await autoRetry(
      async () => {
        const result = await getResult('#download-result')
        const TOTAL = 2 * 1024 * 1024
        const CHUNK = 32 * 1024
        const expectedTicks = TOTAL / CHUNK
        expect(result.done).toBe(true)
        expect(result.isFile).toBe(true)
        expect(result.size).toBe(TOTAL)
        expect(result.tickCount).greaterThan(expectedTicks / 2)
        expect(result.firstTotal).toBe(TOTAL)
        expect(result.lastLoaded).toBe(TOTAL)
        expect(result.monotonic).toBe(true)
        expect(result.totalsAllEqual).toBe(true)
        expect(Array.isArray(result.ticks)).toBe(true)
        expect(result.ticks.length).toBe(result.tickCount)
        // Strict tick-sequence checks:
        const ticks = result.ticks as { i: number; loaded: number; total: number }[]
        expect(ticks[0]!.i).toBe(1)
        expect(ticks[ticks.length - 1]!.i).toBe(ticks.length)
        expect(ticks.every((t, idx) => t.i === idx + 1)).toBe(true) // strictly increasing index, no dupes
        expect(ticks.every((t) => t.total === TOTAL)).toBe(true)
        expect(ticks.every((t, idx) => idx === 0 || t.loaded > ticks[idx - 1]!.loaded)).toBe(true) // strictly increasing
      },
      { timeout: 30_000 },
    )
  })

  test('file download: download → onProgress with undefined total when no size', async () => {
    await page.click('#test-progress-unknown')
    await autoRetry(
      async () => {
        const result = await getResult('#download-result')
        const TOTAL = 1 * 1024 * 1024
        expect(result.done).toBe(true)
        expect(result.isFile).toBe(true)
        expect(result.size).toBe(TOTAL)
        expect(result.tickCount).greaterThan(0)
        expect(result.firstTotalIsUndefined).toBe(true)
        expect(result.allTotalsUndefined).toBe(true)
        expect(result.monotonic).toBe(true)
        expect(result.lastLoaded).toBe(TOTAL)
        const ticks = result.ticks as { i: number; loaded: number; total: undefined }[]
        expect(ticks.length).toBe(result.tickCount)
        expect(ticks.every((t, idx) => t.i === idx + 1)).toBe(true) // no duplicate indices
        expect(ticks.every((t) => t.total === undefined)).toBe(true)
        expect(ticks.every((t, idx) => idx === 0 || t.loaded > ticks[idx - 1]!.loaded)).toBe(true)
      },
      { timeout: 30_000 },
    )
  })

  test('file download: download → cancel mid-flight rejects .file', async () => {
    await page.click('#test-progress-cancel')
    await autoRetry(
      async () => {
        const result = await getResult('#download-result')
        expect(result.done).toBe(true)
        expect(result.rejected).toBe(true)
        expect(result.errorContains).toBe(true)
        expect(result.bytes).toBe(0)
        // Cancel fires after first non-zero tick, so we should have at least one tick recorded.
        expect(result.cancelledAfter).greaterThan(0)
        expect(result.ticks.length).greaterThanOrEqual(result.cancelledAfter)
      },
      { timeout: 30_000 },
    )
  })

  test('file download: binary content with multi-chunk source', async () => {
    await page.click('#test-file-binary')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      // bytes 0..255 repeated 4 times = 1024 bytes total; checksum = 32640 × 4 = 130560
      expect(result).deep.equal({
        isFile: true,
        name: 'binary.bin',
        size: 1024,
        type: 'application/octet-stream',
        byteLength: 1024,
        checksum: 130560,
        firstByte: 0,
        lastByte: 255,
      })
    })
  })

  test('file download: 5 MB file across many chunks', async () => {
    await page.click('#test-file-large')
    await autoRetry(
      async () => {
        const result = await getResult('#download-result')
        expect(result).deep.equal({
          isFile: true,
          name: 'large.bin',
          size: 5 * 1024 * 1024,
          type: 'application/octet-stream',
          byteLength: 5 * 1024 * 1024,
        })
      },
      { timeout: 30_000 },
    )
  })

  test('file download: empty (0 bytes)', async () => {
    await page.click('#test-file-empty')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        isFile: true,
        name: 'empty.txt',
        size: 0,
        type: 'text/plain',
        content: '',
      })
    })
  })

  test('file download: nested FileDownload slot', async () => {
    await page.click('#test-file-nested')
    await autoRetry(async () => {
      const result = await getResult('#download-result')
      expect(result).deep.equal({
        label: 'wrapped',
        count: 7,
        isFile: true,
        name: 'nested.txt',
        size: TEXT_BYTES,
        content: TEXT,
      })
    })
  })
}
