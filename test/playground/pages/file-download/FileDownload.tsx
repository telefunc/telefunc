export { FileDownload }

import React, { useEffect, useState } from 'react'
import {
  onDownloadFileEager,
  onDownloadBlobEager,
  onDownloadFileStream,
  onDownloadFileStreamNoSize,
  onDownloadBlobStream,
  onDownloadBlobStreamNoSize,
  onDownloadWrapFile,
  onDownloadWrapBlob,
  onDownloadProgressKnownSize,
  onDownloadProgressUnknownSize,
  onDownloadFileBinary,
  onDownloadFileLarge,
  onDownloadFileEmpty,
  onDownloadFileNested,
} from './FileDownload.telefunc'

function FileDownload() {
  const [result, setResult] = useState<string>('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div>
      <h2>File Download Tests</h2>
      {hydrated && <span id="hydrated" />}

      {/* eager native File — auto-materializes on client */}
      <button
        id="test-file-eager"
        onClick={async () => {
          const f = await onDownloadFileEager()
          setResult(
            JSON.stringify({
              isFile: f instanceof File,
              name: f.name,
              size: f.size,
              type: f.type,
              lastModified: f.lastModified,
              content: await f.text(),
            }),
          )
        }}
      >
        new File() eager
      </button>

      {/* eager native Blob — auto-materializes on client */}
      <button
        id="test-blob-eager"
        onClick={async () => {
          const b = await onDownloadBlobEager()
          setResult(
            JSON.stringify({
              isBlob: b instanceof Blob,
              isFile: b instanceof File,
              size: b.size,
              type: b.type,
              content: await b.text(),
            }),
          )
        }}
      >
        new Blob() eager
      </button>

      {/* download(stream, {name, ...}) — returns FileDownload tuple */}
      <button
        id="test-download-file-stream"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const dlMeta = { name: dl.name, type: dl.type, size: dl.size, lastModified: dl.lastModified }
          const f = await dl.saveToMemory()
          setResult(
            JSON.stringify({
              isFile: f instanceof File,
              name: f.name,
              size: f.size,
              type: f.type,
              lastModified: f.lastModified,
              content: await f.text(),
              dl: dlMeta,
            }),
          )
        }}
      >
        download(stream) → File
      </button>

      <button
        id="test-download-file-stream-no-size"
        onClick={async () => {
          const dl = await onDownloadFileStreamNoSize()
          const dlMeta = { name: dl.name, type: dl.type, size: dl.size, lastModified: dl.lastModified }
          const f = await dl.saveToMemory()
          setResult(
            JSON.stringify({
              isFile: f instanceof File,
              name: f.name,
              size: f.size,
              type: f.type,
              lastModified: f.lastModified,
              content: await f.text(),
              dl: dlMeta,
            }),
          )
        }}
      >
        download(stream) → File w/o size
      </button>

      {/* download(stream, {type, ...}) — returns BlobDownload tuple */}
      <button
        id="test-download-blob-stream"
        onClick={async () => {
          const dl = await onDownloadBlobStream()
          const dlMeta = { type: dl.type, size: dl.size }
          const b = await dl.saveToMemory()
          setResult(
            JSON.stringify({
              isBlob: b instanceof Blob,
              isFile: b instanceof File,
              size: b.size,
              type: b.type,
              content: await b.text(),
              dl: dlMeta,
            }),
          )
        }}
      >
        download(stream) → Blob
      </button>

      <button
        id="test-download-blob-stream-no-size"
        onClick={async () => {
          const dl = await onDownloadBlobStreamNoSize()
          const dlMeta = { type: dl.type, size: dl.size }
          const b = await dl.saveToMemory()
          setResult(
            JSON.stringify({
              isBlob: b instanceof Blob,
              size: b.size,
              type: b.type,
              content: await b.text(),
              dl: dlMeta,
            }),
          )
        }}
      >
        download(stream) → Blob w/o size
      </button>

      {/* download(File) — wraps native File for opt-in tuple */}
      <button
        id="test-download-wrap-file"
        onClick={async () => {
          const dl = await onDownloadWrapFile()
          const dlMeta = { name: dl.name, type: dl.type, size: dl.size, lastModified: dl.lastModified }
          const f = await dl.saveToMemory()
          setResult(
            JSON.stringify({
              isFile: f instanceof File,
              name: f.name,
              size: f.size,
              type: f.type,
              lastModified: f.lastModified,
              content: await f.text(),
              dl: dlMeta,
            }),
          )
        }}
      >
        download(file) wrap
      </button>

      {/* download(Blob) — wraps native Blob */}
      <button
        id="test-download-wrap-blob"
        onClick={async () => {
          const dl = await onDownloadWrapBlob()
          const dlMeta = { type: dl.type, size: dl.size }
          const b = await dl.saveToMemory()
          setResult(
            JSON.stringify({
              isBlob: b instanceof Blob,
              size: b.size,
              type: b.type,
              content: await b.text(),
              dl: dlMeta,
            }),
          )
        }}
      >
        download(blob) wrap
      </button>

      {/* progress with known total — result updates live on each tick */}
      <button
        id="test-progress-known"
        onClick={async () => {
          const dl = await onDownloadProgressKnownSize()
          const ticks: { i: number; loaded: number; total: number | undefined }[] = []
          let monotonic = true
          let lastLoaded = -1
          let firstTotal: number | undefined
          const render = (extra: Record<string, unknown> = {}) =>
            setResult(
              JSON.stringify(
                {
                  tickCount: ticks.length,
                  firstTotal,
                  lastLoaded: ticks[ticks.length - 1]?.loaded ?? 0,
                  monotonic,
                  totalsAllEqual: ticks.length > 0 && ticks.every((t) => t.total === ticks[0]!.total),
                  ticks,
                  ...extra,
                },
                null,
                2,
              ),
            )
          dl.onProgress((loaded, total) => {
            if (ticks.length === 0) firstTotal = total
            if (loaded < lastLoaded) monotonic = false
            lastLoaded = loaded
            ticks.push({ i: ticks.length + 1, loaded, total })
            render()
          })
          const f = await dl.saveToMemory()
          render({ done: true, isFile: f instanceof File, size: f.size })
        }}
      >
        download → onProgress (known size)
      </button>

      {/* progress with unknown total — result updates live on each tick */}
      <button
        id="test-progress-unknown"
        onClick={async () => {
          const dl = await onDownloadProgressUnknownSize()
          const ticks: { i: number; loaded: number; total: number | undefined }[] = []
          let monotonic = true
          let lastLoaded = -1
          const render = (extra: Record<string, unknown> = {}) =>
            setResult(
              JSON.stringify(
                {
                  tickCount: ticks.length,
                  firstTotalIsUndefined: ticks[0]?.total === undefined,
                  allTotalsUndefined: ticks.every((t) => t.total === undefined),
                  lastLoaded: ticks[ticks.length - 1]?.loaded ?? 0,
                  monotonic,
                  ticks,
                  ...extra,
                },
                null,
                2,
              ),
            )
          dl.onProgress((loaded, total) => {
            if (loaded < lastLoaded) monotonic = false
            lastLoaded = loaded
            ticks.push({ i: ticks.length + 1, loaded, total })
            render()
          })
          const f = await dl.saveToMemory()
          render({ done: true, isFile: f instanceof File, size: f.size })
        }}
      >
        download → onProgress (no size)
      </button>

      {/* cancel mid-download — result updates live, then settles with rejection */}
      <button
        id="test-progress-cancel"
        onClick={async () => {
          const dl = await onDownloadProgressKnownSize()
          const ticks: { i: number; loaded: number; total: number | undefined }[] = []
          let cancelled = false
          let cancelAfter = 0
          const render = (extra: Record<string, unknown> = {}) =>
            setResult(
              JSON.stringify(
                {
                  tickCount: ticks.length,
                  cancelledAfter: cancelAfter,
                  ticks,
                  ...extra,
                },
                null,
                2,
              ),
            )
          dl.onProgress((loaded, total) => {
            ticks.push({ i: ticks.length + 1, loaded, total })
            render()
            if (!cancelled && loaded > 0) {
              cancelled = true
              cancelAfter = ticks.length
              dl.cancel()
            }
          })
          let error: string | null = null
          let bytes = 0
          try {
            const f = await dl.saveToMemory()
            bytes = f.size
          } catch (e: unknown) {
            error = e instanceof Error ? e.message : String(e)
          }
          render({
            done: true,
            rejected: error !== null,
            errorContains: error?.includes('cancelled') ?? false,
            errorMessage: error,
            bytes,
          })
        }}
      >
        download → cancel mid-flight
      </button>

      {/* binary content with multi-chunk source */}
      <button
        id="test-file-binary"
        onClick={async () => {
          const dl = await onDownloadFileBinary()
          const f = await dl.saveToMemory()
          const ab = await f.arrayBuffer()
          const bytes = new Uint8Array(ab)
          let checksum = 0
          for (const byte of bytes) checksum += byte
          setResult(
            JSON.stringify({
              isFile: f instanceof File,
              name: f.name,
              size: f.size,
              type: f.type,
              byteLength: ab.byteLength,
              checksum,
              firstByte: bytes[0],
              lastByte: bytes[bytes.length - 1],
            }),
          )
        }}
      >
        Binary file
      </button>

      {/* 5 MB file across many chunks */}
      <button
        id="test-file-large"
        onClick={async () => {
          const dl = await onDownloadFileLarge()
          const f = await dl.saveToMemory()
          const ab = await f.arrayBuffer()
          setResult(
            JSON.stringify({
              isFile: f instanceof File,
              name: f.name,
              size: f.size,
              type: f.type,
              byteLength: ab.byteLength,
            }),
          )
        }}
      >
        5 MB file
      </button>

      {/* zero bytes */}
      <button
        id="test-file-empty"
        onClick={async () => {
          const dl = await onDownloadFileEmpty()
          const f = await dl.saveToMemory()
          setResult(
            JSON.stringify({
              isFile: f instanceof File,
              name: f.name,
              size: f.size,
              type: f.type,
              content: await f.text(),
            }),
          )
        }}
      >
        Empty file
      </button>

      {/* nested FileDownload — verifies tuple at non-top-level slot */}
      <button
        id="test-file-nested"
        onClick={async () => {
          const res = await onDownloadFileNested()
          const f = await res.nested.payload.saveToMemory()
          setResult(
            JSON.stringify({
              label: res.label,
              count: res.count,
              isFile: f instanceof File,
              name: f.name,
              size: f.size,
              content: await f.text(),
            }),
          )
        }}
      >
        Nested download
      </button>

      {/* dl.stream() consumes underlying stream — verifies File-like streaming */}
      <button
        id="test-dl-stream"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const isFileLike = typeof dl.stream === 'function' && typeof dl.text === 'function'
          const text = await new Response(dl.stream()).text()
          setResult(
            JSON.stringify({
              isFileLike,
              dlName: dl.name,
              dlType: dl.type,
              dlSize: dl.size,
              streamedText: text,
            }),
          )
        }}
      >
        dl.stream() → Response.text()
      </button>

      {/* dl.text() — convenience that buffers the stream into a string */}
      <button
        id="test-dl-text"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const text = await dl.text()
          setResult(JSON.stringify({ name: dl.name, type: dl.type, text }))
        }}
      >
        dl.text() direct
      </button>

      {/* dl.arrayBuffer() — convenience that buffers the stream into an ArrayBuffer */}
      <button
        id="test-dl-arraybuffer"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const buf = await dl.arrayBuffer()
          const decoded = new TextDecoder().decode(new Uint8Array(buf))
          setResult(JSON.stringify({ byteLength: buf.byteLength, decoded }))
        }}
      >
        dl.arrayBuffer() direct
      </button>

      {/* dl.slice() returns a Blob — slice consumes the parent stream */}
      <button
        id="test-dl-slice"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const slice = dl.slice(7, 13, 'text/x-slice')
          const sliceText = await slice.text()
          setResult(
            JSON.stringify({
              isBlob: slice instanceof Blob,
              sliceSize: slice.size,
              sliceType: slice.type,
              sliceText,
            }),
          )
        }}
      >
        dl.slice() → Blob.text()
      </button>

      {/* One-shot enforcement: calling stream/arrayBuffer/text/saveToMemory twice throws */}
      <button
        id="test-dl-one-shot"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          await dl.text() // first consumer wins
          let secondCallError: string | null = null
          try {
            await dl.text()
          } catch (e: unknown) {
            secondCallError = e instanceof Error ? e.message : String(e)
          }
          setResult(
            JSON.stringify({
              firstCallSucceeded: true,
              secondCallError,
              secondCallRejected: secondCallError !== null,
              errorMentionsConsumed: secondCallError?.toLowerCase().includes('consumed') ?? false,
            }),
          )
        }}
      >
        one-shot enforcement (text twice → throws)
      </button>

      {/* saveToMemory → returns a real File. createObjectURL on the result must work. */}
      <button
        id="test-savetomemory-objecturl"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const f = await dl.saveToMemory()
          const url = URL.createObjectURL(f)
          const fetched = await (await fetch(url)).text()
          URL.revokeObjectURL(url)
          setResult(
            JSON.stringify({
              isRealFile: f instanceof File,
              isRealBlob: f instanceof Blob,
              fName: f.name,
              fSize: f.size,
              fType: f.type,
              fLastModified: f.lastModified,
              createObjectURLWorks: fetched === 'Hello, file download!',
              fetchedText: fetched,
            }),
          )
        }}
      >
        saveToMemory() → URL.createObjectURL works
      </button>

      {/* saveToDisk() → picker (Chromium) or memory-buffer + native trigger (everywhere else).
          Triggers a real download. Manual visual check — file should land in user's Downloads. */}
      <button
        id="test-savetodisk"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const f = await dl.saveToDisk()
          setResult(
            JSON.stringify({
              isRealFile: f instanceof File,
              fName: f.name,
              fSize: f.size,
              fType: f.type,
              note: 'Check picker (Chromium) or Downloads folder (other browsers)',
            }),
          )
        }}
      >
        saveToDisk() → picker / memory-buffer trigger
      </button>

      {/* saveToDisk({ mode: 'memory' }) → force native trigger via in-memory buffer */}
      <button
        id="test-savetodisk-memory"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const f = await dl.saveToDisk({ mode: 'memory' })
          setResult(
            JSON.stringify({
              isRealFile: f instanceof File,
              fName: f.name,
              fSize: f.size,
              fType: f.type,
              note: 'mode:memory — native trigger to Downloads, RAM-buffered',
            }),
          )
        }}
      >
        saveToDisk({'{ mode: "memory" }'}) → Downloads (in-memory buffer)
      </button>

      {/* saveToDisk({ mode: 'opfs' }) → force native trigger via OPFS stage */}
      <button
        id="test-savetodisk-opfs"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const f = await dl.saveToDisk({ mode: 'opfs' })
          setResult(
            JSON.stringify({
              isRealFile: f instanceof File,
              fName: f.name,
              fSize: f.size,
              fType: f.type,
              note: 'mode:opfs — native trigger to Downloads via OPFS stage, no RAM bloat',
            }),
          )
        }}
      >
        saveToDisk({'{ mode: "opfs" }'}) → Downloads (OPFS-staged)
      </button>

      {/* saveToDisk({ mode: 'picker' }) → force picker (errors if unsupported) */}
      <button
        id="test-savetodisk-picker"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          try {
            const f = await dl.saveToDisk({ mode: 'picker' })
            setResult(
              JSON.stringify({
                isRealFile: f instanceof File,
                fName: f.name,
                fSize: f.size,
                fType: f.type,
                note: 'mode:picker — user chose location',
              }),
            )
          } catch (err) {
            setResult(JSON.stringify({ error: err instanceof Error ? err.message : String(err) }))
          }
        }}
      >
        saveToDisk({'{ mode: "picker" }'}) → picker (errors on non-Chromium)
      </button>

      {/* BlobDownload.saveToDisk() → name comes from picker / browser default */}
      <button
        id="test-savetodisk-blob"
        onClick={async () => {
          const dl = await onDownloadBlobStream()
          const f = await dl.saveToDisk()
          setResult(
            JSON.stringify({
              isRealFile: f instanceof File,
              fName: f.name,
              fSize: f.size,
              fType: f.type,
              note: 'Blob has no server-side name — browser/picker assigns one',
            }),
          )
        }}
      >
        BlobDownload.saveToDisk() → picker / Downloads trigger
      </button>

      {/* saveToOpfs(path) → writes to OPFS, returns disk-backed real File */}
      <button
        id="test-savetoopfs"
        onClick={async () => {
          const dl = await onDownloadFileStream()
          const path = `telefunc-test/${Date.now()}-saved.txt`
          const f = await dl.saveToOpfs(path)
          const text = await f.text()
          // Cleanup OPFS
          try {
            const root = await navigator.storage.getDirectory()
            const dir = await root.getDirectoryHandle('telefunc-test')
            await dir.removeEntry(f.name)
          } catch {
            /* ignore cleanup errors */
          }
          setResult(
            JSON.stringify({
              isRealFile: f instanceof File,
              fName: f.name,
              fSize: f.size,
              fType: f.type,
              text,
              path,
            }),
          )
        }}
      >
        saveToOpfs("path") → OPFS-backed File
      </button>

      <pre id="download-result" style={{ maxHeight: 480, overflow: 'auto', fontSize: 11 }}>
        {result}
      </pre>
    </div>
  )
}
