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
          const f = await dl.file
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
          const f = await dl.file
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
          const b = await dl.blob
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
          const b = await dl.blob
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
          const f = await dl.file
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
          const b = await dl.blob
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
          const f = await dl.file
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
          const f = await dl.file
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
            const f = await dl.file
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
          const f = await dl.file
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
          const f = await dl.file
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
          const f = await dl.file
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
          const f = await res.nested.payload.file
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

      <pre id="download-result" style={{ maxHeight: 480, overflow: 'auto', fontSize: 11 }}>
        {result}
      </pre>
    </div>
  )
}
