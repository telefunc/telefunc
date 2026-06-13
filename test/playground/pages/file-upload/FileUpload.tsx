export { FileUpload }

import React, { useEffect, useState } from 'react'
import {
  onUploadFile,
  onUploadMultiple,
  onUploadArray,
  onUploadStream,
  onUploadArrayBuffer,
  onReadTwice,
  onUploadOutOfOrder,
  onUploadBackpressure,
  onUploadSlice,
  onUploadEmpty,
  onUploadManyFiles,
  onUploadBinary,
  onUploadMixed,
  onUploadLarge,
  onUploadSliceMiddle,
  onUploadSliceNegative,
  onUploadSliceEmpty,
  onUploadConcurrent,
  onUploadProps,
  onUploadIgnored,
} from './FileUpload.telefunc'

function FileUpload() {
  const [result, setResult] = useState<string>('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  const file = (name: string, content: string) => new File([content], name, { type: 'text/plain' })

  return (
    <div>
      <h2>File Upload Tests</h2>
      {hydrated && <span id="hydrated" />}

      <button
        id="test-single"
        onClick={async () => {
          const res = await onUploadFile(file('test.txt', 'hello'), 'desc1')
          setResult(JSON.stringify(res))
        }}
      >
        Single file
      </button>

      <button
        id="test-multiple"
        onClick={async () => {
          const res = await onUploadMultiple(file('a.txt', 'aaa'), file('b.txt', 'bbb'))
          setResult(JSON.stringify(res))
        }}
      >
        Multiple files
      </button>

      <button
        id="test-array"
        onClick={async () => {
          const res = await onUploadArray([file('x.txt', 'xxx'), file('y.txt', 'yyy'), file('z.txt', 'zzz')])
          setResult(JSON.stringify(res))
        }}
      >
        File[] array
      </button>

      <button
        id="test-stream"
        onClick={async () => {
          const res = await onUploadStream(file('s.txt', 'streamed'.repeat(100_000)))
          setResult(JSON.stringify(res))
        }}
      >
        file.stream()
      </button>

      <button
        id="test-arraybuffer"
        onClick={async () => {
          const res = await onUploadArrayBuffer(file('ab.txt', 'buffered'))
          setResult(JSON.stringify(res))
        }}
      >
        file.arrayBuffer()
      </button>

      <button
        id="test-read-twice"
        onClick={async () => {
          const res = await onReadTwice(file('twice.txt', 'data'))
          setResult(JSON.stringify(res))
        }}
      >
        Read twice
      </button>

      <button
        id="test-backpressure"
        onClick={async () => {
          const res = await onUploadBackpressure(file('bp.txt', 'x'.repeat(800_000)))
          setResult(JSON.stringify(res))
        }}
      >
        Backpressure
      </button>

      <button
        id="test-slice"
        onClick={async () => {
          const res = await onUploadSlice(file('slice.txt', 'hello world'))
          setResult(JSON.stringify(res))
        }}
      >
        file.slice()
      </button>

      <button
        id="test-out-of-order"
        onClick={async () => {
          const res = await onUploadOutOfOrder(file('first.txt', 'AAA'), file('second.txt', 'BBB'))
          setResult(JSON.stringify(res))
        }}
      >
        Out of order
      </button>

      <button
        id="test-empty"
        onClick={async () => {
          const res = await onUploadEmpty(new File([], 'empty.txt', { type: 'text/plain' }))
          setResult(JSON.stringify(res))
        }}
      >
        Empty file
      </button>

      <button
        id="test-many-files"
        onClick={async () => {
          const files = Array.from({ length: 20 }, (_, i) => file(`file${i}.txt`, `content${i}`))
          const res = await onUploadManyFiles(files)
          setResult(JSON.stringify(res))
        }}
      >
        20 files
      </button>

      <button
        id="test-binary"
        onClick={async () => {
          // Create binary content: bytes 0-255 repeated
          const bytes = new Uint8Array(1024)
          for (let i = 0; i < bytes.length; i++) bytes[i] = i % 256
          const res = await onUploadBinary(new File([bytes], 'binary.bin', { type: 'application/octet-stream' }))
          setResult(JSON.stringify(res))
        }}
      >
        Binary
      </button>

      <button
        id="test-mixed"
        onClick={async () => {
          const res = await onUploadMixed(
            file('m1.txt', 'hello'),
            'my-label',
            42,
            { deep: { file: file('m2.txt', 'world'), tags: ['a', 'b'] } },
            true,
          )
          setResult(JSON.stringify(res))
        }}
      >
        Mixed args
      </button>

      <button
        id="test-large"
        onClick={async () => {
          // 5MB file
          const content = 'X'.repeat(5 * 1024 * 1024)
          const res = await onUploadLarge(new File([content], 'large.bin', { type: 'application/octet-stream' }))
          setResult(JSON.stringify(res))
        }}
      >
        5MB file
      </button>

      <button
        id="test-slice-middle"
        onClick={async () => {
          const res = await onUploadSliceMiddle(file('mid.txt', 'abcdefghij'))
          setResult(JSON.stringify(res))
        }}
      >
        Slice middle
      </button>

      <button
        id="test-slice-negative"
        onClick={async () => {
          const res = await onUploadSliceNegative(file('neg.txt', 'abcdefghij'))
          setResult(JSON.stringify(res))
        }}
      >
        Slice negative
      </button>

      <button
        id="test-slice-empty"
        onClick={async () => {
          const res = await onUploadSliceEmpty(file('se.txt', 'abcdefghij'))
          setResult(JSON.stringify(res))
        }}
      >
        Slice empty
      </button>

      <button
        id="test-concurrent"
        onClick={async () => {
          // Fire 5 concurrent uploads
          const results = await Promise.all(
            Array.from({ length: 5 }, (_, i) => onUploadConcurrent(file(`c${i}.txt`, `data${i}`), i)),
          )
          setResult(JSON.stringify(results))
        }}
      >
        Concurrent
      </button>

      <button
        id="test-props"
        onClick={async () => {
          const f = new File(['hello'], 'props.txt', { type: 'text/plain', lastModified: 1700000000000 })
          const res = await onUploadProps(f)
          setResult(JSON.stringify(res))
        }}
      >
        Props round-trip
      </button>

      <button
        id="test-ignored"
        onClick={async () => {
          const res = await onUploadIgnored(file('ig1.txt', 'ignored1'), file('ig2.txt', 'ignored2'))
          setResult(JSON.stringify(res))
        }}
      >
        Ignored files
      </button>

      <pre id="upload-result">{result}</pre>
    </div>
  )
}
