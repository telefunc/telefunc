export {
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
}

import { sleep } from '../../sleep'

const onUploadFile = async (file: File, description: string) => {
  const content = await file.text()
  return {
    fileName: file.name,
    fileSize: file.size,
    fileType: file.type,
    content,
    description,
  }
}

const onUploadMultiple = async (file1: File, file2: File) => {
  const text1 = await file1.text()
  const text2 = await file2.text()
  return {
    file1: { name: file1.name, content: text1 },
    file2: { name: file2.name, content: text2 },
  }
}

const onUploadArray = async (files: File[]) => {
  const results = []
  for (const file of files) {
    results.push({ name: file.name, content: await file.text() })
  }
  return results
}

const onUploadStream = async (file: File) => {
  let totalBytes = 0
  let chunkCount = 0
  for await (const chunk of file.stream()) {
    totalBytes += chunk.byteLength
    chunkCount++
  }
  return { totalBytes, chunkCount }
}

const onUploadArrayBuffer = async (file: File) => {
  const ab = await file.arrayBuffer()
  return { content: new TextDecoder().decode(ab), byteLength: ab.byteLength }
}

// Reading the same file twice should throw (one-shot consumption)
const onReadTwice = async (file: File) => {
  await file.text()
  try {
    await file.text()
    return { error: null }
  } catch (e: any) {
    return { error: e.message || String(e) }
  }
}

// Slow consumer — delays 50ms per chunk to test backpressure
const onUploadBackpressure = async (file: File) => {
  let totalBytes = 0
  let chunkCount = 0
  const start = Date.now()
  for await (const chunk of file.stream()) {
    totalBytes += chunk.byteLength
    chunkCount++
    await sleep(50)
  }
  const elapsed = Date.now() - start
  return { totalBytes, chunkCount, elapsed }
}

const onUploadSlice = async (file: File) => {
  const slice = file.slice(0, 5)
  const content = await slice.text()
  return { content, sliceSize: slice.size, originalSize: file.size }
}

// Reading file2 first (out of order) drains file1 — it becomes unreadable
const onUploadOutOfOrder = async (file1: File, file2: File) => {
  const text2 = await file2.text()
  try {
    await file1.text()
    return { text2, file1Error: null }
  } catch (e: any) {
    return { text2, file1Error: e.message || String(e) }
  }
}

const onUploadEmpty = async (file: File) => {
  const content = await file.text()
  return { name: file.name, size: file.size, content, isEmpty: content === '' }
}

const onUploadManyFiles = async (files: File[]) => {
  const results = []
  for (const file of files) {
    results.push({ name: file.name, content: await file.text() })
  }
  return { count: results.length, results }
}

const onUploadBinary = async (file: File) => {
  const ab = await file.arrayBuffer()
  const bytes = new Uint8Array(ab)
  let sum = 0
  for (const b of bytes) sum += b
  return { byteLength: ab.byteLength, checksum: sum }
}

// Mixed args: file, text, number, deeply nested file, boolean
const onUploadMixed = async (
  file1: File,
  label: string,
  count: number,
  nested: { deep: { file: File; tags: string[] } },
  flag: boolean,
) => {
  const text1 = await file1.text()
  const text2 = await nested.deep.file.text()
  return {
    text1,
    label,
    count,
    text2,
    tags: nested.deep.tags,
    flag,
    name1: file1.name,
    name2: nested.deep.file.name,
  }
}

const onUploadLarge = async (file: File) => {
  let totalBytes = 0
  let chunkCount = 0
  for await (const chunk of file.stream()) {
    totalBytes += chunk.byteLength
    chunkCount++
  }
  return { totalBytes, chunkCount, name: file.name, size: file.size }
}

const onUploadSliceMiddle = async (file: File) => {
  const slice = file.slice(3, 7)
  const content = await slice.text()
  return { content, sliceSize: slice.size, originalSize: file.size }
}

const onUploadSliceNegative = async (file: File) => {
  const slice = file.slice(-4)
  const content = await slice.text()
  return { content, sliceSize: slice.size, originalSize: file.size }
}

const onUploadSliceEmpty = async (file: File) => {
  const slice = file.slice(0, 0)
  const content = await slice.text()
  return { content, sliceSize: slice.size, originalSize: file.size }
}

const onUploadConcurrent = async (file: File, id: number) => {
  const content = await file.text()
  return { id, content, name: file.name }
}

const onUploadProps = async (file: File) => {
  return {
    name: file.name,
    size: file.size,
    type: file.type,
    lastModified: file.lastModified,
  }
}

// Files sent but never read on the server — should not hang or error
const onUploadIgnored = async (_file1: File, _file2: File) => {
  return { ok: true }
}
