export { FileDownload }
export { BlobDownload }
export { isFileDownload }
export { isBlobDownload }

import { LazyBlob, LazyFile } from '../../LazyFile.js'
import type { StreamSource, FileDownloadMetadata, BlobDownloadMetadata, DownloadProgress } from '../../types.js'

const FILE_DOWNLOAD_BRAND = Symbol.for('telefunc.client.FileDownload')
const BLOB_DOWNLOAD_BRAND = Symbol.for('telefunc.client.BlobDownload')

function isFileDownload(value: unknown): value is FileDownload {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as { [FILE_DOWNLOAD_BRAND]?: true })[FILE_DOWNLOAD_BRAND] === true
  )
}

function isBlobDownload(value: unknown): value is BlobDownload {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as { [BLOB_DOWNLOAD_BRAND]?: true })[BLOB_DOWNLOAD_BRAND] === true
  )
}

/** File-shaped streaming download. Inherits Blob/File methods (`stream`/`arrayBuffer`/
 *  `text`/`slice`) from `LazyFile`; overrides `bytes` to use the StreamSource's built-in
 *  onChunk hook for progress + cancel detection. */
class FileDownload extends LazyFile {
  readonly [FILE_DOWNLOAD_BRAND] = true
  readonly #core: DownloadCore

  constructor(metadata: FileDownloadMetadata, source: StreamSource) {
    super(metadata.size ?? 0, metadata.type, metadata.name, metadata.lastModified, source.stream)
    this.#core = new DownloadCore(source, metadata.size, this.name, this.type, () => this._markConsumed())
  }

  get loaded(): number {
    return this.#core.state.loaded
  }
  onProgress(cb: DownloadProgress): void {
    this.#core.onProgress(cb)
  }
  cancel(): void {
    this.#core.cancel()
  }
  override stream(): ReadableStream<Uint8Array<ArrayBuffer>> {
    return this.#core.stream()
  }
  /** Saves to user's filesystem. `opts.mode` selects the path:
   *  - `'picker'` — opens [`showSaveFilePicker`](https://developer.mozilla.org/en-US/docs/Web/API/Window/showSaveFilePicker)
   *    so the user chooses the location (throws if not supported).
   *  - `'memory'` — buffers in RAM, then triggers a native browser download (Downloads folder).
   *  - `'opfs'` — streams via browser-private storage, then triggers a native download (for
   *    huge files where memory buffering isn't viable).
   *
   *  Default: `'picker'` if `showSaveFilePicker` is available, else `'memory'`. */
  saveToDisk(opts?: { mode?: 'picker' | 'memory' | 'opfs' }): Promise<File> {
    return this.#core.saveToDisk(opts)
  }
  /** Streams into OPFS (browser-private), returns a disk-backed `File` for `URL.createObjectURL` etc. */
  saveToOpfs(path?: string): Promise<File> {
    return this.#core.saveToOpfs(path)
  }
  /** Buffers all bytes into memory, returns a real `File`. */
  async saveToMemory(): Promise<File> {
    const buf = await this.bytes()
    return new File([buf.buffer], this.name, { type: this.type, lastModified: this.lastModified })
  }
}

/** Blob-shaped streaming download. Same shape as FileDownload minus name/lastModified. */
class BlobDownload extends LazyBlob {
  readonly [BLOB_DOWNLOAD_BRAND] = true
  readonly #core: DownloadCore

  constructor(metadata: BlobDownloadMetadata, source: StreamSource) {
    super(metadata.size ?? 0, metadata.type, source.stream)
    this.#core = new DownloadCore(source, metadata.size, '', this.type, () => this._markConsumed())
  }

  get loaded(): number {
    return this.#core.state.loaded
  }
  onProgress(cb: DownloadProgress): void {
    this.#core.onProgress(cb)
  }
  cancel(): void {
    this.#core.cancel()
  }
  override stream(): ReadableStream<Uint8Array<ArrayBuffer>> {
    return this.#core.stream()
  }
  /** Saves to user's filesystem. `opts.mode` selects the path:
   *  - `'picker'` — opens [`showSaveFilePicker`](https://developer.mozilla.org/en-US/docs/Web/API/Window/showSaveFilePicker)
   *    so the user chooses the location (throws if not supported).
   *  - `'memory'` — buffers in RAM, then triggers a native browser download (Downloads folder).
   *  - `'opfs'` — streams via browser-private storage, then triggers a native download (for
   *    huge files where memory buffering isn't viable).
   *
   *  Default: `'picker'` if `showSaveFilePicker` is available, else `'memory'`. */
  saveToDisk(opts?: { mode?: 'picker' | 'memory' | 'opfs' }): Promise<File> {
    return this.#core.saveToDisk(opts)
  }
  /** Streams into OPFS (browser-private), returns a disk-backed `File` for `URL.createObjectURL` etc. */
  saveToOpfs(path?: string): Promise<File> {
    return this.#core.saveToOpfs(path)
  }
  /** Buffers all bytes into memory, returns a real `Blob`. */
  async saveToMemory(): Promise<Blob> {
    const buf = await this.bytes()
    return new Blob([buf.buffer], { type: this.type })
  }
}

// ─── Shared download semantics ─────────────────────────────────────────────────────

/** Holds the state, source, and methods shared by FileDownload and BlobDownload —
 *  everything except `saveToMemory` (return type differs) and the brand. */
class DownloadCore {
  readonly state: ProgressState
  readonly #source: StreamSource
  readonly #name: string
  readonly #type: string
  readonly #markConsumed: () => void

  constructor(source: StreamSource, total: number | undefined, name: string, type: string, markConsumed: () => void) {
    this.#source = source
    this.#name = name
    this.#type = type
    this.#markConsumed = markConsumed
    this.state = makeState(total)
  }

  onProgress(cb: DownloadProgress): void {
    this.state.callbacks.push(cb)
    if (this.state.loaded > 0) cb(this.state.loaded, this.state.total)
  }

  cancel(): void {
    this.#source.cancel()
  }

  /** Returns the underlying source's instrumented stream — fires progress + errors on
   *  cancel/truncation. Inherited `bytes`/`text`/`arrayBuffer` flow through this. */
  stream(): ReadableStream<Uint8Array<ArrayBuffer>> {
    this.#markConsumed()
    return this.#source.stream({ expectedSize: this.state.total, onChunk: (n) => this.state.fire(n) })
  }

  async saveToDisk(opts: { mode?: 'picker' | 'memory' | 'opfs' } = {}): Promise<File> {
    this.#markConsumed()
    const showPicker = getShowSaveFilePicker()
    const mode = opts.mode ?? (showPicker ? 'picker' : 'memory')
    if (mode === 'picker') {
      if (!showPicker) {
        throw new Error('saveToDisk({ mode: "picker" }) requires window.showSaveFilePicker')
      }
      const handle = await openPickerHandle(showPicker, this.#name, this.#type)
      return pipeAndGetFile(this.#source, this.state, handle)
    }
    if (mode === 'opfs') {
      return streamToOpfsThenTrigger(this.#source, this.state, this.#name)
    }
    return bufferThenTrigger(this.#source, this.state, this.#name, this.#type)
  }

  async saveToOpfs(path?: string): Promise<File> {
    this.#markConsumed()
    const finalPath = path ?? `__telefunc_opfs/${crypto.randomUUID()}${this.#name ? `-${this.#name}` : ''}`
    const handle = await openOpfsHandle(finalPath)
    return pipeAndGetFile(this.#source, this.state, handle)
  }
}

// ─── Progress state ────────────────────────────────────────────────────────────────

type ProgressState = {
  loaded: number
  total: number | undefined
  callbacks: DownloadProgress[]
  fire(chunkSize: number): void
}

function makeState(total: number | undefined): ProgressState {
  const state: ProgressState = {
    loaded: 0,
    total,
    callbacks: [],
    fire(chunkSize) {
      state.loaded += chunkSize
      for (const cb of state.callbacks) cb(state.loaded, total)
    },
  }
  return state
}

// ─── File-handle helpers ───────────────────────────────────────────────────────────

async function openOpfsHandle(path: string): Promise<FileSystemFileHandle> {
  const root = await navigator.storage.getDirectory()
  const segments = path.split('/').filter(Boolean)
  const filename = segments.pop()
  if (!filename) throw new Error(`saveToOpfs(path): path must end in a file name (got '${path}')`)
  let dir: FileSystemDirectoryHandle = root
  for (const seg of segments) {
    dir = await dir.getDirectoryHandle(seg, { create: true })
  }
  return dir.getFileHandle(filename, { create: true })
}

type ShowSaveFilePicker = (opts?: {
  suggestedName?: string
  types?: Array<{ description?: string; accept: Record<string, string[]> }>
}) => Promise<FileSystemFileHandle>

function getShowSaveFilePicker(): ShowSaveFilePicker | undefined {
  return (window as Window & { showSaveFilePicker?: ShowSaveFilePicker }).showSaveFilePicker
}

async function openPickerHandle(
  showPicker: ShowSaveFilePicker,
  suggestedName: string,
  type: string,
): Promise<FileSystemFileHandle> {
  return showPicker({
    suggestedName,
    ...(type ? { types: [{ accept: { [type]: [] } }] } : {}),
  })
}

// ─── Pipe + cross-browser download trigger ─────────────────────────────────────────

/** Pipes the source's instrumented stream to a writable file handle, returns the
 *  resulting disk-backed `File`. `pipeTo` auto-aborts the writable if the source errors
 *  (consumer-cancel mid-flight or size mismatch — the source's `stream(opts)` errors
 *  the stream) so no partial file is left on disk. */
async function pipeAndGetFile(source: StreamSource, state: ProgressState, handle: FileSystemFileHandle): Promise<File> {
  const writable = await handle.createWritable()
  await source.stream({ expectedSize: state.total, onChunk: (n) => state.fire(n) }).pipeTo(writable)
  return handle.getFile()
}

const TEMP_DIR = '__telefunc_temp'
const TEMP_RETENTION_MS = 60_000

/** Buffers the source fully into memory, builds a File from the bytes, triggers a native
 *  browser download via blob URL. Default `saveToDisk` fallback when no picker — RAM-bounded
 *  but no OPFS leak risk. */
async function bufferThenTrigger(
  source: StreamSource,
  state: ProgressState,
  filename: string,
  type: string,
): Promise<File> {
  const bytes = await source.bytes({ expectedSize: state.total, onChunk: (n) => state.fire(n) })
  const file = new File([bytes.buffer], filename, { type })
  triggerDownload(file, filename)
  return file
}

function triggerDownload(file: File, filename: string): void {
  const url = URL.createObjectURL(file)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  setTimeout(() => URL.revokeObjectURL(url), TEMP_RETENTION_MS)
}

/** Streams `source` into a temp OPFS file, triggers a native browser download via blob URL,
 *  schedules per-temp cleanup. Cross-browser fallback when `showSaveFilePicker` isn't
 *  available. Returns the OPFS-backed `File` (valid until the cleanup timer fires —
 *  enough for the browser to consume the blob URL).
 *
 *  Cleanup is per-temp only (no directory-wide wipe) — multiple tabs may have in-flight
 *  downloads under the same `TEMP_DIR`, and a wipe would yank the rug from a sibling tab.
 *  Cost: a tab crash before the timer fires leaks one OPFS file until storage-quota
 *  eviction. The leak is browser-private and bounded. */
async function streamToOpfsThenTrigger(source: StreamSource, state: ProgressState, filename: string): Promise<File> {
  const tempName = `${crypto.randomUUID()}-${filename || 'download'}`
  const handle = await openOpfsHandle(`${TEMP_DIR}/${tempName}`)
  let file: File
  try {
    file = await pipeAndGetFile(source, state, handle)
  } catch (err) {
    // Pipe failed (cancel/truncate/source error) — getFileHandle already created an empty
    // entry on disk; remove it so we don't leak. Fire-and-forget; the original error wins.
    void removeOpfsTemp(tempName)
    throw err
  }
  triggerDownload(file, filename)
  // Per-temp cleanup after the browser has had time to consume the blob URL — see file
  // header for the multi-tab safety rationale (no directory-wide wipe).
  setTimeout(() => void removeOpfsTemp(tempName), TEMP_RETENTION_MS)
  return file
}

async function removeOpfsTemp(tempName: string): Promise<void> {
  try {
    const root = await navigator.storage.getDirectory()
    const dir = await root.getDirectoryHandle(TEMP_DIR)
    await dir.removeEntry(tempName)
  } catch {
    // Best-effort cleanup.
  }
}
