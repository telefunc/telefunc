export { responseTypes, createBodyReader }
export { FrameDemuxer } from './response/FrameDemuxer.js'

import type { ClientReviverContext, ReviverType, TypeContract } from '../types.js'
import type { BaseStreamReader } from './response/BaseStreamReader.js'
import { StreamReader } from './response/StreamReader.js'
import { SseStreamReader } from './response/SseStreamReader.js'
import { asyncGeneratorReviver } from './response/asyncGenerator.js'
import { readableStreamReviver } from './response/readableStream.js'
import { fileReviver } from './response/file.js'
import { blobReviver } from './response/blob.js'
import { fileDownloadReviver } from './response/fileDownload.js'
import { blobDownloadReviver } from './response/blobDownload.js'
import { promiseReviver } from './response/promise.js'

const responseTypes: ReviverType<TypeContract, ClientReviverContext>[] = [
  asyncGeneratorReviver,
  readableStreamReviver,
  fileReviver,
  blobReviver,
  fileDownloadReviver,
  blobDownloadReviver,
  promiseReviver,
]

function createBodyReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  isSse: boolean,
  call: { telefunctionName: string; telefuncFilePath: string; abortController: AbortController },
): BaseStreamReader {
  return isSse ? new SseStreamReader(reader, call) : new StreamReader(reader, call)
}
