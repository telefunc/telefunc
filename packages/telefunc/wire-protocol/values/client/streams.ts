export { responseTypes, createBodyReader }
export { FrameDemuxer } from './response/FrameDemuxer.js'

import type { FeatureResponseTypes } from './response/registry.js'
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
import {
  SERIALIZER_PREFIX_GENERATOR,
  SERIALIZER_PREFIX_STREAM,
  SERIALIZER_PREFIX_FILE,
  SERIALIZER_PREFIX_BLOB,
  SERIALIZER_PREFIX_FILE_DOWNLOAD,
  SERIALIZER_PREFIX_BLOB_DOWNLOAD,
  SERIALIZER_PREFIX_PROMISE,
} from '../../constants.js'

const responseTypes: FeatureResponseTypes<'streams'> = {
  [SERIALIZER_PREFIX_GENERATOR]: asyncGeneratorReviver,
  [SERIALIZER_PREFIX_STREAM]: readableStreamReviver,
  [SERIALIZER_PREFIX_FILE]: fileReviver,
  [SERIALIZER_PREFIX_BLOB]: blobReviver,
  [SERIALIZER_PREFIX_FILE_DOWNLOAD]: fileDownloadReviver,
  [SERIALIZER_PREFIX_BLOB_DOWNLOAD]: blobDownloadReviver,
  [SERIALIZER_PREFIX_PROMISE]: promiseReviver,
}

function createBodyReader(
  reader: ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>,
  isSse: boolean,
  call: { telefunctionName: string; telefuncFilePath: string; abortController: AbortController },
): BaseStreamReader {
  return isSse ? new SseStreamReader(reader, call) : new StreamReader(reader, call)
}
