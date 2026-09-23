export { serverDialect }

import { asyncGeneratorReplacer } from './response/async-generator.js'
import { readableStreamReplacer } from './response/readable-stream.js'
import { fileDownloadReplacer } from './response/fileDownload.js'
import { blobDownloadReplacer } from './response/blobDownload.js'
import { fileReplacer } from './response/file.js'
import { blobReplacer } from './response/blob.js'
import { promiseReplacer } from './response/promise.js'
import { broadcastReplacer } from './response/broadcast.js'
import { channelReplacer } from './response/channel.js'
import { functionReplacer } from './response/function.js'
import { roomReplacer, roomParticipantReplacer, roomRemoteReplacer } from '../room/response-server.js'

// Order: fileDownload/blobDownload before file/blob (brand-checked vs instanceof);
// file before blob (File extends Blob); broadcast before channel (Broadcast extends Channel).
const serverDialect = Object.freeze([
  asyncGeneratorReplacer,
  readableStreamReplacer,
  fileDownloadReplacer,
  blobDownloadReplacer,
  fileReplacer,
  blobReplacer,
  promiseReplacer,
  roomReplacer,
  roomParticipantReplacer,
  roomRemoteReplacer,
  broadcastReplacer,
  channelReplacer,
  functionReplacer,
])
