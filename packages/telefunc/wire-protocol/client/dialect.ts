export { clientDialect }

import { asyncGeneratorReviver } from './response/async-generator.js'
import { readableStreamReviver } from './response/readable-stream.js'
import { fileReviver } from './response/file.js'
import { blobReviver } from './response/blob.js'
import { fileDownloadReviver } from './response/fileDownload.js'
import { blobDownloadReviver } from './response/blobDownload.js'
import { promiseReviver } from './response/promise.js'
import { broadcastReviver } from './response/broadcast.js'
import { channelReviver } from './response/channel.js'
import { functionReviver } from './response/function.js'
import { roomReviver, roomParticipantReviver, roomRemoteReviver } from '../room/response-client.js'

const clientDialect = Object.freeze([
  asyncGeneratorReviver,
  readableStreamReviver,
  fileReviver,
  blobReviver,
  fileDownloadReviver,
  blobDownloadReviver,
  promiseReviver,
  roomReviver,
  roomParticipantReviver,
  roomRemoteReviver,
  broadcastReviver,
  channelReviver,
  functionReviver,
])
