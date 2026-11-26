export { decodeChannelFrame, decodePiecesAck, parseString }

import { parse } from '@brillout/json-serializer/parse'
import { decode, type ChannelFrame, type PiecesAck } from './frame.js'
import { TAG } from './tags.js'

/** `frame` decoded, which a test expects to be a channel's. */
function decodeChannelFrame(frame: Uint8Array): ChannelFrame {
  const decoded = decode(frame)
  if (!('index' in decoded)) throw new Error(`Expected a channel's frame, got tag ${decoded.tag}`)
  return decoded
}

/** `frame` decoded, which a test expects to be a `PIECES_ACK`. */
function decodePiecesAck(frame: Uint8Array): PiecesAck {
  const decoded = decode(frame)
  if (decoded.tag !== TAG.PIECES_ACK) throw new Error(`Expected a PIECES_ACK, got tag ${decoded.tag}`)
  return decoded
}

/** `text` parsed, which a test expects to be a string. */
function parseString(text: string): string {
  const value = parse(text)
  if (typeof value !== 'string') throw new Error(`Expected a string, got ${text}`)
  return value
}
