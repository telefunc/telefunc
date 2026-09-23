export {
  decodeRoomRequest,
  decodeRoomDeclaration,
  decodeRoomPublish,
  decodeStubBinaryFrame,
  decodeParticipantRequest,
  decodeParticipantFrame,
}
export type { RoomRequest, RoomDeclaration }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { ProtocolViolationError } from '../../shared-ws.js'
import { sanitizeBinaryWants, unframeMemberId, uuidToBytes, type BinaryFrame } from '../binary.js'
import { isRecord } from '../model.js'
import { decodeDmReply, type ParticipantStubRequest, type RoomDataPublish, type RoomStubRequest } from '../protocol.js'
assertIsNotBrowser()

type RoomRequest = Extract<
  RoomStubRequest,
  { __r: 'req-join' | 'req-leave' | 'req-set-meta' | 'req-set-attrs' | 'req-dm' }
>
type RoomDeclaration = Exclude<RoomStubRequest, RoomRequest>

// A stub's client library sends only these shapes, so anything else comes from a broken or hostile peer and ends its connection.
function malformed(what: string): never {
  throw new ProtocolViolationError(`malformed Room ${what}`)
}
function record(value: unknown, what: string): Record<string, unknown> {
  if (!isRecord(value)) malformed(what)
  return value
}
function text(value: unknown, what: string): string {
  if (typeof value !== 'string') malformed(what)
  return value
}
function memberId(value: unknown, what: string): string {
  if (typeof value !== 'string' || uuidToBytes(value) === null) malformed(what)
  return value
}
function flag(value: unknown, what: string): boolean {
  if (typeof value !== 'boolean') malformed(what)
  return value
}
function optionalTrue(value: unknown, what: string): boolean {
  if (value !== undefined && value !== true) malformed(what)
  return value === true
}

function decodeRoomRequest(value: unknown): RoomRequest {
  const req = record(value, 'request')
  switch (req.__r) {
    case 'req-join':
      return { __r: 'req-join', meta: record(req.meta, 'join meta'), selfDelivery: flag(req.selfDelivery, 'join') }
    case 'req-leave':
      return { __r: 'req-leave', id: memberId(req.id, 'leave') }
    case 'req-set-meta':
      return { __r: 'req-set-meta', id: memberId(req.id, 'setMeta'), meta: record(req.meta, 'setMeta meta') }
    case 'req-set-attrs':
      return { __r: 'req-set-attrs', id: memberId(req.id, 'setAttributes'), attrs: record(req.attrs, 'attributes') }
    case 'req-dm':
      return {
        __r: 'req-dm',
        id: memberId(req.id, 'send'),
        to: text(req.to, 'send recipient'),
        data: req.data,
        ...(optionalTrue(req.ack, 'send ack') ? { ack: true } : {}),
      }
  }
  return malformed('request')
}

function decodeRoomDeclaration(value: unknown): RoomDeclaration {
  const decl = record(value, 'declaration')
  switch (decl.__r) {
    case 'dm-reply':
      return {
        __r: 'dm-reply',
        id: memberId(decl.id, 'DM reply'),
        ackId: text(decl.ackId, 'DM reply'),
        reply: decodeDmReply(decl.reply) ?? malformed('DM reply'),
      }
    case 'sub-binary':
      return { __r: 'sub-binary', wants: sanitizeBinaryWants(decl.wants) ?? malformed('binary wants') }
    case 'sub-text': {
      if (!Array.isArray(decl.members)) malformed('text wants')
      return {
        __r: 'sub-text',
        members: decl.members.map((member) => memberId(member, 'text wants')),
        announce: flag(decl.announce, 'text wants'),
      }
    }
  }
  return malformed('declaration')
}

function decodeRoomPublish(value: unknown): RoomDataPublish {
  const publish = record(value, 'publish')
  if (publish.__r !== 'data') malformed('publish')
  return {
    __r: 'data',
    from: memberId(publish.from, 'publish'),
    data: publish.data,
    ...(optionalTrue(publish.retain, 'publish retain') ? { retain: true } : {}),
  }
}

function decodeStubBinaryFrame(framed: Uint8Array): BinaryFrame {
  return unframeMemberId(framed) ?? malformed('binary frame')
}

function decodeParticipantFrame(framed: Uint8Array, participantId: string): BinaryFrame {
  const frame = decodeStubBinaryFrame(framed)
  if (frame.from !== participantId) malformed('binary frame sender')
  return frame
}

function decodeParticipantRequest(value: unknown): ParticipantStubRequest {
  const req = record(value, 'participant request')
  switch (req.__r) {
    case 'req-publish':
      return {
        __r: 'req-publish',
        data: req.data,
        ...(optionalTrue(req.retain, 'publish retain') ? { retain: true } : {}),
      }
    case 'req-set-meta':
      return { __r: 'req-set-meta', meta: record(req.meta, 'setMeta meta') }
    case 'req-set-attrs':
      return { __r: 'req-set-attrs', attrs: record(req.attrs, 'attributes') }
    case 'req-dm':
      return {
        __r: 'req-dm',
        to: text(req.to, 'send recipient'),
        data: req.data,
        ...(optionalTrue(req.ack, 'send ack') ? { ack: true } : {}),
      }
    case 'req-leave':
      return { __r: 'req-leave' }
  }
  return malformed('participant request')
}
