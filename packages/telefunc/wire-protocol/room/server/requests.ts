export {
  isRoomRequest,
  decodeRoomRequest,
  decodeRoomDeclaration,
  decodeRoomPublish,
  decodeStubBinaryFrame,
  decodeParticipantRequest,
  decodeParticipantFrame,
  decodeParticipantDeclaration,
}
export type { RoomRequest, RoomDeclaration }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { ProtocolViolationError } from '../../wire/connectionFrame.js'
import { ALL, decodeBinaryFrame, isMemberId, isRoomTrack, type BinaryFrame, type BinaryWantChange } from '../binary.js'
import { isRecord } from '../model.js'
import type {
  DmReply,
  MemberMutationRequest,
  ParticipantStubRequest,
  RoomDataPublish,
  RoomStubRequest,
} from '../protocol.js'
assertIsNotBrowser()

type RoomRequest = Extract<
  RoomStubRequest,
  { __r: 'req-join' | 'req-leave' | 'req-set-meta' | 'req-set-attrs' | 'req-dm' }
>

type RoomDeclaration =
  | Exclude<RoomStubRequest, RoomRequest | { __r: 'sub-binary' }>
  | ({ __r: 'sub-binary' } & BinaryWantChange)

/** A member's meta, attributes and DM requests, by the API call each serves. */
const MEMBER_MUTATIONS = { 'req-set-meta': 'setMeta', 'req-set-attrs': 'setAttributes', 'req-dm': 'send' } as const

/** A stub's client library sends only these shapes, so anything else comes from a broken or hostile peer
 *  and ends its connection. */
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
  if (!isMemberId(value)) malformed(what)
  return value
}

function flag(value: unknown, what: string): boolean {
  if (typeof value !== 'boolean') malformed(what)
  return value
}

function integer(value: unknown, what: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) malformed(what)
  return value
}

function optionalTrue(value: unknown, what: string): boolean {
  if (value !== undefined && value !== true) malformed(what)
  return value === true
}

/** A stub's requests and declarations share its channel: a request is tagged `req-`. */
function isRoomRequest(value: unknown): boolean {
  return isRecord(value) && typeof value.__r === 'string' && value.__r.startsWith('req-')
}

function decodeRoomRequest(value: unknown): RoomRequest {
  const req = record(value, 'request')
  switch (req.__r) {
    case 'req-join':
      return {
        __r: 'req-join',
        reqId: integer(req.reqId, 'join'),
        meta: record(req.meta, 'join meta'),
        selfDelivery: flag(req.selfDelivery, 'join'),
      }
    case 'req-leave':
      return { __r: 'req-leave', id: memberId(req.id, 'leave') }
    case 'req-set-meta':
    case 'req-set-attrs':
    case 'req-dm': {
      const id = memberId(req.id, MEMBER_MUTATIONS[req.__r])
      return { ...decodeMemberMutation(req.__r, req), id }
    }
  }
  return malformed('request')
}

function decodeRoomDeclaration(value: unknown): RoomDeclaration {
  const decl = record(value, 'declaration')
  switch (decl.__r) {
    case 'dm-reply':
      return decodeDmReplyDeclaration(decl)
    case 'sub-binary': {
      const { member, track, on } = decl
      if (member !== null && !isMemberId(member)) malformed('binary wants')
      if (track !== null && !isRoomTrack(track)) malformed('binary wants')
      return { __r: 'sub-binary', member: member ?? ALL, track: track ?? ALL, on: flag(on, 'binary wants') }
    }
    case 'sub-text':
      return {
        __r: 'sub-text',
        announce: flag(decl.announce, 'text wants'),
        members: decodeTextWants(decl.members),
      }
  }
  return malformed('declaration')
}

function decodeParticipantDeclaration(value: unknown): Extract<RoomDeclaration, { __r: 'dm-reply' }> {
  const decl = record(value, 'declaration')
  if (decl.__r !== 'dm-reply') malformed('declaration')
  return decodeDmReplyDeclaration(decl)
}

function decodeDmReplyDeclaration(decl: Record<string, unknown>): Extract<RoomDeclaration, { __r: 'dm-reply' }> {
  return { __r: 'dm-reply', ackId: text(decl.ackId, 'DM reply'), reply: decodeDmReply(decl.reply) }
}

/** A client-supplied reply, rebuilt field by field so no other key rides into the `dm-ack` envelope. */
function decodeDmReply(value: unknown): DmReply {
  const reply = record(value, 'DM reply')
  if (reply.ok === true) return { ok: true, result: reply.result }
  if (reply.ok !== false) malformed('DM reply')
  if (reply.abort === true) return { ok: false, abort: true, abortValue: reply.abortValue }
  return { ok: false, err: text(reply.err, 'DM reply') }
}

/** A declared text want per member id, into a record no key can reach the prototype of. */
function decodeTextWants(value: unknown): Record<string, boolean> {
  const changes = record(value, 'text wants')
  const members: Record<string, boolean> = Object.create(null)
  for (const [memberId, wanted] of Object.entries(changes)) {
    if (!isMemberId(memberId)) malformed('text wants')
    members[memberId] = flag(wanted, 'text wants')
  }
  return members
}

function decodeRoomPublish(value: unknown): RoomDataPublish {
  const publish = record(value, 'publish')
  if (publish.__r !== 'data') malformed('publish')
  const decoded: RoomDataPublish = { __r: 'data', from: memberId(publish.from, 'publish'), data: publish.data }
  if (optionalTrue(publish.retain, 'publish retain')) decoded.retain = true
  return decoded
}

function decodeStubBinaryFrame(framed: Uint8Array): BinaryFrame {
  return decodeBinaryFrame(framed) ?? malformed('binary frame')
}

function decodeParticipantFrame(framed: Uint8Array, participantId: string): BinaryFrame {
  const frame = decodeStubBinaryFrame(framed)
  if (frame.from !== participantId) malformed('binary frame sender')
  return frame
}

function decodeParticipantRequest(value: unknown): ParticipantStubRequest {
  const req = record(value, 'participant request')
  switch (req.__r) {
    case 'req-publish': {
      const publish: Extract<ParticipantStubRequest, { __r: 'req-publish' }> = { __r: 'req-publish', data: req.data }
      if (optionalTrue(req.retain, 'publish retain')) publish.retain = true
      return publish
    }
    case 'req-set-meta':
    case 'req-set-attrs':
    case 'req-dm':
      return decodeMemberMutation(req.__r, req)
    case 'req-leave':
      return { __r: 'req-leave' }
  }
  return malformed('participant request')
}

/** A member's meta, attributes or DM request, as both stubs decode it; a Room stub's also names the acting member. */
function decodeMemberMutation(kind: MemberMutationRequest['__r'], req: Record<string, unknown>): MemberMutationRequest {
  switch (kind) {
    case 'req-set-meta':
      return { __r: 'req-set-meta', meta: record(req.meta, 'setMeta meta') }
    case 'req-set-attrs':
      return { __r: 'req-set-attrs', attrs: record(req.attrs, 'attributes') }
    case 'req-dm': {
      const dm: Extract<MemberMutationRequest, { __r: 'req-dm' }> = {
        __r: 'req-dm',
        to: text(req.to, 'send recipient'),
        data: req.data,
      }
      if (optionalTrue(req.ack, 'send ack')) dm.ack = true
      return dm
    }
  }
}
