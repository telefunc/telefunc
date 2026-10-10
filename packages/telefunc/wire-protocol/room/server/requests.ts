export {
  decodeRoomRequest,
  decodeRoomDeclaration,
  decodeRoomPublish,
  decodeStubBinaryFrame,
  decodeParticipantRequest,
  decodeParticipantFrame,
  decodeDmReply,
  decodeBinaryWantsChange,
}
export type { RoomRequest, RoomDeclaration }

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { ProtocolViolationError } from '../../shared-ws.js'
import { decodeBinaryFrame, isMemberId, isRoomTrack, type BinaryFrame, type TrackWants } from '../binary.js'
import { ROOM_NAMED_TRACKS_MAX } from '../constants.js'
import { isRecord } from '../model.js'
import type { DmReply, ParticipantStubRequest, RoomDataPublish, RoomStubRequest } from '../protocol.js'
assertIsNotBrowser()

type RoomRequest = Extract<
  RoomStubRequest,
  { __r: 'req-join' | 'req-leave' | 'req-set-meta' | 'req-set-attrs' | 'req-dm' }
>
type RoomDeclaration = Exclude<RoomStubRequest, RoomRequest>
/** A member's meta, attributes and DM requests, by the API call each serves. */
const MEMBER_MUTATIONS = { 'req-set-meta': 'setMeta', 'req-set-attrs': 'setAttributes', 'req-dm': 'send' } as const
type MemberMutation = Extract<ParticipantStubRequest, { __r: keyof typeof MEMBER_MUTATIONS }>

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
  if (!isMemberId(value)) malformed(what)
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
      return {
        __r: 'dm-reply',
        ackId: text(decl.ackId, 'DM reply'),
        reply: decodeDmReply(decl.reply) ?? malformed('DM reply'),
      }
    case 'sub-binary':
      return { __r: 'sub-binary', ...decodeBinaryWantsChange(decl) }
    case 'sub-text':
      return {
        __r: 'sub-text',
        announce: flag(decl.announce, 'text wants'),
        members: decodeMemberChanges(decl.members, 'text wants', (wanted) => flag(wanted, 'text wants')),
      }
  }
  return malformed('declaration')
}

/** A client-supplied reply, rebuilt field by field so no other key rides into the `dm-ack` envelope. */
function decodeDmReply(reply: unknown): DmReply | null {
  if (!isRecord(reply)) return null
  if (reply.ok === true) return { ok: true, result: reply.result }
  if (reply.ok !== false) return null
  if (reply.abort === true) return { ok: false, abort: true, abortValue: reply.abortValue }
  return typeof reply.err === 'string' ? { ok: false, err: reply.err } : null
}

/** A client-declared `sub-binary` change. */
function decodeBinaryWantsChange(change: Record<string, unknown>): {
  everyMember: TrackWants
  members: Record<string, TrackWants | null>
} {
  return {
    everyMember: decodeTrackWants(change.everyMember),
    members: decodeMemberChanges(change.members, 'binary wants', (wants) =>
      wants === null ? null : decodeTrackWants(wants),
    ),
  }
}
/** A declared change per member id, into a record no key can reach the prototype of. */
function decodeMemberChanges<T>(changes: unknown, what: string, decode: (change: unknown) => T): Record<string, T> {
  if (!isRecord(changes)) malformed(what)
  const members: Record<string, T> = Object.create(null)
  for (const [memberId, change] of Object.entries(changes)) {
    if (!isMemberId(memberId)) malformed(what)
    members[memberId] = decode(change)
  }
  return members
}
function decodeTrackWants(wants: unknown): TrackWants {
  if (!isRecord(wants) || typeof wants.all !== 'boolean' || !Array.isArray(wants.tracks)) malformed('binary wants')
  if (wants.tracks.length > ROOM_NAMED_TRACKS_MAX || !wants.tracks.every(isRoomTrack)) malformed('binary wants')
  return { all: wants.all, tracks: wants.tracks as string[] }
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
    case 'req-publish':
      return {
        __r: 'req-publish',
        data: req.data,
        ...(optionalTrue(req.retain, 'publish retain') ? { retain: true } : {}),
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
function decodeMemberMutation(kind: MemberMutation['__r'], req: Record<string, unknown>): MemberMutation {
  switch (kind) {
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
  }
}
