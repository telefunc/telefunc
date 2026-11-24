export { RoomParticipantStub }
export type { RoomParticipantStubOptions }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import {
  ProtocolViolationError,
  type BroadcastEndOwner,
  type ChannelPublishAck,
  type PublishBody,
  type ServerBroadcastEnd,
} from 'telefunc/__internal'
import type { ShieldValidator } from 'telefunc'
import type { RoomHandle } from './RoomHandle.js'
import type { ServerLocalParticipant } from './ServerLocalParticipant.js'
import {
  decodeParticipantDeclaration,
  decodeParticipantFrame,
  decodeParticipantRequest,
  isRoomRequest,
  pageFrameMessage,
} from './requests.js'
import type { BinaryFrame, TrackKey } from '../shared/binary.js'
import { DM_FAILURE, participantLeftError } from '../shared/errors.js'
import {
  leaveCauseToWire,
  dmNotice,
  watchStates,
  type DemandChange,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type InboxMessage,
  type MemberSnapshot,
  type RoomResult,
} from '../shared/protocol.js'
import { unorderedEventBody } from './publishBodies.js'
import { reportRoomError } from './reportRoomError.js'
import {
  PendingDmReplies,
  assertPublishShield,
  closeAfterReconnectWindow,
  receiveFromPage,
  roomResult,
} from './pageRequests.js'
import { assert } from '../utils/assert.js'
assertIsNotBrowser()

type RoomParticipantStubOptions = {
  /** The room its participant's requests go to. */
  room: RoomHandle
  participant: ServerLocalParticipant
  end: ServerBroadcastEnd
  publishShield?: ShieldValidator
}

/** One client's hold on a server participant: its requests act as that participant, whose inbox, demand, meta and leave
 *  flow back to the client. */
class RoomParticipantStub implements BroadcastEndOwner {
  /** Its channel to its client. */
  private readonly end: ServerBroadcastEnd
  private readonly room: RoomHandle
  private readonly participant: ServerLocalParticipant
  private readonly publishShield: ShieldValidator | undefined
  private readonly dmReplies = new PendingDmReplies()
  /** Whether its participant left, which its page then holds no more. */
  private left = false

  constructor({ room, participant, end, publishShield }: RoomParticipantStubOptions) {
    this.room = room
    this.participant = participant
    this.publishShield = publishShield
    this.end = end
    end.setOwner(this)
    end.listen((message) => receiveFromPage(end, () => this.receive(message)))
    this.mirrorParticipant()
  }

  /** Its client publishes text as a `req-publish` request. */
  _onPagePublish(): Promise<RoomResult> {
    return receiveFromPage(this.end, () => {
      throw new ProtocolViolationError('text publish on a Room participant stub')
    })
  }

  _onPagePublishBinary(framed: Uint8Array): Promise<RoomResult> {
    return receiveFromPage(this.end, () => {
      const frame = decodeParticipantFrame(framed, this.participant.id)
      return roomResult(this.publishFrame(frame, framed, pageFrameMessage(frame)))
    })
  }

  private async publishFrame(frame: BinaryFrame, framed: Uint8Array, message: unknown): Promise<ChannelPublishAck> {
    if (frame.text) assertPublishShield(this.publishShield, message)
    this.assertActive()
    return await this.room.writes.publishBinaryFrame(frame, framed, message)
  }

  _onPageAttach(): void {
    const participant = this.participant
    this.notify({ __r: 'p-meta', id: participant.id, ...participant._acceptedMeta })
    if (participant._watches.size > 0) this.followRoster()
    this.notify({ __r: 'demand-state', tracks: watchStates(participant._watches) })
  }

  /** A request, sent with an ack, answers with its result; an ack DM's reply, sent without one, settles that DM. */
  private receive(message: unknown): Promise<RoomResult> | undefined {
    if (isRoomRequest(message)) return roomResult(this.handleRequest(decodeParticipantRequest(message)))
    const { ackId, reply } = decodeParticipantDeclaration(message)
    this.dmReplies.settle(ackId, reply)
    return undefined
  }

  /** Performed as its participant, and not copied: what it decoded from the wire is its own. */
  private async handleRequest(req: ParticipantStubRequest): Promise<unknown> {
    const { id } = this.participant
    switch (req.__r) {
      case 'req-publish':
        assertPublishShield(this.publishShield, req.data)
        this.assertActive()
        return await this.room.writes.publishText(id, req.data, req.retain)
      case 'req-leave':
        if (!this.left) await this.room.request(id, req)
        return undefined
      default:
        this.assertActive()
        return await this.room.request(id, req)
    }
  }

  private assertActive(): void {
    if (this.left) throw participantLeftError()
  }

  private mirrorParticipant(): void {
    const participant = this.participant
    const unlistenMeta = participant._onMeta({
      accepted: (accepted) => this.notify({ __r: 'p-meta', id: participant.id, ...accepted }),
      rewound: () => this.notify({ __r: 'rewound' }),
    })

    const unlistenLeave = participant.onLeave((cause) => {
      this.left = true
      this.room.replica.pageEndChanged(this, participant.id)
      this.notify({ __r: 'left', ...leaveCauseToWire(cause) })
      closeAfterReconnectWindow(this.end)
    })

    this.end.onDisconnect(() => participant._setHolderConnected(false))
    this.end.onReconnect(() => participant._setHolderConnected(true))
    this.end.onClose(() => {
      this.room.replica.leavePage(this)
      this.room.replica.unfollowRoster(this)
      unlistenMeta()
      unlistenLeave()
      this.dmReplies.drop()
      if (!this.left) void participant._releaseHolder()
    })
  }

  /** Who receives one of its participant's tracks changed. */
  demandChanged(track: TrackKey, change: DemandChange): void {
    this.followRoster()
    this.notify({ __r: 'demand', track, ...change })
  }

  /** Its client names who receives its participant's tracks from the room's roster from now on. */
  private followRoster(): void {
    if (!this.end.isClosed) void this.room.replica.followRoster(this).catch(reportRoomError)
  }

  /** Its participant, who is at its page until it leaves. */
  heldMembers(): string[] {
    return this.left ? [] : [this.participant.id]
  }

  selfSuppressed(): string[] {
    return this.left || this.participant.selfDelivery ? [] : [this.participant.id]
  }

  holds(member: string): boolean {
    return !this.left && member === this.participant.id
  }

  suppresses(member: string): boolean {
    return this.holds(member) && !this.participant.selfDelivery
  }

  /** Its participant's inbox forwards here. */
  deliver(msg: InboxMessage): void {
    this.notify(dmNotice(msg))
  }

  /** A closed stub's client left with the participant; a closing stub's close waits for the replies to DMs it
   *  forwarded. */
  deliverAck(msg: InboxMessage): Promise<RoomResult> {
    if (this.end.isClosed) return Promise.resolve(DM_FAILURE.left)
    assert(msg.ackId !== undefined)
    const reply = this.awaitDmReply(msg.ackId)
    this.notify(dmNotice(msg))
    return this.end.trackAck(reply)
  }

  private async awaitDmReply(ackId: string): Promise<RoomResult> {
    return (await this.dmReplies.await(ackId)) ?? DM_FAILURE.left
  }

  /** Its close waits for the reply's commit, as a room stub's does (`awaitDmReply`). */
  trackReply(replied: Promise<void>): void {
    if (!this.end.isClosed) void this.end.trackAck(replied).catch(() => {})
  }

  /** A closed stub's client gets nothing: a body sent after an overflow ended it would arrive after a gap. */
  relayEventBody(body: PublishBody): void {
    if (!this.end.isClosed) this.end.deliver(body)
  }

  /** Its client was handed no hidden member. */
  rosterMembers(_part: MemberSnapshot[], visible: MemberSnapshot[]): MemberSnapshot[] {
    return visible
  }

  relayControl(body: PublishBody, hiddenMember: string | null): void {
    if (hiddenMember === null) this.relayEventBody(body)
  }

  private notify(notice: ParticipantStubNotice): void {
    this.relayEventBody(unorderedEventBody(notice))
  }
}
