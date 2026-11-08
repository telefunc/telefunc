export { RoomParticipantStub }
export type { RoomParticipantStubOptions }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import {
  ProtocolViolationError,
  reconnectWindow,
  type BroadcastEndOwner,
  type ChannelPublishAck,
  type ServerBroadcastEnd,
} from 'telefunc/__internal'
import type { ShieldValidator } from 'telefunc'
import type { ServerLocalParticipant } from './room.js'
import {
  decodeParticipantDeclaration,
  decodeParticipantFrame,
  decodeParticipantRequest,
  isRoomRequest,
  pageFrameMessage,
} from './requests.js'
import { DEFAULT_TRACK, type BinaryFrame } from '../shared/binary.js'
import { DM_FAILURE } from '../shared/errors.js'
import {
  leaveCauseToWire,
  dmNotice,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type InboxMessage,
  type RoomResult,
} from '../shared/protocol.js'
import { unorderedEventBody } from './publishBodies.js'
import { PendingDmReplies, assertPublishShield, receiveFromPage, roomResult } from './pageRequests.js'
assertIsNotBrowser()

type RoomParticipantStubOptions = {
  participant: ServerLocalParticipant
  end: ServerBroadcastEnd
  publishShield?: ShieldValidator
}

/** One client's hold on a server participant: its requests act as that participant, whose inbox, demand, meta and leave
 *  flow back to the client. */
class RoomParticipantStub implements BroadcastEndOwner {
  /** Its channel to its client. */
  private readonly end: ServerBroadcastEnd
  private readonly participant: ServerLocalParticipant
  private readonly publishShield: ShieldValidator | undefined
  private readonly dmReplies = new PendingDmReplies()

  constructor({ participant, end, publishShield }: RoomParticipantStubOptions) {
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
    return await this.participant._publishFrame(frame, framed, message)
  }

  _onPageAttach(): void {
    this.notify({ __r: 'p-meta', ...this.participant._acceptedMeta })
    this.notify({ __r: 'demand-state', tracks: this.participant._demandedTracks })
  }

  /** A request, sent with an ack, answers with its result; an ack DM's reply, sent without one, settles that DM. */
  private receive(message: unknown): Promise<RoomResult> | undefined {
    if (isRoomRequest(message)) return roomResult(this.handleRequest(decodeParticipantRequest(message)))
    const { ackId, reply } = decodeParticipantDeclaration(message)
    this.dmReplies.settle(ackId, reply)
    return undefined
  }

  private async handleRequest(req: ParticipantStubRequest): Promise<unknown> {
    const participant = this.participant
    switch (req.__r) {
      case 'req-publish':
        assertPublishShield(this.publishShield, req.data)
        return await participant.publish(req.data, req.retain ? { retain: true } : undefined)
      case 'req-set-meta':
        return await participant._setMeta(req.meta)
      case 'req-set-attrs':
        return await participant._setAttributes(req.attrs)
      case 'req-dm':
        return await participant.send(req.to, req.data, req.ack ? { ack: true } : undefined)
      case 'req-leave':
        return await participant.leave()
    }
  }

  private mirrorParticipant(): void {
    const participant = this.participant
    const unlistenMeta = participant._onMeta({
      accepted: (accepted) => this.notify({ __r: 'p-meta', ...accepted }),
      rewound: () => this.notify({ __r: 'rewound' }),
    })

    const unlistenDemand = participant.onDemand((track, wanted) =>
      this.notify({ __r: 'demand', track: track ?? DEFAULT_TRACK, wanted }),
    )

    let left = false
    const unlistenLeave = participant.onLeave((cause) => {
      left = true
      this.notify({ __r: 'left', ...leaveCauseToWire(cause) })
      // A client back while its channel still holds it gets its leave.
      void this.end.close({ timeout: reconnectWindow() }).catch(() => {})
    })

    this.end.onDisconnect(() => participant._setHolderConnected(false))
    this.end.onReconnect(() => participant._setHolderConnected(true))
    this.end.onClose(() => {
      unlistenMeta()
      unlistenDemand()
      unlistenLeave()
      this.dmReplies.drop()
      if (!left) void participant._releaseHolder()
    })
  }

  /** Its participant's inbox forwards here. */
  deliver(msg: InboxMessage): void {
    this.notify(dmNotice(msg))
  }

  /** A closed stub's client left with the participant; a closing stub's close waits for the replies to DMs it
   *  forwarded. */
  deliverAck(msg: InboxMessage): Promise<RoomResult> {
    if (this.end.isClosed) return Promise.resolve(DM_FAILURE.left)
    const reply = this.dmReplies.await(msg.ackId!)
    this.notify(dmNotice(msg))
    return this.end.trackAck(reply.then((answer) => answer ?? DM_FAILURE.left))
  }

  /** Its close waits for the reply's commit, as a room stub's does (`awaitDmReply`). */
  trackReply(replied: Promise<void>): void {
    if (!this.end.isClosed) void this.end.trackAck(replied).catch(() => {})
  }

  /** A closed stub's client gets no notice: one sent after an overflow ended it would arrive after a gap. */
  private notify(notice: ParticipantStubNotice): void {
    if (this.end.isClosed) return
    this.end.deliver(unorderedEventBody(notice))
  }
}
