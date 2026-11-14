export { LoneMember }

import { assert } from '../utils/assert.js'
import type { ChannelPublishAck, ClientBroadcast } from 'telefunc/__internal'
import {
  hasRoomTag,
  inboxMessageFromWire,
  leaveCauseFromWire,
  type MemberMetaUpdate,
  type MemberMutationRequest,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
} from '../shared/protocol.js'
import { roomResultValue } from '../shared/errors.js'
import type { MetaRewinds } from '../shared/MemberMeta.js'
import type { RoomState } from '../shared/state.js'
import { ClientParticipant, request, sendDmReply, type PageLink } from './ClientParticipant.js'
import { roomsOf } from './page.js'
import type { Sender } from '../types.js'

type LoneMemberOptions = { end: ClientBroadcast; metadata: ParticipantStubMetadata }

/** A server's `LocalParticipant` that a telefunction returned alone: the page's participant, on a channel of its own
 *  to the participant's stub. */
class LoneMember implements PageLink {
  readonly participant: ClientParticipant
  private readonly end: ClientBroadcast
  private readonly roomId: string
  /** Its own: the `rewound` notice forgets its seq. */
  private readonly rewinds: MetaRewinds = { count: 0 }

  constructor({ end, metadata }: LoneMemberOptions) {
    this.end = end
    this.roomId = metadata.roomId
    this.participant = new ClientParticipant({ link: this, participant: metadata, rewinds: this.rewinds })
    const { participant } = this

    end._subscribeLocal('text', (notice) => this.onNotice(notice))
    end.onClose(() => participant._onLeft({ type: 'disconnected' }))
    // Held from its channel's open: before it, the channel reads disconnected.
    end.onOpen(() => participant._holdOnPage())
    end.onDisconnect(() => participant._onDisconnected())
    end.onReconnect(() => participant._onReconnected())
  }

  get isConnected(): boolean {
    return this.end.isConnected
  }

  /** Its stub takes a text publish as a request. */
  async publishText(_from: string, data: unknown, retain: boolean | undefined): Promise<ChannelPublishAck> {
    const req: ParticipantStubRequest = { __r: 'req-publish', data }
    if (retain) req.retain = true
    return (await request(this.end, req)) as ChannelPublishAck
  }

  async publishFrame(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    return roomResultValue<ChannelPublishAck>(this.end._publishBinaryUnreported(framed, stream, lossy))
  }

  request(_from: string, req: MemberMutationRequest): Promise<unknown> {
    return request(this.end, req)
  }

  async leave(): Promise<void> {
    await request(this.end, { __r: 'req-leave' })
    this.participant._onLeft({ type: 'left' })
    void this.end.close().catch(() => {})
  }

  /** Its own writes and its stub's `p-meta` notices both land here. */
  acceptOwnMeta(_from: string, accepted: MemberMetaUpdate): void {
    this.participant._acceptMeta(accepted)
  }

  resolveSender(): Sender | null {
    return null
  }

  /** It has no room of its own: the page's rooms of its room list the participants. */
  *rooms(): Generator<RoomState> {
    for (const room of roomsOf(this.roomId)) yield room.state
  }

  private onNotice(notice: unknown): void {
    assert(hasRoomTag(notice))
    const msg = notice as ParticipantStubNotice
    const { participant } = this
    switch (msg.__r) {
      case 'p-meta':
        return this.acceptOwnMeta(participant.id, msg)
      case 'rewound':
        this.rewinds.count++
        return
      case 'demand':
        return participant._applyDemand(msg.track, msg)
      case 'demand-state':
        return participant._applyDemandState(msg.tracks)
      case 'dm':
        if (msg.ackId === undefined) return participant._deliverMessage(inboxMessageFromWire(msg))
        return sendDmReply(this.end, msg.ackId, participant._deliverMessageAck(inboxMessageFromWire(msg)))
      case 'left':
        return participant._onLeft(leaveCauseFromWire(msg))
    }
  }
}
