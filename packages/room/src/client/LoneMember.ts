export { LoneMember }

import { assert } from '../utils/assert.js'
import type { ChannelPublishAck, ClientBroadcast } from 'telefunc/__internal'
import {
  hasRoomTag,
  inboxMessageFromWire,
  leaveCauseFromWire,
  type MemberRequest,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
} from '../shared/protocol.js'
import { roomResultValue } from '../shared/errors.js'
import { RoomState, RoomStateView, type RoomStateOwner } from '../shared/state.js'
import {
  ClientParticipant,
  acceptedMeta,
  reportClientCallbackError,
  request,
  sendDmReply,
  type PageLink,
} from './ClientParticipant.js'
import { RoomMirror } from './RoomMirror.js'
import { roomsOf } from './page.js'
import type { Sender } from '../types.js'

type LoneMemberOptions = { end: ClientBroadcast; metadata: ParticipantStubMetadata }

/** A server's `LocalParticipant` that a telefunction returned alone: the page's participant, on a channel of its own
 *  to the participant's stub, and the room it lists its watchers by. */
class LoneMember implements PageLink {
  readonly participant: ClientParticipant
  private readonly end: ClientBroadcast
  private readonly room: LoneRoom

  constructor({ end, metadata }: LoneMemberOptions) {
    this.end = end
    this.room = new LoneRoom(metadata)
    const rewinds = this.room.mirror.model.metaRewinds
    this.participant = new ClientParticipant({ link: this, participant: metadata, rewinds })
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

  /** Its own meta, as the room accepted it, lands here as its stub's `p-meta` notices of it do; its leave ends it and
   *  its channel. */
  async request(_from: string, req: MemberRequest): Promise<unknown> {
    const value = await request(this.end, req)
    const accepted = acceptedMeta(req, value)
    if (accepted) this.participant._acceptMeta(accepted)
    if (req.__r === 'req-leave') {
      this.participant._onLeft({ type: 'left' })
      void this.end.close().catch(() => {})
    }
    return value
  }

  resolveSender(): Sender | null {
    return null
  }

  get state(): RoomState {
    return this.room._state
  }

  /** The package fields run on the page's rooms of its room. */
  deliverPrivate(field: string, from: string, data: unknown): void {
    for (const room of roomsOf(this.room.id)) room.state.applyPrivate(field, from, data)
  }

  private onNotice(notice: unknown): void {
    assert(hasRoomTag(notice))
    const msg = notice as ParticipantStubNotice
    const { participant } = this
    switch (msg.__r) {
      case 'roster':
      case 'rewound':
      case 'join':
      case 'leave':
        return this.room.mirror.apply(msg)
      case 'p-meta':
        this.room.mirror.apply(msg)
        if (msg.id === participant.id) participant._acceptMeta(msg)
        return
      // It shows no room meta.
      case 'update':
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

/** The room a `me` returned alone lists its watchers by: the roster and presence events its stub sends once someone
 *  receives one of its tracks; it knows nobody before. Its listeners declare nothing: the stub's channel carries no
 *  message of the room. */
class LoneRoom extends RoomStateView implements RoomStateOwner {
  /** @internal */
  readonly _state: RoomState
  readonly mirror: RoomMirror

  constructor({ channelId, roomId }: ParticipantStubMetadata) {
    super()
    this.mirror = new RoomMirror({
      snapshot: { channelId, roomId, meta: [], metaSeq: 0, closed: false, members: [] },
      onMemberLeft: () => {},
      onMemberMeta: () => {},
    })
    this._state = new RoomState({ view: this, model: this.mirror.model, owner: this })
  }

  onWantChanged(): void {}

  reportCallbackError(error: unknown): void {
    reportClientCallbackError(error)
  }
}
