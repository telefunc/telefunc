export { LoneMember }

import type { ChannelPublishAck } from 'telefunc/__internal'
import {
  inboxMessageFromWire,
  leaveCauseFromWire,
  type MemberRequest,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
} from '../shared/protocol.js'
import { publishAck, roomResultValue } from '../shared/errors.js'
import { RoomState, type RoomStateOwner } from '../shared/RoomState.js'
import { RoomRemotes } from '../shared/RoomView.js'
import { ClientParticipant, reportClientCallbackError, type PageLink } from './ClientParticipant.js'
import { deliverDm, request, type RoomChannel } from './stubChannel.js'
import { RoomMirror } from './RoomMirror.js'
import { roomsOf } from './page.js'
import type { Sender } from '../types.js'

/** A participant stub's channel: it relays the stub's notices, and takes no text publish (one goes as a request). */
type LoneMemberChannel = RoomChannel<ParticipantStubNotice, never>
type LoneMemberOptions = { end: LoneMemberChannel; metadata: ParticipantStubMetadata }

/** A server's `LocalParticipant` that a telefunction returned alone: the page's participant, on a channel of its own
 *  to the participant's stub, and the room it lists its watchers by. */
class LoneMember implements PageLink {
  readonly participant: ClientParticipant
  private readonly end: LoneMemberChannel
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
    const req: Extract<ParticipantStubRequest, { __r: 'req-publish' }> = { __r: 'req-publish', data }
    if (retain) req.retain = true
    return await request(this.end, req)
  }

  async publishFrame(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    return publishAck(await roomResultValue(this.end._publishBinaryUnreported(framed, stream, lossy)))
  }

  /** Its own meta, as the room accepted it, lands here as its stub's `p-meta` notices of it do; its leave ends it and
   *  its channel. */
  async request(_from: string, req: MemberRequest): Promise<unknown> {
    if (req.__r === 'req-set-meta' || req.__r === 'req-update-meta') {
      const accepted = await request(this.end, req)
      this.participant._acceptMeta(accepted)
      return accepted
    }
    const value = await request(this.end, req)
    if (req.__r === 'req-leave') {
      this.participant._onLeft({ type: 'left' })
      void this.end.close()
    }
    return value
  }

  resolveSender(): Sender | null {
    return null
  }

  get remotes(): RoomRemotes {
    return this.room.remotes
  }

  /** The package fields run on the page's rooms of its room. */
  deliverPrivate(field: string, from: string, data: unknown): void {
    for (const room of roomsOf(this.room.id)) room.state.applyPrivate(field, from, data)
  }

  private onNotice(msg: ParticipantStubNotice): void {
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
        return deliverDm(this.end, participant, inboxMessageFromWire(msg))
      case 'left':
        return participant._onLeft(leaveCauseFromWire(msg))
    }
  }
}

/** The room a `me` returned alone lists its watchers by: the roster and presence events its stub sends once someone
 *  receives one of its tracks; it knows nobody before. Its listeners declare nothing: the stub's channel carries no
 *  message of the room. */
class LoneRoom implements RoomStateOwner {
  readonly mirror: RoomMirror
  readonly remotes: RoomRemotes

  constructor({ roomId }: ParticipantStubMetadata) {
    this.mirror = new RoomMirror({
      snapshot: { roomId, meta: [], metaSeq: 0, closed: false, members: [] },
      onMemberLeft: () => {},
      onMemberMeta: () => {},
    })
    const state = new RoomState({ model: this.mirror.model, owner: this })
    this.remotes = new RoomRemotes({ state, adder: 'app' })
  }

  get id(): string {
    return this.mirror.model.roomId
  }

  onWantChanged(): void {}

  reportCallbackError(error: unknown): void {
    reportClientCallbackError(error)
  }
}
