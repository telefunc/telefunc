export { ServerLocalParticipant }

import { handleCallbackBug } from 'telefunc/__internal'
import { assertUsage } from '../utils/assert.js'
import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { type TrackKey } from '../shared/binary.js'
import { ROOM_BUG_MESSAGE } from '../shared/errors.js'
import { ownMessage } from '../shared/arguments.js'
import {
  type MemberMetaUpdate,
  type ParticipantStubMetadata,
  type InboxMessage,
  type RoomResult,
  type DemandChange,
  type TrackWatch,
} from '../shared/protocol.js'
import { ParticipantBase, type ParticipantBaseOptions } from '../shared/ParticipantBase.js'
import { RoomParticipantStub, type RoomParticipantStubOptions } from './RoomParticipantStub.js'
import type { RoomDemand } from './lanes/RoomDemand.js'
import { reportRoomError } from './reportRoomError.js'
import type { RemoteParticipant, Sender } from '../types.js'
import { RoomHandle } from './RoomHandle.js'
assertIsNotBrowser()

const SERVER_PARTICIPANT_BRAND: unique symbol = Symbol.for('telefunc.ServerRoomParticipant')

type MetaMirror = { accepted(update: MemberMetaUpdate): void; rewound(): void }

type ServerLocalParticipantOptions = {
  /** Where its acts go, and the room it lists the other participants by. */
  room: RoomHandle
  participant: ParticipantBaseOptions['participant']
  /** Holds who receives the participant's tracks, once for the whole room. */
  incomingDemand: Pick<RoomDemand, 'watchesOf'>
}

/** Server-side `LocalParticipant`, returned by `ServerRoom.join()`. Its acts go to its room's writes. */
class ServerLocalParticipant extends ParticipantBase {
  readonly [SERVER_PARTICIPANT_BRAND] = true
  /** @internal */
  readonly room: RoomHandle
  private metaMirror: MetaMirror | null = null
  /** The client holding this participant, which its inbox forwards to instead of local listeners. */
  private holder: RoomParticipantStub | null = null
  protected readonly _bugMessage = ROOM_BUG_MESSAGE

  private readonly incomingDemand: ServerLocalParticipantOptions['incomingDemand']

  constructor({ room, participant, incomingDemand }: ServerLocalParticipantOptions) {
    super({ participant, rewinds: room.model.metaRewinds, link: room })
    this.room = room
    this.incomingDemand = incomingDemand
  }

  static isServerLocalParticipant(value: unknown): value is ServerLocalParticipant {
    return value !== null && typeof value === 'object' && SERVER_PARTICIPANT_BRAND in value
  }

  /** @internal Read from the room's demand, which holds it once. */
  get _watches(): ReadonlyMap<TrackKey, TrackWatch> {
    return this.incomingDemand.watchesOf(this.id)
  }

  protected _shownParticipants(ids: ReadonlySet<string>): RemoteParticipant[] {
    return this.room.remotes.shownOf(ids)
  }

  protected get _rosterVersion(): number {
    return this.room.remotes.state.model.version
  }

  protected _followRoom(onChange: () => void): () => void {
    return this.room.state.onChange(onChange, undefined, 'app')
  }

  protected _deliverPrivate(field: string, from: string, data: unknown): void {
    this.room.state.applyPrivate(field, from, data)
  }

  /** @internal Who receives one of its tracks changed, which the client holding it hears too. */
  _demandChanged(track: TrackKey, change: DemandChange): void {
    this._demandStale(track)
    this._changed()
    this.holder?.demandChanged(track, change)
  }

  /** No wire stands between it and a listener here. */
  protected override _ownMessage(data: unknown): unknown {
    return ownMessage(data)
  }

  /** @internal A DM for this member: to the client holding it, else its listeners. */
  override _deliverMessage(msg: InboxMessage): void {
    if (this.holder) return this.holder.deliver(msg)
    super._deliverMessage(msg)
  }

  /** @internal An `{ ack: true }` DM: to the client holding it, else its listeners; never rejects. */
  override _deliverMessageAck(msg: InboxMessage): Promise<RoomResult> {
    if (this.holder) return this.holder.deliverAck(msg)
    return super._deliverMessageAck(msg)
  }

  /** @internal A reply to an acknowledged DM, on its way to the sender. */
  _trackReply(replied: Promise<void>): void {
    this.holder?.trackReply(replied)
  }

  /** @internal Whether a page holds this participant, which is then at that page. */
  get _heldByPage(): boolean {
    return this.holder !== null
  }

  /** @internal A client's hold on this participant, which its inbox forwards to, at the page `page` names
   *  (`PageIndex.place`). */
  _openStub({
    end,
    publishShield,
    page,
  }: Omit<RoomParticipantStubOptions, 'room' | 'participant'> & { page: string }): ParticipantStubMetadata {
    // A LocalParticipant has one holder; rebinding would overwrite its inbox forwarder and let either close drop it.
    assertUsage(
      this.holder === null,
      'This LocalParticipant is already bound to a client and cannot be handed to another. A LocalParticipant is a ' +
        'single member: give each client its own join(), or share a getParticipants() view (which is read-only) ' +
        'instead.',
    )
    this.holder = new RoomParticipantStub({ room: this.room, participant: this, end, publishShield })
    // It is at the holder's page from now on, not at its server-side room's.
    this.room.replica.placeAtPage(this.holder, page)
    this.room.replica.pageEndChanged(this.room.local, this.id)
    this._flushHeld(this.holder)
    return {
      channelId: end.id,
      roomId: this.room.id,
      id: this.id,
      meta: this.meta,
      selfDelivery: this.selfDelivery,
      identity: this.identity,
    }
  }

  /** @internal Each meta update this participant applies, and each failover rewind of its seq, which a client holding
   *  it mirrors. */
  _onMeta(mirror: MetaMirror): () => void {
    this.metaMirror = mirror
    return () => {
      if (this.metaMirror === mirror) this.metaMirror = null
    }
  }

  /** @internal A failover rewound its member's meta seq. */
  _metaRewound(): void {
    this.metaMirror?.rewound()
  }

  /** As the room accepted it: always true for one no page holds. */
  get isConnected(): boolean {
    return this._acceptedConnected
  }

  override _acceptMeta(update: MemberMetaUpdate): boolean {
    const wasConnected = this.isConnected
    if (!super._acceptMeta(update)) return false
    this.metaMirror?.accepted(update)
    if (this.isConnected === wasConnected) return true
    if (this.isConnected) this._onReconnected()
    else this._onDisconnected()
    return true
  }

  /** @internal The page holding this participant can reach it, or can't. */
  _setHolderConnected(connected: boolean): void {
    void this.room.writes.setMemberConnected(this.id, connected).catch(reportRoomError)
  }

  /** @internal The client holding this participant went away without leaving. */
  _releaseHolder(): Promise<void> {
    return this.room.removeDepartedMember(this.id)
  }

  protected override _resolveSender(id: string): Sender | null {
    return this.room.remotes.get(id) // sync view read (delivery must not wait on I/O)
  }

  protected _reportError(err: unknown): void {
    handleCallbackBug(err)
  }
}
