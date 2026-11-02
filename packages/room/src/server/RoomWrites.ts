export { RoomWrites, runAfterHook }
export type { RoomWritesOptions, RoomWritesOwner }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../utils/unrefTimer.js'
import type { CommitAccepted } from '../backend/contract.js'
import { encodeLaneKey } from '../backend/laneKey.js'
import { type ChannelPublishAck, reportBug, createDeferred } from 'telefunc/__internal'
import type { BinaryFrame } from '../shared/binary.js'
import { ROOM_DM_ACK_TIMEOUT_MS } from './constants.js'
import {
  DM_FAILURE,
  messageLostError,
  participantGoneError,
  participantLeftError,
  roomClosedError,
  roomFailureError,
  toRoomFailure,
  ROOM_BUG_MESSAGE,
} from '../shared/errors.js'
import { ownMetaArgument, senderSnapshot } from '../shared/arguments.js'
import type {
  MemberMetaUpdate,
  WholeMemberUpdate,
  RoomResult,
  RoomDmEnvelope,
  SenderStamp,
} from '../shared/protocol.js'
import type { RoomDataEnvelope, RoomDmAckEnvelope, RoomIncarnation } from './protocol.js'
import type { RoomModel } from '../shared/RoomModel.js'
import type { RoomState } from '../shared/state.js'
import type { ParticipantMeta, RoomAckReceipt, RoomSendReceipt, Sender } from '../types.js'
import { memberCellKey } from './store/cells.js'
import { reportRoomError } from './reportRoomError.js'
import { SEMANTIC_LANE, acceptedOrThrow, commitLane, encodeRoomRecord, inboxOf } from './lanes/lanes.js'
import { reapAndReadMembersById } from './store/roster.js'
import { storeDmReply, takeDmReply } from './store/dmReplies.js'
import { setMemberConnection, setMemberMeta, type MemberMetaWrite } from './store/meta.js'
import type { RoomReplica } from './RoomReplica.js'
import type { PendingDmAcks } from './PendingDmAcks.js'
import type { RoomGuardHooks } from './roomStatics.js'
assertIsNotBrowser()

/** The handle the writes go out through: its guards pass them. */
interface RoomWritesOwner {
  readonly guards: Partial<RoomGuardHooks> | null
}

type RoomWritesOptions = {
  owner: RoomWritesOwner
  incarnation: RoomIncarnation
  model: Pick<RoomModel, 'closed' | 'acceptedMeta'>
  state: Pick<RoomState, 'getRemote'>
  replica: RoomReplica
  /** The view's in-flight ack DMs, which a leave of either end or the room's close settles too. */
  acks: PendingDmAcks
  /** The handle's local participants, whose stamps a view that holds no roster yet doesn't hold. */
  localParticipants: ReadonlyMap<
    string,
    { meta: ParticipantMeta; identity: string | null; _acceptedMeta: WholeMemberUpdate }
  >
}

/** A member's writes through one handle: each passes its guard, carries its sender's stamp, commits its cells with its
 *  event in one exchange, then runs its after-hook. */
class RoomWrites {
  /** The last publish in call order, by key: a member's id for its text, `meta:<id>` for its meta writes, and
   *  its lane key for a binary lane. */
  private readonly publishTurns = new Map<string, Promise<void>>()

  private readonly owner: RoomWritesOwner
  private readonly incarnation: RoomIncarnation
  private readonly model: RoomWritesOptions['model']
  private readonly state: RoomWritesOptions['state']
  private readonly replica: RoomReplica
  private readonly acks: PendingDmAcks
  private readonly localParticipants: RoomWritesOptions['localParticipants']

  constructor({ owner, incarnation, model, state, replica, acks, localParticipants }: RoomWritesOptions) {
    this.owner = owner
    this.incarnation = incarnation
    this.model = model
    this.state = state
    this.replica = replica
    this.acks = acks
    this.localParticipants = localParticipants
  }

  /** Full replace (`setMeta`). */
  async setMemberMeta(id: string, meta: ParticipantMeta): Promise<MemberMetaUpdate> {
    return await this.writeMetaInOrder(id, { replace: ownMetaArgument(meta, 'setMeta() meta') })
  }

  /** Per-key merge (`setAttributes`); an `undefined` value deletes the key. */
  async mergeMemberMeta(id: string, attrs: ParticipantMeta): Promise<MemberMetaUpdate> {
    return await this.writeMetaInOrder(id, { merge: ownMetaArgument(attrs, 'setAttributes() attributes') })
  }

  /** The connection of the page holding the member, which its holder here saw change. */
  async setMemberConnected(id: string, connected: boolean): Promise<void> {
    await this.inCallOrder(`meta:${id}`, async () => {
      const written = await setMemberConnection(this.incarnation, id, connected)
      if (written !== null) this.replica.applyControl(written.event)
    })
  }

  /** The member's writes, from its single owner here, each start once the one before it committed, so they publish
   *  in seq order. */
  private async writeMetaInOrder(id: string, write: MemberMetaWrite): Promise<MemberMetaUpdate> {
    return await this.inCallOrder(`meta:${id}`, async () => {
      const { update, event } = await setMemberMeta(this.incarnation, id, write)
      this.replica.applyControl(event)
      return update
    })
  }

  async publishText(from: string, data: unknown, retain = false): Promise<ChannelPublishAck> {
    const { sender, commit } = await this.inCallOrder(from, async (sent) => {
      const { sender, stamp } = await this.admitPublish(from, data)
      const envelope: RoomDataEnvelope = { __r: 'data', from, data, ...stamp }
      const record = encodeRoomRecord(envelope)
      const target = { roomId: this.incarnation.id, inc: this.incarnation.inc, lane: SEMANTIC_LANE }
      const opts = { retain, owner: from, requiredCellKeys: [memberCellKey(from)] }
      const committing = commitLane(target, record, opts)
      sent()
      return { sender, commit: acceptedOrThrow(this.incarnation.id, await committing) }
    })
    return await this.finishPublish(sender, data, commit)
  }

  /** `frame` is `framed` decoded; receivers trust its sender id, which every caller checked is the publisher's. */
  async publishBinaryFrame(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    const { from } = frame
    const lane = { kind: 'binary', member: from, track: frame.track } as const
    const { sender, commit } = await this.inCallOrder(encodeLaneKey(lane), async (sent) => {
      const { sender } = await this.admitPublish(from, frame.payload)
      const admitted = this.replica.admitTrack(from, lane.track)
      if (admitted !== null) await admitted
      const opts = { retain: frame.retain, owner: from, requiredCellKeys: [memberCellKey(from)] }
      const committing = commitLane({ roomId: this.incarnation.id, inc: this.incarnation.inc, lane }, framed, opts)
      sent()
      return { sender, commit: acceptedOrThrow(this.incarnation.id, await committing) }
    })
    return await this.finishPublish(sender, frame.payload, commit)
  }

  /** A member's publishes on one lane leave in call order: each starts once the one before it was sent to the backend
   *  (its guard ran and its commit went out) or failed, never waiting on that commit's answer, which the backend
   *  keeps in sending order. */
  private async inCallOrder<T>(key: string, publish: (sent: () => void) => Promise<T>): Promise<T> {
    const previous = this.publishTurns.get(key)
    const turn = createDeferred()
    this.publishTurns.set(key, turn.promise)
    try {
      await previous
      return await publish(turn.resolve)
    } finally {
      turn.resolve()
      if (this.publishTurns.get(key) === turn.promise) this.publishTurns.delete(key)
    }
  }

  /** The sender as its guard sees it, with its stamp. */
  private async admitPublish(from: string, payload: unknown): Promise<{ sender: Sender; stamp: SenderStamp }> {
    if (this.model.closed) throw roomClosedError(this.incarnation.id)
    const sender = this.memberSender(from)
    const stamp = this.stampedSender(from, sender)
    const onBeforePublish = this.owner.guards?.onBeforePublish
    if (onBeforePublish) await onBeforePublish(sender, payload)
    return { sender, stamp }
  }

  private async finishPublish(sender: Sender, payload: unknown, commit: CommitAccepted): Promise<ChannelPublishAck> {
    const receipt: Omit<ChannelPublishAck, 'key'> = { seq: commit.seq, timestamp: commit.timestamp }
    const onAfterPublish = this.owner.guards?.onAfterPublish
    if (onAfterPublish) await runAfterHook(() => onAfterPublish(sender, payload, receipt))
    return { key: this.incarnation.id, ...receipt }
  }

  /** `sender` with the meta seq it reflects, read before a guard waits: a meta write meanwhile takes the next seq. */
  private stampedSender(from: string, sender: Sender): SenderStamp {
    const accepted = this.model.acceptedMeta(from) ?? this.localParticipants.get(from)!._acceptedMeta
    const stamp: SenderStamp = { fromMeta: sender.meta, fromMetaSeq: accepted.seq }
    if (sender.identity !== null) stamp.fromIdentity = sender.identity
    if (accepted.disconnected) stamp.fromDisconnected = true
    return stamp
  }

  /** A member that left while its call waited has no sender to show a guard. */
  private memberSender(from: string): Sender {
    const known = this.state.getRemote(from) ?? this.localParticipants.get(from)
    if (!known) throw participantLeftError()
    return senderSnapshot(from, known.meta, known.identity)
  }

  async sendDm(from: string, to: string, data: unknown, ack: boolean): Promise<RoomSendReceipt | RoomAckReceipt> {
    return ack ? await this.sendDmAck(from, to, data) : await this.publishDm(from, to, data)
  }

  private async sendDmAck(from: string, to: string, data: unknown): Promise<RoomAckReceipt> {
    const ackId = crypto.randomUUID()
    const acks = this.acks
    const reply = new Promise<RoomResult>((settle) => acks.add(ackId, { from, to, settle }))
    // Bounds the wait nothing else settles: a lost reply frame, or a recipient that never handled the DM.
    const timer = unrefTimer(setTimeout(() => void this.settleTimedOut(from, ackId), ROOM_DM_ACK_TIMEOUT_MS))
    let receipt: RoomSendReceipt
    try {
      receipt = await this.publishDm(from, to, data, ackId)
    } catch (err) {
      acks.delete(ackId)
      clearTimeout(timer)
      throw err
    }
    const settled = await reply
    clearTimeout(timer)
    if (!settled.ok) throw roomFailureError(settled)
    return { ...receipt, response: settled.value }
  }

  /** The recipient stores its reply before committing its frame, so a reply whose frame was lost is still read. */
  private async settleTimedOut(from: string, ackId: string): Promise<void> {
    let reply: RoomResult
    try {
      reply = (await takeDmReply(this.incarnation.id, this.incarnation.inc, from, ackId)) ?? DM_FAILURE.timeout
    } catch (err) {
      reply = toRoomFailure(err, reportRoomError, ROOM_BUG_MESSAGE)
    }
    this.acks.settle(ackId, reply)
  }

  private async publishDm(from: string, to: string, data: unknown, ackId?: string): Promise<RoomSendReceipt> {
    if (this.model.closed) throw roomClosedError(this.incarnation.id)
    const target = await this.resolveMember(to)
    if (!target) throw participantGoneError(to)
    const sender = this.memberSender(from)
    const stamp = this.stampedSender(from, sender)
    const onBeforeSend = this.owner.guards?.onBeforeSend
    if (onBeforeSend) await onBeforeSend(sender, target, data)
    const envelope: RoomDmEnvelope = { __r: 'dm', to, from, ...stamp, data }
    if (ackId) envelope.ackId = ackId
    const requiredCellKeys = [memberCellKey(from), memberCellKey(to)]
    const committing = commitLane(inboxOf(this.incarnation.id, this.incarnation.inc, to), encodeRoomRecord(envelope), {
      requiredCellKeys,
    })
    const receipt = acceptedOrThrow(this.incarnation.id, await committing)
    if (receipt.lost !== undefined) throw messageLostError()
    const info: RoomSendReceipt = { seq: receipt.seq, timestamp: receipt.timestamp }
    const onAfterSend = this.owner.guards?.onAfterSend
    if (onAfterSend) await runAfterHook(() => onAfterSend(sender, target, data, info))
    return info
  }

  /** A sender that left had its wait failed by its leave; only a closed room is an error. */
  async publishDmAck(to: string, ackId: string, reply: RoomResult): Promise<void> {
    await storeDmReply(this.incarnation.id, this.incarnation.inc, to, ackId, reply)
    const { id, inc } = this.incarnation
    const envelope: RoomDmAckEnvelope = { __r: 'dm-ack', to, ackId, ...reply }
    const requiredCellKeys = [memberCellKey(to)]
    const acked = await commitLane(inboxOf(id, inc, to), encodeRoomRecord(envelope), { requiredCellKeys })
    if (acked.kind === 'stale-incarnation') throw roomClosedError(id)
  }

  private async resolveMember(id: string): Promise<Sender | null> {
    const remote = this.state.getRemote(id)
    if (remote) return senderSnapshot(id, remote.meta, remote.identity)
    const [member] = await reapAndReadMembersById(this.incarnation, [id])
    return member === undefined ? null : senderSnapshot(id, member.meta, member.identity ?? null)
  }
}

async function runAfterHook(hook: () => unknown): Promise<void> {
  try {
    await hook()
  } catch (error) {
    reportBug(error)
  }
}
